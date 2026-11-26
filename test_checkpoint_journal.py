"""Tests for CheckpointJournal: published baselines plus retained deltas."""
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest

try:
    import resource
except ImportError:  # pragma: no cover - non-Linux hosts
    resource = None

from journal import (
    CheckpointJournal,
    ExpiredOffset,
    MAGIC,
    RetainedJournal,
    _HEADER,
    _ck_decode,
)

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))


def read_all_raw(journal, limit=256):
    rows = []
    offset = journal.floor
    while True:
        batch = journal.read(offset, limit=limit)
        if not batch:
            break
        rows.extend(batch)
        offset = batch[-1].next_offset
    return rows


def read_all_changes(journal, start, limit=16):
    changes = []
    offset = start
    while True:
        batch = journal.read(offset, limit=limit)
        if not batch:
            break
        changes.extend(batch)
        offset = batch[-1].next_offset
    return changes


class AcceptanceFlowTest(unittest.TestCase):
    def setUp(self):
        self.root = TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.dir = Path(self.root.name) / "config"

    def test_baseline_cut_delta_and_retention_flow(self):
        with CheckpointJournal(self.dir) as log:
            self.assertEqual((log.floor, log.end), (4, 4))
            empty = log.put("empty", b"")
            first = log.put("alpha", b"one")
            second = log.put("alpha", b"two")
            # The empty value is still live just before its delete frame.
            pre_delete = log.end
            deletion = log.delete("empty")
            # Cut right after the delete frame.
            cut = log.end
            # An online subscriber starts at the initial floor (4) and has
            # only acknowledged ``second``, behind the future cut.
            self.assertEqual(log.register("sub"), 4)
            log.ack("sub", pre_delete)

            self.assertEqual((empty, first, second, deletion),
                             (4, 27, 53, 79))
            # Rewrite an existing key after the cut; it must not leak into
            # the published baseline.
            later = log.put("alpha", b"three")
            self.assertEqual(
                log.end, later + _HEADER.size + 2 + 5 + 4 + 5
            )

            # Publishing alone moves nothing: with an unpublished baseline
            # prune stays at the floor.
            self.assertEqual(log.prune(), 4)
            self.assertEqual(log.publish(cut), cut)
            self.assertEqual(log.cut, cut)
            # Now prune reaches the subscriber's ack, which is below the cut.
            self.assertEqual(log.prune(), pre_delete)
            self.assertEqual(log.floor, pre_delete)
            self.assertEqual(log.publish(cut), cut)  # same-point retry
            published_cut, rows = log.bootstrap()
            self.assertEqual(published_cut, cut)
            # The baseline shows pre-cut state and omits the deleted key.
            self.assertEqual(list(rows), [("alpha", b"two")])

            # A newcomer continues from the same cut and sees the newer value.
            delta = log.read(cut)
            self.assertEqual(
                [(c.offset, c.key, c.value) for c in delta],
                [(later, "alpha", b"three")],
            )
            self.assertEqual(delta[0].next_offset, log.end)

            # Advancing the ack to end still cannot pass the published cut.
            log.ack("sub", log.end)
            self.assertEqual(log.prune(), cut)
            self.assertEqual(log.floor, cut)
            self.assertEqual(log.end, later + _HEADER.size + 2 + 5 + 4 + 5)

        # Inspect the real underlying journal after close.
        with RetainedJournal(self.dir / "events") as raw:
            self.assertEqual(raw.floor, cut)
            self.assertEqual(raw.end, later + _HEADER.size + 2 + 5 + 4 + 5)
            with self.assertRaises(ExpiredOffset):
                raw.read(empty)
            with self.assertRaises(ExpiredOffset):
                raw.read(first)
            survivors = read_all_raw(raw)
            self.assertEqual([r.offset for r in survivors], [later])
            frame_key, frame_value = _ck_decode(survivors[0].data)
            self.assertEqual((frame_key, frame_value), ("alpha", b"three"))

        # Reopen: the baseline and cut survive and continue to work.
        with CheckpointJournal(self.dir) as log:
            self.assertEqual(log.floor, cut)
            self.assertEqual(log.cut, cut)
            cut_again, rows = log.bootstrap()
            self.assertEqual(cut_again, cut)
            self.assertEqual(list(rows), [("alpha", b"two")])
            self.assertEqual(
                [(c.key, c.value) for c in log.read(cut)],
                [("alpha", b"three")],
            )
            start = log.end
            log.put("beta", b"b")
            self.assertEqual(
                [(c.key, c.value) for c in log.read(cut)],
                [("alpha", b"three"), ("beta", b"b")],
            )
            self.assertEqual(log.end, start + _HEADER.size + 2 + 4 + 4 + 1)

    def test_baseline_before_delete_includes_empty_value(self):
        with CheckpointJournal(self.dir) as log:
            log.put("empty", b"")
            log.put("alpha", b"two")
            pre_delete = log.end
            log.delete("empty")
            log.put("alpha", b"three")
            self.assertEqual(log.publish(pre_delete), pre_delete)
            _, rows = log.bootstrap()
            self.assertEqual(list(rows), [("alpha", b"two"), ("empty", b"")])
            # The delete and the rewrite are both readable from the cut.
            self.assertEqual(
                [(c.key, c.value) for c in log.read(pre_delete)],
                [("empty", None), ("alpha", b"three")],
            )
            # Rolling the published cut back is refused.
            with self.assertRaises(ValueError):
                log.publish(4)

    def test_unpublished_empty_value_and_empty_values_survive(self):
        with CheckpointJournal(self.dir) as log:
            log.put("e", b"")
            log.put("z", b"")
            cut = log.end
            log.publish(cut)
            _, rows = log.bootstrap()
            # Empty values are present (not confused with deletions).
            self.assertEqual(list(rows), [("e", b""), ("z", b"")])
            log.delete("e")
            self.assertEqual(log.publish(log.end), log.end)
            _, rows = log.bootstrap()
            self.assertEqual(list(rows), [("z", b"")])

    def test_untouched_keys_survive_prefix_reclamation(self):
        with CheckpointJournal(self.dir) as log:
            log.put("a", b"1")
            log.put("b", b"2")
            log.put("c", b"3")
            cut1 = log.publish(log.end)
            # Release the whole prefix covered by the baseline.
            self.assertEqual(log.prune(), cut1)
            log.put("a", b"updated")
            cut2 = log.publish(log.end)
            _, rows = log.bootstrap()
            # b and c were not touched after cut1 and must remain.
            self.assertEqual(
                list(rows), [("a", b"updated"), ("b", b"2"), ("c", b"3")]
            )
            self.assertEqual(cut2, log.end)
            self.assertEqual(log.prune(), cut2)

    def test_online_subscriber_alone_does_not_hold_history(self):
        with CheckpointJournal(self.dir) as log:
            log.put("a", b"1")
            log.register("slow")  # ack stays at 4
            self.assertEqual(log.prune(), 4)
            cut = log.publish(log.end)
            self.assertEqual(log.prune(), 4)  # subscriber still blocks
            log.drop("slow")
            self.assertEqual(log.prune(), cut)


class RandomizedModelTest(unittest.TestCase):
    def setUp(self):
        self.root = TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.dir = Path(self.root.name) / "config"

    def test_baseline_matches_replayed_model_across_operations(self):
        import random

        rng = random.Random(20261126)
        # (offset, key, value_or_None) for every appended change.
        history = []

        def model_at(cut):
            state = {}
            for offset, key, value in history:
                if offset >= cut:
                    break
                if value is None:
                    state.pop(key, None)
                else:
                    state[key] = value
            return state

        with CheckpointJournal(self.dir) as log:
            for _round in range(40):
                for _ in range(rng.randint(1, 6)):
                    key = "k%02d" % rng.randrange(12)
                    if rng.random() < 0.35:
                        offset = log.delete(key)
                        history.append((offset, key, None))
                    else:
                        value = bytes([rng.randrange(256)]) * rng.randrange(6)
                        offset = log.put(key, value)
                        history.append((offset, key, value))
                boundaries = [log.floor] + [
                    c.offset for c in read_all_changes(log, log.floor)
                ] + [log.end]
                candidates = [b for b in boundaries if b >= log.cut]
                cut = rng.choice(candidates)
                self.assertEqual(log.publish(cut), cut)
                self.assertEqual(dict(log.bootstrap()[1]), model_at(cut))
                # Sometimes advance an online subscriber and prune; the
                # published baseline must be unaffected by reclamation.
                log.register("sub")
                current = log.checkpoint("sub")
                forward = [b for b in boundaries if b >= current]
                log.ack("sub", rng.choice(forward))
                log.prune()
                self.assertEqual(dict(log.bootstrap()[1]), model_at(cut))
                # Reclamation must never pass the published cut.
                self.assertLessEqual(log.floor, cut)

        # Reopen: the last published baseline still matches the model.
        with CheckpointJournal(self.dir) as log:
            cut, rows = log.bootstrap()
            self.assertEqual(dict(rows), model_at(cut))


class ValidationTest(unittest.TestCase):
    def setUp(self):
        self.root = TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.dir = Path(self.root.name) / "config"

    def test_put_delete_validation_does_not_append(self):
        with CheckpointJournal(self.dir) as log:
            for key in ("", "x" * 33, "has space", "dot.name", "slash/x",
                        "café", 123, b"A", True):
                with self.subTest(key=key):
                    with self.assertRaises(ValueError):
                        log.put(key, b"v")
                    with self.assertRaises(ValueError):
                        log.delete(key)
            for value in ("text", bytearray(b"x"), b"x" * 1_000_001, None, 7):
                with self.subTest(value=type(value)):
                    with self.assertRaises(ValueError):
                        log.put("k", value)
            self.assertEqual(log.end, 4)
            # Boundary values are accepted.
            log.put("a-Z_0", b"x" * 1_000_000)
            log.put("k", b"")
            log.delete("never-present")  # deleting an absent key appends

    def test_read_validation(self):
        with CheckpointJournal(self.dir) as log:
            o1 = log.put("a", b"1")
            o2 = log.put("b", b"2")
            self.assertEqual(log.read(log.end), [])
            for bad_offset in (1.0, True, "4", None):
                with self.subTest(offset=bad_offset):
                    with self.assertRaises(ValueError):
                        log.read(bad_offset)
            for bad_limit in (0, 17, -1, True, 1.5, "16"):
                with self.subTest(limit=bad_limit):
                    with self.assertRaises(ValueError):
                        log.read(o1, limit=bad_limit)
            # Frame-internal and out-of-range offsets.
            with self.assertRaises(ValueError):
                log.read(o1 + 1)
            with self.assertRaises(ValueError):
                log.read(log.end + _HEADER.size)
            # Advance past o1 and prune; the old cursor is now expired.
            log.register("c")
            log.ack("c", o2)
            log.publish(o2)
            log.prune()
            with self.assertRaises(ExpiredOffset):
                log.read(o1)
            with self.assertRaises(ExpiredOffset):
                log.read(1)
            # A limit above the frame count simply returns fewer rows.
            self.assertEqual(len(log.read(o2, limit=16)), 1)

    def test_publish_validation_and_retry(self):
        with CheckpointJournal(self.dir) as log:
            o1 = log.put("a", b"1")
            o2 = log.put("b", b"2")
            end = log.end
            # Initial baseline cursor is 4; earlier or interior cuts are bad.
            for bad_cut in (3, 0, -1, o1 + 1, end + 1, 1.0, True, "4", None):
                with self.subTest(cut=bad_cut):
                    with self.assertRaises(ValueError):
                        log.publish(bad_cut)
            self.assertEqual(log.cut, 4)
            # An empty published state at the initial cursor.
            self.assertEqual(log.publish(4), 4)
            _, rows = log.bootstrap()
            self.assertEqual(list(rows), [])
            # Publishing an earlier frame boundary works, then rollback fails.
            self.assertEqual(log.publish(o2), o2)
            for earlier in (4, o1):
                with self.subTest(earlier=earlier):
                    with self.assertRaises(ValueError):
                        log.publish(earlier)
            # b was published before its frame; state only reflects [0, o2).
            _, rows = log.bootstrap()
            self.assertEqual(list(rows), [("a", b"1")])
            # End is a valid cut.
            self.assertEqual(log.publish(end), end)
            _, rows = log.bootstrap()
            self.assertEqual(list(rows), [("a", b"1"), ("b", b"2")])

    def test_subscriber_api_semantics(self):
        with CheckpointJournal(self.dir) as log:
            self.assertEqual(log.register("A-Z_0"), 4)
            self.assertEqual(log.register("A-Z_0"), 4)
            with self.assertRaises(KeyError):
                log.checkpoint("ghost")
            with self.assertRaises(KeyError):
                log.ack("ghost", 4)
            with self.assertRaises(ValueError):
                log.register("has space")
            for i in range(15):
                log.register(f"c{i:02d}")
            with self.assertRaises(ValueError):
                log.register("too-many")
            self.assertTrue(log.drop("c00"))
            self.assertFalse(log.drop("c00"))

    def test_single_active_handle(self):
        with CheckpointJournal(self.dir) as _log:
            with self.assertRaises(RuntimeError):
                CheckpointJournal(self.dir)


class ReopenStateTest(unittest.TestCase):
    def setUp(self):
        self.root = TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.dir = Path(self.root.name) / "config"

    def test_writes_baseline_acks_and_cut_survive(self):
        with CheckpointJournal(self.dir) as log:
            log.put("a", b"1")
            log.put("b", b"2")
            cut = log.publish(log.end)
            log.delete("a")
            log.register("sub")
            log.ack("sub", cut)
            self.assertEqual(log.prune(), cut)
            end = log.end
        with CheckpointJournal(self.dir) as log:
            self.assertEqual((log.floor, log.end, log.cut), (cut, end, cut))
            self.assertEqual(log.checkpoint("sub"), cut)
            _, rows = log.bootstrap()
            self.assertEqual(list(rows), [("a", b"1"), ("b", b"2")])
            self.assertEqual(
                [(c.key, c.value) for c in log.read(cut)],
                [("a", None)],
            )
            # Continue writing after the reopen.
            log.put("c", b"3")
            self.assertEqual(log.end, end + _HEADER.size + 1 + 1 + 1 + 4 + 1)

    def test_stale_tmp_and_uncommitted_baseline_are_cleaned(self):
        with CheckpointJournal(self.dir) as log:
            log.put("a", b"1")
            cut = log.publish(log.end)
        (self.dir / "state-000000000999.ckpt").write_bytes(b"junk")
        (self.dir / "state-000000000999.ckpt.tmp").write_bytes(b"junk")
        (self.dir / "checkpoint.json.tmp").write_bytes(b"junk")
        with CheckpointJournal(self.dir) as log:
            self.assertEqual(log.cut, cut)
            self.assertEqual(list(log.bootstrap()[1]), [("a", b"1")])
        names = {p.name for p in self.dir.iterdir()}
        self.assertNotIn("state-000000000999.ckpt", names)
        self.assertNotIn("state-000000000999.ckpt.tmp", names)
        self.assertNotIn("checkpoint.json.tmp", names)


# --- 48 MiB address-space cap and publish write amplification --------------

BUDGET_CHILD = r"""
import os, resource, sys
sys.path.insert(0, os.environ["PROJECT_DIR"])
from journal import CheckpointJournal, MAGIC

directory = sys.argv[1]
VALUE = 1_000_000  # near the per-value ceiling, distinct contents
NEEDED = 56

def wchar():
    with open("/proc/self/io", "rb") as handle:
        for line in handle:
            if line.startswith(b"wchar:"):
                return int(line.split()[1])
    raise RuntimeError("wchar unavailable")

def payload(prefix, i):
    filler = ("|%s%06d|" % (prefix, i)).encode("ascii")
    return (bytes([i % 251 + 1]) * (VALUE - len(filler))) + filler

def append_until_full(log, prefix):
    offsets = []
    i = 0
    while True:
        try:
            offsets.append(log.put("%s%03d" % (prefix, i), payload(prefix, i)))
        except ValueError:
            return offsets
        i += 1

log = CheckpointJournal(directory)
offsets = append_until_full(log, "k")
assert len(offsets) >= NEEDED, len(offsets)

# Without a published baseline, prune cannot release anything even when a
# subscriber acknowledges the end.  It is not enough to make room for a write.
log.register("sub")
log.ack("sub", log.end)
assert log.prune() == 4, log.floor
try:
    log.put("blocked", b"x" * VALUE)
    raise AssertionError("write unexpectedly admitted before publish")
except ValueError:
    pass

# Publish an existing frame boundary, prune to it, then the blocked write
# must actually land at the old end; surviving frame cursors do not move.
cut_index = len(offsets) // 2
cut = offsets[cut_index]
survivor = offsets[cut_index]
assert log.publish(cut) == cut
assert log.prune() == cut, log.floor
# Drop the online subscriber: with no external acknowledgements the baseline
# alone bounds reclamation from now on.
assert log.drop("sub") is True
end_before = log.end
resumed = log.put("after-prune", b"small")
assert resumed == end_before, (resumed, end_before)
assert log.read(survivor, limit=1)[0].offset == survivor
# The real journal moved its floor; old cursors are genuinely expired.
try:
    log.read(offsets[0])
    raise AssertionError("old cursor was not expired")
except ValueError:
    pass

# Fill the readable window again with distinct near-1MiB values.  The live
# state (published half plus this new half) now exceeds the 64 MiB ceiling,
# so publishing the new end must be rejected with the baseline untouched.
more = append_until_full(log, "n")
assert len(more) >= NEEDED - cut_index, len(more)
big_cut = log.cut
try:
    log.publish(log.end)
    raise AssertionError("oversized baseline was accepted")
except ValueError:
    pass
assert log.cut == big_cut
assert log.floor == cut

# Delete the whole second batch of values, which brings the live state (the
# published k values plus this round of deltas) back under 64 MiB.  Publish
# again from the old baseline plus the still-retained deltas and verify the
# survivors and their contents while streaming.
for index in range(len(more)):
    log.delete("n%03d" % index)
grown_cut = log.publish(log.end)
assert grown_cut == log.end
_, rows = log.bootstrap()
survivors = []
survivor_bytes = 0
for key, value in rows:
    survivors.append(key)
    survivor_bytes += len(key) + len(value)
expected_keys = ["after-prune"] + [
    "k%03d" % index for index in range(len(offsets))
]
assert survivors == expected_keys, (len(survivors), len(expected_keys))
assert survivor_bytes > 48 * 1024 * 1024, survivor_bytes
# Spot-check untouched forward and just-touched values.
_, rows = log.bootstrap()
for key, value in rows:
    if key == "after-prune":
        assert value == b"small"
        continue
    index = int(key[1:])
    assert value == payload("k", index), key
    if index >= 60:
        break

# Shrink the >48 MiB baseline down to just two values and measure the bytes
# this single publish wrote (baseline bytes + 16 MiB allowance).  This covers
# the "large old baseline, tiny new baseline" case.
keep = {"k000", "k001"}
for key in survivors:
    if key not in keep:
        log.delete(key)
target = log.end
expected = sum(len(key) + VALUE for key in keep)
start_w = wchar()
new_cut = log.publish(target)
delta_w = wchar() - start_w
assert new_cut == target
assert delta_w <= expected + 16 * 1024 * 1024, (delta_w, expected)

# Reopen in this same constrained process and stream the small baseline.
log.close()
log = CheckpointJournal(directory)
got_cut, rows = log.bootstrap()
assert got_cut == target
got = list(rows)
assert [key for key, _value in got] == sorted(keep), got
assert got[0][1] == payload("k", 0), "k000 content mismatch"
assert got[1][1] == payload("k", 1), "k001 content mismatch"
assert len(got[0][1]) == VALUE
assert log.floor <= target
log.close()
print("BUDGET_OK frames=%d wchar_delta=%d expected=%d" % (
    len(offsets) + len(more) + 1, delta_w, expected))
"""


@unittest.skipUnless(sys.platform.startswith("linux"), "Linux /proc and rlimit")
class BudgetTest(unittest.TestCase):
    def setUp(self):
        self.root = TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.dir = Path(self.root.name) / "config"
        self.script = Path(self.root.name) / "budget_child.py"
        self.script.write_text(BUDGET_CHILD, encoding="utf-8")

    def test_publish_under_48mib_with_bounded_write_amplification(self):
        limit = 48 * 1024 * 1024

        def apply_address_limit():
            resource.setrlimit(resource.RLIMIT_AS, (limit, limit))

        env = dict(os.environ, PROJECT_DIR=PROJECT_DIR)
        proc = subprocess.run(
            [sys.executable, "-I", str(self.script), str(self.dir)],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            preexec_fn=apply_address_limit,
            check=False,
        )
        if proc.returncode != 0:
            self.fail(
                "budget child failed\n"
                f"stdout:\n{proc.stdout}\n"
                f"stderr:\n{proc.stderr}"
            )
        self.assertIn("BUDGET_OK", proc.stdout)
        # The final published baseline holds exactly the two kept values and
        # the delta log saw real reclamation: reopening sees a floor equal to
        # the published cut with no readable deltas behind it.
        with CheckpointJournal(self.dir) as log:
            cut, rows = log.bootstrap()
            rows = list(rows)
            self.assertEqual([k for k, _ in rows], ["k000", "k001"])
            self.assertEqual(log.read(cut), [])
            self.assertEqual(log.end, cut)


if __name__ == "__main__":
    unittest.main(verbosity=2)
