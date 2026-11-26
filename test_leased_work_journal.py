"""Tests for LeasedWorkJournal: per-device ordered leased dispatch."""
import json
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
    Dispatch,
    ExpiredOffset,
    LeasedWorkJournal,
    RetainedJournal,
    _HEADER,
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


class DispatchFlowTest(unittest.TestCase):
    def setUp(self):
        self.root = TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.dir = Path(self.root.name) / "queue"

    def test_full_acceptance_flow_with_reopen_and_raw_inspection(self):
        with LeasedWorkJournal(self.dir) as queue:
            self.assertEqual(queue.floor, 4)
            self.assertEqual(queue.end, 4)
            # Nothing to do on an empty queue.
            self.assertIsNone(queue.claim(0, 10))

            a1 = queue.submit("A", b"a-one")
            a2 = queue.submit("A", b"")          # empty payload task
            b1 = queue.submit("B", b"b-one")
            self.assertEqual(a1, 4)
            self.assertEqual(a2, a1 + _HEADER.size + 1 + 1 + len(b"a-one"))
            self.assertEqual(b1, a2 + _HEADER.size + 1 + 1)
            self.assertEqual(queue.end, b1 + _HEADER.size + 1 + 1 + len(b"b-one"))

            first = queue.claim(0, 10)
            self.assertIsInstance(first, Dispatch)
            self.assertEqual(
                (first.offset, first.key, first.data),
                (a1, "A", b"a-one"),
            )
            self.assertEqual(first.deadline, 10)
            self.assertIsInstance(first.receipt, str)
            self.assertTrue(first.receipt)

            # A's head is leased but B is independent and still dispatchable.
            second = queue.claim(5, 10)
            self.assertEqual((second.offset, second.key, second.data),
                             (b1, "B", b"b-one"))
            self.assertNotEqual(second.receipt, first.receipt)
            # Both devices now have active leases: nothing else is claimable,
            # in particular A's second task must not jump the queue.
            self.assertIsNone(queue.claim(5, 10))

            self.assertTrue(queue.finish(second.receipt, 6))
            # B finished first, but A's unfinished oldest task blocks the
            # prefix, so reclaim cannot advance the floor.
            self.assertEqual(queue.reclaim(), 4)

            old_receipt = first.receipt
            old_deadline = first.deadline

        # Reopen while the A lease is still valid: receipts, deadlines and
        # device blocking survive, and no replacement receipt is issued.
        with LeasedWorkJournal(self.dir) as queue:
            self.assertIsNone(queue.claim(5, 10))
            # The deadline instant itself is outside the valid interval.
            self.assertFalse(queue.finish(old_receipt, old_deadline))

            # After expiry the same offset is re-leased with a new receipt.
            reclaimed = queue.claim(old_deadline, 10)
            self.assertEqual(reclaimed.offset, a1)
            self.assertEqual(reclaimed.key, "A")
            self.assertEqual(reclaimed.data, b"a-one")
            self.assertNotEqual(reclaimed.receipt, old_receipt)
            self.assertEqual(reclaimed.deadline, old_deadline + 10)

            # The late worker comes back with the stale receipt: it must not
            # finish the task nor release the new lease.
            self.assertFalse(queue.finish(old_receipt, old_deadline + 1))
            # A's second task is still blocked behind the re-leased head.
            self.assertIsNone(queue.claim(old_deadline + 1, 10))
            # B stays finished and never re-dispatches.
            self.assertFalse(queue.finish(second.receipt, old_deadline + 1))

            self.assertTrue(
                queue.finish(reclaimed.receipt, old_deadline + 2)
            )
            # Reclaim releases exactly the finished prefix (A1), keeping A2's
            # original offset even though B (later) was already finished.
            self.assertEqual(queue.reclaim(), a2)
            self.assertEqual(queue.floor, a2)

            next_task = queue.claim(old_deadline + 3, 10)
            self.assertEqual(
                (next_task.offset, next_task.key, next_task.data),
                (a2, "A", b""),
            )
            self.assertTrue(queue.finish(next_task.receipt, old_deadline + 4))
            # Now the finished prefix runs through the end.
            self.assertEqual(queue.reclaim(), queue.end)
            self.assertEqual(queue.floor, queue.end)

            # Appending after a full reclaim resumes at the old end.
            resumed = queue.submit("C", b"c-one")
            self.assertEqual(resumed, b1 + _HEADER.size + 1 + 1 + len(b"b-one"))
            again = queue.claim(old_deadline + 5, 10)
            self.assertEqual((again.offset, again.key, again.data),
                             (resumed, "C", b"c-one"))
            self.assertTrue(queue.finish(again.receipt, old_deadline + 6))

        # The exclusive handle is closed; inspect the real cut point and
        # expired cursors through a plain RetainedJournal.
        events_dir = self.dir / "events"
        with RetainedJournal(events_dir) as raw:
            self.assertEqual(raw.floor, resumed)
            self.assertEqual(raw.end,
                             resumed + _HEADER.size + 1 + 1 + len(b"c-one"))
            self.assertEqual(raw.checkpoint("leased-work"), resumed)
            with self.assertRaises(ExpiredOffset):
                raw.read(a1)
            with self.assertRaises(ExpiredOffset):
                raw.read(b1)
            rows = read_all_raw(raw)
            self.assertEqual([r.data for r in rows],
                             [bytes((1,)) + b"C" + b"c-one"])

        # Reopen once more: completed tasks never come back, offsets stand.
        with LeasedWorkJournal(self.dir) as queue:
            self.assertEqual(queue.floor, resumed)
            self.assertIsNone(queue.claim(old_deadline + 6, 10))
            fresh = queue.submit("D", b"d")
            pending = queue.claim(old_deadline + 7, 10)
            self.assertEqual(pending.offset, fresh)

    def test_receipt_rules_and_deadline_boundary(self):
        with LeasedWorkJournal(self.dir) as queue:
            offset = queue.submit("A", b"x")
            lease = queue.claim(0, 5)
            self.assertEqual(lease.deadline, 5)
            # now == deadline is already expired.
            self.assertFalse(queue.finish(lease.receipt, 5))
            # Re-lease at the deadline instant; the new receipt works.
            renewed = queue.claim(5, 5)
            self.assertEqual(renewed.offset, offset)
            self.assertNotEqual(renewed.receipt, lease.receipt)
            self.assertTrue(queue.finish(renewed.receipt, 9))
            # A used receipt cannot finish anything a second time.
            self.assertFalse(queue.finish(renewed.receipt, 9))
            # Unknown receipts are rejected without touching state.
            self.assertFalse(queue.finish("R0000000000-4", 9))
            self.assertIsNone(queue.claim(10, 5))

    def test_finish_validation_leaves_state_untouched(self):
        with LeasedWorkJournal(self.dir) as queue:
            offset = queue.submit("A", b"x")
            lease = queue.claim(0, 10)
            for bad_receipt in (None, "", b"R1", 123, 4.5):
                with self.subTest(bad_receipt=bad_receipt):
                    with self.assertRaises(ValueError):
                        queue.finish(bad_receipt, 1)
            for bad_now in (-1, 2 ** 62, 1.0, True, False, "1"):
                with self.subTest(bad_now=bad_now):
                    with self.assertRaises(ValueError):
                        queue.finish(lease.receipt, bad_now)
            # The valid lease survived all the rejected calls.
            self.assertTrue(queue.finish(lease.receipt, 2))
            self.assertEqual(offset, 4)

    def test_claim_clock_validation(self):
        with LeasedWorkJournal(self.dir) as queue:
            queue.submit("A", b"x")
            for bad_now in (-1, 2 ** 62, 1.5, True, False, "0"):
                with self.subTest(bad_now=bad_now):
                    with self.assertRaises(ValueError):
                        queue.claim(bad_now, 10)
            for bad_ttl in (0, 86401, -1, 2.0, True, False):
                with self.subTest(bad_ttl=bad_ttl):
                    with self.assertRaises(ValueError):
                        queue.claim(0, bad_ttl)
            # Bounds are inclusive.
            self.assertIsNotNone(queue.claim(2 ** 62 - 1, 86400))

    def test_submit_validation_never_appends(self):
        with LeasedWorkJournal(self.dir) as queue:
            bad_keys = ("", "x" * 33, "has space", "dot.name",
                        "slash/x", "café", 123, b"A")
            for key in bad_keys:
                with self.subTest(key=key):
                    with self.assertRaises(ValueError):
                        queue.submit(key, b"x")
            bad_data = ("not bytes", bytearray(b"x"), b"x" * 1_000_001, 7)
            for data in bad_data:
                with self.subTest(data=type(data)):
                    with self.assertRaises(ValueError):
                        queue.submit("A", data)
            # Every rejection left the journal empty.
            self.assertEqual(queue.end, 4)
            self.assertIsNone(queue.claim(0, 10))
            # Boundary values are accepted.
            offset = queue.submit("a-Z_0", b"x" * 1_000_000)
            self.assertEqual(offset, 4)
            queue.submit("a-Z_0", b"")

    def test_byte_quota_rejects_and_reclaim_frees_it(self):
        with LeasedWorkJournal(self.dir) as queue:
            payload = b"q" * 999_900
            # Each task frame costs 12 (AJ1) + 1 (key length) + 1 (key)
            # + 999_900 payload bytes; 67 fit inside the 64 MiB readable
            # budget and the 68th does not.
            offsets = [queue.submit("D", payload) for _ in range(67)]
            # Remaining readable budget is about 114 KiB; a near-1 MiB frame
            # no longer fits, while the 67 accepted frames are all on disk.
            with self.assertRaises(ValueError):
                queue.submit("D", payload)
            end_before = queue.end

            # Finish every task but do not reclaim yet: finished frames that
            # have not been reclaimed still occupy the quota.
            now = 0
            while True:
                lease = queue.claim(now, 86400)
                if lease is None:
                    break
                self.assertTrue(queue.finish(lease.receipt, now + 1))
                now += 1
            with self.assertRaises(ValueError):
                queue.submit("D", payload)
            self.assertEqual(queue.end, end_before)

            self.assertEqual(queue.reclaim(), end_before)
            self.assertEqual(queue.floor, end_before)
            renewed = queue.submit("D", b"after-reclaim")
            self.assertEqual(renewed, end_before)
            lease = queue.claim(now + 1, 10)
            self.assertEqual((lease.offset, lease.data),
                             (renewed, b"after-reclaim"))

    def test_later_device_cannot_jump_an_unfinished_older_task(self):
        with LeasedWorkJournal(self.dir) as queue:
            a1 = queue.submit("A", b"a1")
            b1 = queue.submit("B", b"b1")
            a2 = queue.submit("A", b"a2")
            b_lease = queue.claim(0, 10)
            self.assertEqual(b_lease.offset, a1)  # smallest head overall
            # While A is blocked, B can run.
            lease_b = queue.claim(1, 10)
            self.assertEqual(lease_b.offset, b1)
            self.assertTrue(queue.finish(lease_b.receipt, 2))
            self.assertEqual(queue.reclaim(), 4)       # A1 still pending
            # A2 must not be offered while A1 is unfinished.
            self.assertIsNone(queue.claim(2, 10))
            # Expire and finish A1; B's completion must not let reclaim skip
            # past A1 retroactively, but the next reclaim takes A1 plus any
            # contiguous finished frames.
            renewed = queue.claim(10, 10)
            self.assertEqual(renewed.offset, a1)
            self.assertTrue(queue.finish(renewed.receipt, 11))
            # A2 is still unfinished, so the cut stops right after A1.  B1,
            # although submitted after A2 in queue terms, sits inside the
            # contiguous finished byte prefix and is physically released with
            # A1; only the unfinished A2 survives, keeping its original offset.
            self.assertEqual(queue.reclaim(), a2)
            self.assertGreater(a2, b1)
            surviving = [a2]
            floor_after = a2

        # The exclusive queue handle is closed; inspect the raw cut point.
        with RetainedJournal(self.dir / "events") as raw:
            self.assertEqual(raw.floor, floor_after)
            self.assertEqual(
                sorted(r.offset for r in read_all_raw(raw)),
                sorted(surviving),
            )

    def test_single_active_handle_and_context_manager(self):
        with LeasedWorkJournal(self.dir) as queue:
            queue.submit("A", b"x")
            with self.assertRaises(RuntimeError):
                LeasedWorkJournal(self.dir)
        with LeasedWorkJournal(self.dir) as queue:
            self.assertEqual(queue.claim(0, 10).offset, 4)
        with self.assertRaises(ValueError):
            LeasedWorkJournal(self.dir, hook=123)

    def test_hook_event_names(self):
        events = []
        with LeasedWorkJournal(self.dir, hook=events.append) as queue:
            queue.submit("A", b"x")
            lease = queue.claim(0, 10)
            queue.finish(lease.receipt, 1)
        self.assertEqual(
            events,
            ["after_payload_sync", "after_finish_commit"],
        )

    def test_reopen_keeps_receipt_and_allows_its_ack(self):
        with LeasedWorkJournal(self.dir) as queue:
            offset = queue.submit("A", b"x")
            lease = queue.claim(100, 50)
            receipt = lease.receipt
        # Reopen: no replacement receipt is issued and the original one,
        # still unexpired, can confirm the task directly.
        with LeasedWorkJournal(self.dir) as queue:
            self.assertTrue(queue.finish(receipt, 120))
            self.assertEqual(queue.reclaim(), queue.end)
            self.assertIsNone(queue.claim(200, 10))

    def test_keys_are_case_sensitive_and_ordered_independently(self):
        with LeasedWorkJournal(self.dir) as queue:
            o_lower = queue.submit("dev", b"lower")
            o_upper = queue.submit("DEV", b"upper")
            # The two names are different devices; both heads are claimable.
            lease1 = queue.claim(0, 10)
            lease2 = queue.claim(1, 10)
            self.assertEqual({lease1.offset, lease2.offset},
                             {o_lower, o_upper})
            self.assertEqual({lease1.key, lease2.key}, {"dev", "DEV"})
            self.assertTrue(queue.finish(lease1.receipt, 2))
            self.assertTrue(queue.finish(lease2.receipt, 3))
            self.assertEqual(queue.reclaim(), queue.end)


# --- real subprocess exits inside the hooks -------------------------------

SUBMIT_HOOK_CHILD = r"""
import os, signal, sys
sys.path.insert(0, os.environ["PROJECT_DIR"])
from journal import LeasedWorkJournal

directory = sys.argv[1]
seen = {"n": 0}

def hook(event):
    if event == "after_payload_sync":
        seen["n"] += 1
        # Exit inside the hook of the second submit; the first task and the
        # synced payload of the second must both survive intact.
        if seen["n"] == 2:
            os.kill(os.getpid(), signal.SIGKILL)

queue = LeasedWorkJournal(directory, hook=hook)
try:
    queue.submit("A", b"prior-task")
    queue.submit("A", b"only-once")
finally:
    queue.close()
"""

FINISH_HOOK_CHILD = r"""
import os, signal, sys
sys.path.insert(0, os.environ["PROJECT_DIR"])
from journal import LeasedWorkJournal

directory = sys.argv[1]

def hook(event):
    if event == "after_finish_commit":
        os.kill(os.getpid(), signal.SIGKILL)

queue = LeasedWorkJournal(directory, hook=hook)
queue.submit("A", b"done-before-return")
queue.submit("A", b"later-task")
lease = queue.claim(0, 100)
try:
    queue.finish(lease.receipt, 1)
finally:
    queue.close()
"""


class HookCrashTest(unittest.TestCase):
    def setUp(self):
        self.root = TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.dir = Path(self.root.name) / "queue"
        self.submit_script = Path(self.root.name) / "submit_child.py"
        self.finish_script = Path(self.root.name) / "finish_child.py"
        self.submit_script.write_text(SUBMIT_HOOK_CHILD, encoding="utf-8")
        self.finish_script.write_text(FINISH_HOOK_CHILD, encoding="utf-8")

    def _run(self, script):
        env = dict(os.environ, PROJECT_DIR=PROJECT_DIR)
        proc = subprocess.run(
            [sys.executable, "-I", str(script), str(self.dir)],
            env=env, capture_output=True, text=True,
        )
        self.assertEqual(proc.returncode, -9, proc.stderr)

    def test_exit_after_payload_sync_keeps_exactly_one_claimable_task(self):
        self._run(self.submit_script)
        with LeasedWorkJournal(self.dir) as queue:
            # Both frames exist; the prior task is intact and the in-flight
            # task was synced exactly once, so it is claimable in order.
            first = queue.claim(100, 10)
            self.assertEqual(
                (first.offset, first.key, first.data),
                (4, "A", b"prior-task"),
            )
            self.assertTrue(queue.finish(first.receipt, 101))
            second = queue.claim(102, 10)
            self.assertEqual(
                (second.offset, second.key, second.data),
                (first.offset + _HEADER.size + 1 + 1 + len(b"prior-task"),
                 "A", b"only-once"),
            )
            self.assertIsNone(queue.claim(102, 10))
        with RetainedJournal(self.dir / "events") as raw:
            self.assertEqual(len(read_all_raw(raw)), 2)

    def test_exit_after_finish_commit_task_stays_finished_and_reclaimable(self):
        self._run(self.finish_script)
        with LeasedWorkJournal(self.dir) as queue:
            # The first task stays finished and never re-dispatches; the
            # later task is untouched and becomes the new device head.
            later = queue.claim(10_000, 10)
            self.assertIsNotNone(later)
            self.assertEqual(later.data, b"later-task")
            self.assertIsNone(queue.claim(10_000, 10))
            self.assertTrue(queue.finish(later.receipt, 10_001))
            # Both finished tasks now participate in prefix reclamation.
            self.assertEqual(queue.reclaim(), queue.end)
            self.assertEqual(queue.floor, queue.end)
        with RetainedJournal(self.dir / "events") as raw:
            self.assertEqual(read_all_raw(raw), [])
            self.assertEqual(raw.end, raw.floor)


# --- 48 MiB address-space backlog with bounded claim read amplification ---

BACKLOG_CHILD = r"""
import os, resource, sys
sys.path.insert(0, os.environ["PROJECT_DIR"])
from journal import LeasedWorkJournal

directory = sys.argv[1]
N = 54
PAYLOAD = 1_000_000 - 64

def rchar():
    with open("/proc/self/io", "rb") as handle:
        for line in handle:
            if line.startswith(b"rchar:"):
                return int(line.split()[1])
    raise RuntimeError("rchar unavailable")

queue = LeasedWorkJournal(directory)
a_offsets = []
for i in range(N):
    a_offsets.append(queue.submit("A", bytes([i % 256]) * PAYLOAD))
assert len(a_offsets) == N and a_offsets[0] == 4
head = queue.claim(0, 86400)
assert head.offset == 4 and head.key == "A"
b_payload = b"B-small-" + b"z" * 200
b_offset = queue.submit("B", b_payload)
queue.close()

# Reopen in the same constrained process; the A lease must remain valid.
queue = LeasedWorkJournal(directory)
before = rchar()
lease = queue.claim(1, 86400)
after = rchar()
assert lease is not None, "B must be claimable while A is leased"
assert lease.offset == b_offset, lease.offset
assert lease.key == "B"
assert lease.data == b_payload
delta = after - before
assert delta <= 2 * 1024 * 1024, delta
# A's second task is still blocked by the valid head lease.
assert queue.claim(2, 86400) is None
queue.close()
print("BACKLOG_OK rchar_delta=%d b_offset=%d" % (delta, b_offset))
"""


@unittest.skipUnless(sys.platform.startswith("linux"), "Linux /proc and rlimit")
class BacklogReadAmplificationTest(unittest.TestCase):
    def setUp(self):
        self.root = TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.dir = Path(self.root.name) / "queue"
        self.script = Path(self.root.name) / "backlog_child.py"
        self.script.write_text(BACKLOG_CHILD, encoding="utf-8")

    def test_claim_under_48mib_address_space_reads_at_most_2mib(self):
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
                "backlog child failed\n"
                f"stdout:\n{proc.stdout}\n"
                f"stderr:\n{proc.stderr}"
            )
        self.assertIn("BACKLOG_OK", proc.stdout)
        # Sanity check on the prepared backlog itself: 54 near-1MiB A frames
        # plus the small B frame, with A's head lease persisted.
        with LeasedWorkJournal(self.dir) as queue:
            self.assertGreater(queue.end - queue.floor, 50 * 1024 * 1024)
            state = json.loads((self.dir / "leases.json").read_text())
            self.assertIn("4", state["leases"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
