"""Tests for RetirableJournal source erasure and cursor preservation."""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest

try:
    import resource
except ImportError:  # pragma: no cover - non-Unix hosts
    resource = None

from journal import (
    ExpiredOffset,
    RetainedJournal,
    RetirableJournal,
)

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
MIB = 1024 * 1024


def tree_size(directory):
    total = 0
    for root, _dirs, files in os.walk(directory):
        for name in files:
            path = os.path.join(root, name)
            total += os.lstat(path).st_size
    return total


def file_count(directory):
    return sum(len(files) for _, _dirs, files in os.walk(directory))


def read_all(journal, limit=16):
    rows = []
    offset = journal.floor
    while True:
        batch = journal.read(offset, limit=limit)
        if not batch:
            break
        rows.extend(batch)
        offset = batch[-1].next_offset
    return rows


CRASH_CHILD = r"""
import os, signal, sys
sys.path.insert(0, os.environ["PROJECT_DIR"])
from journal import RetirableJournal

directory, phase = sys.argv[1], sys.argv[2]

def hook(observed):
    if observed == phase:
        os.kill(os.getpid(), signal.SIGKILL)

with RetirableJournal(directory, hook=hook) as journal:
    journal.retire("A")
"""


LARGE_CHILD = r"""
import hashlib
import json
import os
import sys

sys.path.insert(0, os.environ["PROJECT_DIR"])
from journal import RetirableJournal

mode = sys.argv[1]
directory = sys.argv[2]
oracle_path = sys.argv[3]


def wchar():
    with open("/proc/self/io", "rb") as handle:
        for line in handle:
            if line.startswith(b"wchar:"):
                return int(line.split()[1])
    raise RuntimeError("wchar unavailable")


def big_body(subject, index):
    prefix = ("BODY-%s-%03d-" % (subject, index)).encode("ascii")
    fill = 65 + (index % 26)
    return prefix + bytes([fill]) * (1_000_000 - len(prefix))


def extra_body():
    prefix = b"EXTRA-B-"
    return prefix + b"q" * (1_000_000 - len(prefix))


if mode == "setup":
    journal = RetirableJournal(directory)
    slow = journal.register("slow")
    events = []

    def add(subject, data):
        offset = journal.append(subject, data)
        events.append({
            "o": offset,
            "n": journal.end,
            "s": subject,
            "l": len(data),
            "h": hashlib.sha256(data).hexdigest(),
        })

    for i in range(67):
        subject = "A" if i % 3 != 2 else "B"  # 45 A, 22 B
        add(subject, big_body(subject, i))
        for k in range(8):
            add("small", ("small-%03d-%d" % (i, k)).encode("ascii"))

    before_reject = journal.end
    rejected = False
    try:
        journal.append("B", extra_body())
    except ValueError:
        rejected = True
    assert rejected
    assert journal.end == before_reject
    assert journal.checkpoint("slow") == slow == 4

    with open(oracle_path, "w", encoding="utf-8") as handle:
        json.dump({"slow": slow, "events": events}, handle)
    journal.close()

elif mode == "retire":
    io_path = sys.argv[4]
    journal = RetirableJournal(directory)
    start = wchar()

    def hook(phase):
        if phase == "after_first_removal":
            delta = wchar() - start
            with open(io_path, "w", encoding="utf-8") as handle:
                json.dump({"wchar": delta}, handle)
            os.kill(os.getpid(), 9)

    journal._hook = hook
    journal.retire("A")
    raise AssertionError("retire should have been killed")

elif mode == "reopen":
    open_start = wchar()
    journal = RetirableJournal(directory)
    open_wchar = wchar() - open_start
    with open(oracle_path, "r", encoding="utf-8") as handle:
        oracle = json.load(handle)
    events = oracle["events"]

    assert journal.checkpoint("slow") == oracle["slow"] == 4
    cursor = journal.floor
    index = 0
    while index < len(events):
        rows = journal.read(cursor, limit=4)
        assert rows, "verification batch was empty"
        for row in rows:
            expected = events[index]
            assert row.offset == expected["o"]
            assert row.next_offset == expected["n"]
            assert row.subject == expected["s"]
            if expected["s"] == "A":
                assert row.data is None
            else:
                assert isinstance(row.data, bytes)
                assert len(row.data) == expected["l"]
                assert hashlib.sha256(row.data).hexdigest() == expected["h"]
            index += 1
            cursor = row.next_offset
    assert index == len(events)
    assert cursor == journal.end

    extra_offset = journal.append("B", extra_body())
    check = journal.read(extra_offset, limit=1)[0]
    assert check.data == extra_body()
    assert journal.checkpoint("slow") == 4
    journal.close()
    print("LARGE_OK " + json.dumps({
        "open_wchar": open_wchar,
        "extra_offset": extra_offset,
    }))
else:
    raise AssertionError(mode)
"""


class AlternatingAcceptanceTest(unittest.TestCase):
    def setUp(self):
        self.root = TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.dir = Path(self.root.name) / "audit"
        self.crash_script = Path(self.root.name) / "crash_child.py"
        self.crash_script.write_text(CRASH_CHILD, encoding="utf-8")
        self.env = dict(os.environ, PROJECT_DIR=PROJECT_DIR)

    def _run_crash_child(self, phase):
        proc = subprocess.run(
            [sys.executable, "-I", str(self.crash_script),
             str(self.dir), phase],
            env=self.env,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, -9, proc.stderr)

    def test_before_kill_then_retire_preserves_cursors_and_prunes(self):
        a_body = b"A diagnostic body\n" * 8
        b_empty = b""
        b_body = b"B diagnostic body"
        b_tail = b"later B event"

        with RetirableJournal(self.dir) as journal:
            o1 = journal.append("A", a_body)
            o2 = journal.append("B", b_empty)
            o3 = journal.append("A", b"")
            o4 = journal.append("B", b_body)
            o5 = journal.append("B", b_tail)
            slow = journal.register("slow")
            self.assertEqual((o1, slow), (4, 4))
            boundaries = [o1, o2, o3, o4, o5]

        # Kill inside before_erase: no retirement state may be committed.
        self._run_crash_child("before_erase")

        with RetirableJournal(self.dir) as journal:
            rows = read_all(journal)
            self.assertEqual(
                [(r.offset, r.subject, r.data) for r in rows],
                [
                    (o1, "A", a_body),
                    (o2, "B", b""),
                    (o3, "A", b""),
                    (o4, "B", b_body),
                    (o5, "B", b_tail),
                ],
            )
            self.assertEqual(journal.checkpoint("slow"), 4)
            # The source remains admissible after the pre-erasure exit.
            o6 = journal.append("A", b"A after recovery")
            self.assertEqual(o6, rows[-1].next_offset)
            end_before_retire = journal.end

        phases = []
        with RetirableJournal(self.dir, hook=phases.append) as journal:
            self.assertTrue(journal.retire("A"))
            self.assertEqual(
                phases, ["before_erase", "after_first_removal"]
            )
            self.assertFalse(journal.retire("A"))
            # Retry must not invoke either hook again.
            self.assertEqual(
                phases, ["before_erase", "after_first_removal"]
            )

            self.assertEqual(journal.floor, 4)
            self.assertEqual(journal.end, end_before_retire)
            self.assertEqual(journal.checkpoint("slow"), 4)

            rows = read_all(journal)
            self.assertEqual([r.offset for r in rows],
                             boundaries + [o6])
            self.assertEqual(
                [(r.subject, r.data) for r in rows],
                [
                    ("A", None),
                    ("B", b""),
                    ("A", None),
                    ("B", b_body),
                    ("B", b_tail),
                    ("A", None),
                ],
            )
            # Continuation through the normal next_offset cursor works.
            batch = journal.read(o4, limit=2)
            self.assertEqual([r.offset for r in batch], [o4, o5])
            self.assertEqual(batch[1].data, b_tail)

            with self.assertRaises(ValueError):
                journal.append("A", b"must be rejected")

            # Retirement alone did not advance the consumer.  Ack to o5, which
            # reclaims o1..o4 while leaving o5 and o6 readable.
            journal.ack("slow", o5)
            self.assertEqual(journal.checkpoint("slow"), o5)
            self.assertEqual(journal.prune(), o5)
            self.assertEqual(journal.floor, o5)
            self.assertEqual(journal.checkpoint("slow"), o5)

            surviving = journal.read(o5, limit=16)
            self.assertEqual(
                [(r.offset, r.subject, r.data) for r in surviving],
                [(o5, "B", b_tail), (o6, "A", None)],
            )
            with self.assertRaises(ExpiredOffset):
                journal.read(o1)

        # The real cut/cursor is inspectable through the underlying log.
        with RetainedJournal(self.dir / "events") as raw:
            self.assertEqual(raw.floor, o5)
            self.assertEqual(raw.end, end_before_retire)
            self.assertEqual(raw.checkpoint("slow"), o5)
            with self.assertRaises(ExpiredOffset):
                raw.read(o1)
            self.assertEqual(
                [r.offset for r in raw.read(o5, limit=16)], [o5, o6]
            )

        with RetirableJournal(self.dir) as journal:
            self.assertEqual(journal.checkpoint("slow"), o5)
            self.assertEqual(journal.read(o5)[0].data, b_tail)


class SemanticsAndSpaceTest(unittest.TestCase):
    def setUp(self):
        self.root = TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.dir = Path(self.root.name) / "audit"

    def test_subject_data_and_read_validation(self):
        with RetirableJournal(self.dir) as journal:
            offset = journal.append("A", b"abc")
            journal.append("B", b"")

            for bad_subject in ["", "a" * 33, "a/b", "a.b", 1, None, True]:
                with self.assertRaises(ValueError):
                    journal.append(bad_subject, b"x")

            with self.assertRaises(ValueError):
                journal.append("A", "not-bytes")
            with self.assertRaises(ValueError):
                journal.append("A", b"x" * 1_000_001)

            for bad_offset in [True, False, 1.5, "4", [4]]:
                with self.assertRaises(ValueError):
                    journal.read(bad_offset)
            for bad_limit in [0, 17, True, False, 1.0, "16"]:
                with self.assertRaises(ValueError):
                    journal.read(4, limit=bad_limit)

            self.assertEqual(journal.read(journal.end), [])
            self.assertEqual(journal.read(limit=1)[0].offset, 4)
            self.assertTrue(offset == 4)

    def test_subject_limit_includes_retired_never_written_sources(self):
        with RetirableJournal(self.dir) as journal:
            self.assertTrue(journal.retire("never-written"))
            for i in range(31):
                journal.append(f"S{i:02d}", b"")
            with self.assertRaises(ValueError):
                journal.append("S31", b"")
            with self.assertRaises(ValueError):
                journal.append("never-written", b"")

        other = self.dir.parent / "audit-full"
        with RetirableJournal(other) as journal:
            for i in range(32):
                journal.append(f"S{i:02d}", b"")
            with self.assertRaises(ValueError):
                journal.retire("never-written")
            self.assertTrue(journal.retire("S00"))
            with self.assertRaises(ValueError):
                journal.append("S32", b"")
            with self.assertRaises(ValueError):
                journal.append("S00", b"")

    def test_identical_content_in_other_source_survives_raw(self):
        payload = b"shared-diagnostic-bytes" * 100
        with RetirableJournal(self.dir) as journal:
            journal.append("A", payload)
            journal.append("B", payload)
            self.assertTrue(journal.retire("A"))
            rows = read_all(journal)
            self.assertIsNone(rows[0].data)
            self.assertEqual(rows[1].data, payload)

        blob = (self.dir / "body-B.bin").read_bytes()
        self.assertIn(payload, blob)
        self.assertNotIn("body-A.bin", {p.name for p in self.dir.iterdir()})

    def test_prune_removes_prefix_bodies_and_keeps_later_bytes(self):
        a1 = b"A-one-" + b"a" * 1000
        b1 = b"B-one-" + b"b" * 2000
        a2 = b"A-two-" + b"a" * 3000
        b2 = b"B-two-" + b"b" * 4000
        with RetirableJournal(self.dir) as journal:
            o1 = journal.append("A", a1)
            o2 = journal.append("B", b1)
            o3 = journal.append("A", a2)
            o4 = journal.append("B", b2)
            journal.register("slow")
            journal.ack("slow", o3)
            self.assertEqual(journal.prune(), o3)

            rows = journal.read(o3, limit=16)
            self.assertEqual([r.data for r in rows], [a2, b2])
            with self.assertRaises(ExpiredOffset):
                journal.read(o1)
            with self.assertRaises(ExpiredOffset):
                journal.read(o2)

            # The old prefix bytes are not still present in a pack.
            b_pack = (self.dir / "body-B.bin").read_bytes()
            self.assertNotIn(b1, b_pack)
            self.assertIn(b2, b_pack)
            a_pack = (self.dir / "body-A.bin").read_bytes()
            self.assertNotIn(a1, a_pack)
            self.assertIn(a2, a_pack)

            journal.ack("slow", o4)
            journal.prune()
            self.assertEqual(journal.read(o4)[0].data, b2)

    def test_consumer_methods_and_single_handle(self):
        with RetirableJournal(self.dir) as journal:
            journal.append("A", b"x")
            self.assertEqual(journal.register("c"), 4)
            self.assertEqual(journal.checkpoint("c"), 4)
            journal.ack("c", journal.end)
            self.assertEqual(journal.checkpoint("c"), journal.end)
            self.assertTrue(journal.drop("c"))
            self.assertFalse(journal.drop("c"))
            with self.assertRaises(KeyError):
                journal.checkpoint("c")
        with self.assertRaises(RuntimeError):
            with RetirableJournal(self.dir) as first:
                RetirableJournal(self.dir)


@unittest.skipUnless(
    sys.platform.startswith("linux") and os.path.exists("/proc/self/io"),
    "Linux /proc and RLIMIT_AS",
)
class LargeConstrainedRetirementTest(unittest.TestCase):
    LIMIT = 48 * MIB

    def setUp(self):
        self.root = TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.dir = Path(self.root.name) / "audit"
        self.script = Path(self.root.name) / "large_child.py"
        self.oracle = Path(self.root.name) / "oracle.json"
        self.io_path = Path(self.root.name) / "retire-io.json"
        self.script.write_text(LARGE_CHILD, encoding="utf-8")
        self.env = dict(os.environ, PROJECT_DIR=PROJECT_DIR)

    def _run(self, *args):
        def limit_address_space():
            import resource
            resource.setrlimit(
                resource.RLIMIT_AS, (self.LIMIT, self.LIMIT)
            )

        return subprocess.run(
            [sys.executable, "-I", str(self.script), *args],
            env=self.env,
            capture_output=True,
            text=True,
            preexec_fn=limit_address_space,
            check=False,
        )

    def test_67_large_bodies_retire_kill_reopen_and_space_limits(self):
        setup = self._run("setup", str(self.dir), str(self.oracle))
        self.assertEqual(setup.returncode, 0, setup.stderr)
        oracle = json.loads(self.oracle.read_text(encoding="utf-8"))
        events = oracle["events"]
        self.assertEqual(len(events), 67 + 67 * 8)

        retire = self._run(
            "retire", str(self.dir), str(self.oracle), str(self.io_path)
        )
        self.assertEqual(retire.returncode, -9, retire.stderr)

        # The after_first_removal action really unlinked the A pack before the
        # child was killed.
        self.assertFalse((self.dir / "body-A.bin").exists())
        a_numbers = [i for i in range(67) if i % 3 != 2]
        pattern = re.compile(
            b"BODY-A-(?:"
            + b"|".join(f"{i:03d}".encode("ascii") for i in a_numbers)
            + b")-"
        )
        for path in self.dir.rglob("*"):
            if not path.is_file():
                continue
            with path.open("rb") as handle:
                blob = handle.read()
            self.assertIsNone(pattern.search(blob), f"A body remains in {path}")

        reopened = self._run(
            "reopen", str(self.dir), str(self.oracle)
        )
        self.assertEqual(reopened.returncode, 0, reopened.stderr)
        marker = "LARGE_OK "
        line = next(
            line for line in reopened.stdout.splitlines()
            if line.startswith(marker)
        )
        result = json.loads(line[len(marker):])

        retire_wchar = json.loads(
            self.io_path.read_text(encoding="utf-8")
        )["wchar"]
        surviving_bytes = sum(e["l"] for e in events if e["s"] != "A")
        self.assertLessEqual(
            retire_wchar + result["open_wchar"],
            surviving_bytes + 3 * MIB,
            (retire_wchar, result["open_wchar"], surviving_bytes),
        )

        live_bytes = surviving_bytes + 1 * 1_000_000
        self.assertLessEqual(
            tree_size(self.dir), live_bytes + 2 * MIB
        )
        self.assertLessEqual(file_count(self.dir), 256)

        # Raw search after recovery and the quota-restoring B append.
        for path in self.dir.rglob("*"):
            if not path.is_file():
                continue
            with path.open("rb") as handle:
                blob = handle.read()
            self.assertIsNone(pattern.search(blob), f"A body remains in {path}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
