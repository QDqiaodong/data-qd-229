"""Tests for ReplicaJournal: out-of-order ingest and gap-safe replay."""

import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest

from journal import (
    Journal,
    Record,
    ReplicaBackpressure,
    ReplicaConflict,
    ReplicaJournal,
    _HEADER,
)

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

try:
    import resource
except ImportError:  # pragma: no cover - non-Linux hosts
    resource = None


class AHScenarioTest(unittest.TestCase):
    def setUp(self):
        self._root = TemporaryDirectory()
        self.addCleanup(self._root.cleanup)
        root = Path(self._root.name)
        self.source_path = root / "source.aj"
        self.replica_dir = root / "replica"

        with Journal(self.source_path) as journal:
            for letter in b"ABCDEFGH":
                journal.append(bytes([letter]))
        with Journal(self.source_path) as journal:
            rows = journal.read(4, limit=8)
        self.records = {chr(row.data[0]): row for row in rows}

    def open_replica(self):
        return ReplicaJournal(
            self.replica_dir,
            source_id="source-1",
            max_pending_bytes=2 * (_HEADER.size + 1),
        )

    def test_out_of_order_duplicates_conflict_reopen_and_replay(self):
        f = self.records["F"]
        g = self.records["G"]
        d = self.records["D"]
        e = self.records["E"]
        a = self.records["A"]
        b = self.records["B"]
        c = self.records["C"]

        self.assertTrue(issubclass(ReplicaConflict, ValueError))
        self.assertTrue(issubclass(ReplicaBackpressure, ValueError))

        with self.open_replica() as replica:
            self.assertEqual(replica.end, 4)
            self.assertEqual(replica.pending_bytes, 0)

            # Budget is exactly two one-byte frames.
            self.assertEqual(replica.ingest([f, g]), 4)
            self.assertEqual(replica.end, 4)
            self.assertEqual(replica.pending_bytes, 2 * (_HEADER.size + 1))

            # Tuple input is accepted; identical retransmission bills once.
            self.assertEqual(replica.ingest((f, g)), 4)
            self.assertEqual(replica.pending_bytes, 2 * (_HEADER.size + 1))

            # A farther received frame cannot bypass the gap.
            with self.assertRaises(ValueError):
                replica.read(f.offset)
            self.assertEqual(replica.read(4), [])

        # Pending frames, source identity, and budget survive close/reopen.
        with self.open_replica() as replica:
            self.assertEqual(replica.end, 4)
            self.assertEqual(replica.pending_bytes, 2 * (_HEADER.size + 1))

            # D would be a third pending frame while E is still missing.
            with self.assertRaises(ReplicaBackpressure):
                replica.ingest([d])
            self.assertEqual(replica.end, 4)
            self.assertEqual(replica.pending_bytes, 2 * (_HEADER.size + 1))

            # A-C connect to D's boundary and become public.  F/G remain the
            # only billable pending frames.
            self.assertEqual(replica.ingest([a, b, c]), d.offset)
            self.assertEqual(replica.end, d.offset)
            self.assertEqual(replica.pending_bytes, 2 * (_HEADER.size + 1))

            bad_f = Record(f.offset, f.next_offset, b"X")
            with self.assertRaises(ReplicaConflict):
                replica.ingest([d, e, bad_f])

            # The whole batch is rejected: D/E are still not visible.
            self.assertEqual(replica.end, d.offset)
            self.assertEqual(replica.read(d.offset), [])
            self.assertEqual(replica.pending_bytes, 2 * (_HEADER.size + 1))

            # D/E plus the original F overlap old F and connect through G.
            self.assertEqual(replica.ingest([d, e, f]), g.next_offset)
            self.assertEqual(replica.end, g.next_offset)
            self.assertEqual(replica.pending_bytes, 0)

        # Reopen again and read D through G from the old cursor.
        with self.open_replica() as replica:
            rows = replica.read(d.offset, limit=4)
            self.assertEqual(rows, [d, e, f, g])
            self.assertEqual([row.data for row in rows],
                             [b"D", b"E", b"F", b"G"])

            # Replaying does not consume records.
            self.assertEqual(replica.read(d.offset, limit=4), [d, e, f, g])

            # Repeated public contiguous batch has no effect.
            self.assertEqual(
                replica.ingest([
                    self.records[name] for name in "ABCDEFG"
                ]),
                g.next_offset,
            )
            self.assertEqual(replica.pending_bytes, 0)

            # Start one byte inside D and make the frame cross into E.
            inside_offset = d.offset + 1
            overlapping = Record(
                inside_offset,
                inside_offset + _HEADER.size + 1,
                b"z",
            )
            with self.assertRaises(ReplicaConflict):
                replica.ingest([overlapping])

            # Conflict leaves replay and state unchanged.
            self.assertEqual(replica.read(d.offset, limit=4), [d, e, f, g])
            self.assertEqual(replica.pending_bytes, 0)

        # Wrong source identity is a plain rejection and does not reset data.
        with self.assertRaises(ValueError):
            ReplicaJournal(
                self.replica_dir,
                source_id="different",
                max_pending_bytes=2 * (_HEADER.size + 1),
            )
        with self.assertRaises(ValueError):
            ReplicaJournal(
                self.replica_dir,
                source_id="source-1",
                max_pending_bytes=2 * (_HEADER.size + 1) + 1,
            )

        with self.open_replica() as replica:
            self.assertEqual(replica.read(d.offset, limit=4), [d, e, f, g])


class ValidationTest(unittest.TestCase):
    def setUp(self):
        self._root = TemporaryDirectory()
        self.addCleanup(self._root.cleanup)
        self.directory = Path(self._root.name) / "replica"

    def test_constructor_arguments(self):
        for bad_source in ("", "x" * 65, "bad id", "dot.x", "café", 123):
            with self.subTest(bad_source=bad_source):
                with self.assertRaises(ValueError):
                    ReplicaJournal(
                        self.directory / "a",
                        source_id=bad_source,
                        max_pending_bytes=0,
                    )
        for bad_budget in (-1, 64 * 1024 * 1024 + 1, 1.5, True, "26"):
            with self.subTest(bad_budget=bad_budget):
                with self.assertRaises(ValueError):
                    ReplicaJournal(
                        self.directory / "b",
                        source_id="ok",
                        max_pending_bytes=bad_budget,
                    )

    def test_ingest_and_read_validation(self):
        with ReplicaJournal(
            self.directory, source_id="v_1-2", max_pending_bytes=1024
        ) as replica:
            with self.assertRaises(ValueError):
                replica.ingest([])
            with self.assertRaises(ValueError):
                replica.ingest([object()])
            with self.assertRaises(ValueError):
                replica.ingest([object()] * 257)
            with self.assertRaises(ValueError):
                replica.ingest([Record(4, 99, b"x")])
            with self.assertRaises(ValueError):
                replica.ingest([Record(True, True + _HEADER.size + 1, b"x")])
            with self.assertRaises(ValueError):
                replica.ingest([
                    Record(4, 4 + _HEADER.size + 1, b"A"),
                    # Not equal to the first record's next_offset.
                    Record(30, 30 + _HEADER.size + 1, b"C"),
                ])

            big = b"x" * (1024 * 1024 + 1)
            with self.assertRaises(ValueError):
                replica.ingest([
                    Record(4, 4 + _HEADER.size + len(big), big)
                ])

            self.assertEqual(replica.end, 4)
            self.assertEqual(replica.read(4), [])
            for bad_offset in (3, 5, 17, 2 ** 40, 4.0, True):
                with self.subTest(bad_offset=bad_offset):
                    with self.assertRaises(ValueError):
                        replica.read(bad_offset)
            for bad_limit in (0, 9, -1, 1.5, True, "4"):
                with self.subTest(bad_limit=bad_limit):
                    with self.assertRaises(ValueError):
                        replica.read(4, limit=bad_limit)

    def test_conflict_is_reported_before_budget(self):
        f = Record(100, 100 + _HEADER.size + 1, b"F")
        with ReplicaJournal(
            self.directory, source_id="c", max_pending_bytes=13
        ) as replica:
            replica.ingest([f])
            conflicting = Record(f.offset, f.next_offset, b"X")
            with self.assertRaises(ReplicaConflict):
                replica.ingest([conflicting])
            self.assertEqual(replica.pending_bytes, 13)


CHILD_SCRIPT = r"""
import os
import stat
import sys

project_dir, root = sys.argv[1], sys.argv[2]
sys.path.insert(0, project_dir)

from journal import Journal, ReplicaJournal

N = 48
PAYLOAD_SIZE = 1024 * 1024 - 64
FRAME_SIZE = 12 + PAYLOAD_SIZE
BUDGET = (N - 1) * FRAME_SIZE

source = Journal(os.path.join(root, "source.aj"))
offsets = []
# Samples are generated one at a time; no list of large payloads is kept.
for i in range(N):
    offsets.append(source.append(bytes([i + 1]) * PAYLOAD_SIZE))

replica_dir = os.path.join(root, "replica")
replica = ReplicaJournal(
    replica_dir, source_id="large-frames", max_pending_bytes=BUDGET
)

# Last frame arrives first.
last = source.read(offsets[-1], limit=1)[0]
assert replica.ingest([last]) == 4
assert replica.end == 4
assert replica.read(4, limit=4) == []

# Check real allocated blocks immediately after only the last frame.
blocks = 0
for dirpath, _dirnames, filenames in os.walk(replica_dir):
    for name in filenames:
        path = os.path.join(dirpath, name)
        st = os.lstat(path)
        assert stat.S_ISREG(st.st_mode)
        blocks += st.st_blocks * 512
encoded_length = last.next_offset - last.offset
block_limit = encoded_length + 4 * 1024 * 1024
assert blocks <= block_limit, (blocks, block_limit)

# Deliver the remaining suffix in reverse order, one frame per call.
for i in range(N - 2, 0, -1):
    row = source.read(offsets[i], limit=1)[0]
    assert replica.ingest([row]) == 4
    assert replica.end == 4

assert replica.pending_bytes == BUDGET
assert replica.read(4, limit=8) == []

# Reopen while the first-frame hole is still missing.
replica.close()
replica = ReplicaJournal(
    replica_dir, source_id="large-frames", max_pending_bytes=BUDGET
)
assert replica.end == 4
assert replica.pending_bytes == BUDGET
assert replica.read(4, limit=4) == []

# The final delivery is the first frame and closes the only gap.
first = source.read(offsets[0], limit=1)[0]
final_end = offsets[-1] + FRAME_SIZE
assert replica.ingest([first]) == final_end
assert replica.end == final_end
assert replica.pending_bytes == 0
source.close()
replica.close()

# Reopen again and replay in small batches with the original offsets.
replica = ReplicaJournal(
    replica_dir, source_id="large-frames", max_pending_bytes=BUDGET
)
cursor = 4
seen = 0
while True:
    rows = replica.read(cursor, limit=4)
    if not rows:
        break
    for row in rows:
        assert row.offset == offsets[seen], (seen, row.offset, offsets[seen])
        expected_next = (
            offsets[seen + 1] if seen + 1 < N else final_end
        )
        assert row.next_offset == expected_next
        assert row.data == bytes([seen + 1]) * PAYLOAD_SIZE
        seen += 1
    cursor = rows[-1].next_offset

assert seen == N
replica.close()
print("REPLICA_MEMORY_OK blocks=%d limit=%d" % (blocks, block_limit))
"""


@unittest.skipUnless(sys.platform.startswith("linux"), "Linux RLIMIT_AS test")
@unittest.skipIf(resource is None, "resource module unavailable")
class MemoryAndBlockUsageTest(unittest.TestCase):
    def setUp(self):
        self._root = TemporaryDirectory()
        self.addCleanup(self._root.cleanup)
        self.root = Path(self._root.name)
        self.script = self.root / "memory_child.py"
        self.script.write_text(CHILD_SCRIPT, encoding="utf-8")

    def test_forty_eight_near_mib_frames_in_40mib_address_space(self):
        def apply_address_limit():
            limit = 40 * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_AS, (limit, limit))

        proc = subprocess.run(
            [
                sys.executable,
                "-I",
                str(self.script),
                PROJECT_DIR,
                self._root.name,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            preexec_fn=apply_address_limit,
            check=False,
        )
        if proc.returncode != 0:
            self.fail(
                "memory child failed\n"
                f"stdout:\n{proc.stdout}\n"
                f"stderr:\n{proc.stderr}"
            )
        self.assertIn("REPLICA_MEMORY_OK", proc.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
