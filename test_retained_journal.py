"""Tests for RetainedJournal: cursor-driven physical reclamation."""
import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest

from journal import (
    MAGIC,
    Corruption,
    ExpiredOffset,
    MAX_RECORD,
    RetainedJournal,
    _HEADER,
)

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))


def tree_size(directory):
    """Sum of st_size of every regular file in the directory tree."""
    total = 0
    for root, _dirs, files in os.walk(directory):
        for name in files:
            total += os.lstat(os.path.join(root, name)).st_size
    return total


def file_count(directory):
    return sum(len(files) for _, _dirs, files in os.walk(directory))


def read_all(journal, limit=256):
    rows = []
    offset = journal.floor
    while True:
        batch = journal.read(offset, limit=limit)
        if not batch:
            break
        rows.extend(batch)
        offset = batch[-1].next_offset
    return rows


def boundaries(rows):
    return [r.offset for r in rows]


class BasicBehaviourTest(unittest.TestCase):
    def setUp(self):
        self.root = TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.dir = Path(self.root.name) / "journal"

    def test_floor_end_offsets_and_empty_payload(self):
        with RetainedJournal(self.dir) as log:
            self.assertEqual(log.floor, 4)
            self.assertEqual(log.end, 4)
            self.assertEqual(log.read(), [])
            first = log.append(b"one")
            self.assertEqual(first, 4)
            empty = log.append(b"")
            third = log.append(b"three")
            self.assertEqual(empty, first + _HEADER.size + 3)
            self.assertEqual(third, empty + _HEADER.size)
            self.assertEqual(log.end, third + _HEADER.size + 5)
            rows = log.read()
            self.assertEqual([r.data for r in rows], [b"one", b"", b"three"])
            self.assertEqual(rows[1].next_offset, third)
            # Reopen keeps the full prefix.
        with RetainedJournal(self.dir) as log:
            self.assertEqual(log.recovered_bytes, 0)
            self.assertEqual([r.data for r in read_all(log)],
                             [b"one", b"", b"three"])

    def test_read_offset_rules(self):
        with RetainedJournal(self.dir) as log:
            o1 = log.append(b"aaaa")
            o2 = log.append(b"bbbb")
            log.register("c")
            log.ack("c", o2)
            log.prune()
            self.assertEqual(log.floor, o2)
            # Default read starts at floor.
            self.assertEqual([r.offset for r in log.read()], [o2])
            # At end -> empty list.
            self.assertEqual(log.read(log.end), [])
            # Below floor -> ExpiredOffset (a ValueError, not an empty list).
            with self.assertRaises(ExpiredOffset):
                log.read(o1)
            self.assertIsInstance(ExpiredOffset("x"), ValueError)
            # Frame-internal offset -> plain ValueError.
            with self.assertRaises(ValueError):
                log.read(o2 + 1)
            # Beyond end -> ValueError.
            with self.assertRaises(ValueError):
                log.read(log.end + 12)
            # Bad argument types / limit.
            with self.assertRaises(ValueError):
                log.read(1.0)
            with self.assertRaises(ValueError):
                log.read(o2, limit=0)

    def test_append_validation(self):
        with RetainedJournal(self.dir) as log:
            with self.assertRaises(ValueError):
                log.append("not bytes")
            with self.assertRaises(ValueError):
                log.append(b"x" * (MAX_RECORD + 1))

    def test_read_spans_segments_with_stable_offsets(self):
        with RetainedJournal(self.dir) as log:
            offsets = [log.append(bytes([i % 256]) * 70000) for i in range(40)]
            log.register("c")
            self.assertGreater(len(list(Path(self.dir).glob("seg-*.aj"))), 1)
            # A large limit must transparently cross segment files.
            rows = log.read(offsets[3], limit=1000)
            self.assertEqual(boundaries(rows), offsets[3:])
            self.assertTrue(all(r.next_offset == r.offset + _HEADER.size +
                                len(r.data) for r in rows))
            # Prune the first 10 frames; surviving offsets must not move.
            log.ack("c", offsets[10])
            log.prune()
            rows = read_all(log)
            self.assertEqual(boundaries(rows), offsets[10:])
            self.assertEqual(rows[0].data, bytes([10 % 256]) * 70000)

    def test_single_active_handle(self):
        with RetainedJournal(self.dir) as _log:
            with self.assertRaises(RuntimeError):
                RetainedJournal(self.dir)


class ConsumerTest(unittest.TestCase):
    def setUp(self):
        self.root = TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.dir = Path(self.root.name) / "journal"

    def test_register_names_and_cap(self):
        with RetainedJournal(self.dir) as log:
            self.assertEqual(log.register("A-Z_0"), 4)
            self.assertEqual(log.register("A-Z_0"), 4)  # repeat, no reset
            for i in range(15):
                log.register(f"c{i:02d}")
            with self.assertRaises(ValueError):
                log.register("one-too-many")
            for bad in ("", "x" * 33, "has space", "dot.name", "slash/x", "café"):
                with self.assertRaises(ValueError):
                    log.register(bad)
            with self.assertRaises(ValueError):
                log.register(123)

    def test_ack_rules(self):
        with RetainedJournal(self.dir) as log:
            o1 = log.append(b"a")
            o2 = log.append(b"b")
            end = log.end
            log.register("c")
            self.assertEqual(log.checkpoint("c"), 4)
            # Unknown name -> KeyError on query / ack, ValueError on bad name.
            with self.assertRaises(KeyError):
                log.checkpoint("ghost")
            with self.assertRaises(KeyError):
                log.ack("ghost", o1)
            with self.assertRaises(ValueError):
                log.ack("c", 1.5)
            # Frame-internal and out of range are rejected, state untouched.
            with self.assertRaises(ValueError):
                log.ack("c", o1 + 1)
            with self.assertRaises(ValueError):
                log.ack("c", end + 12)
            with self.assertRaises(ValueError):
                log.ack("c", 3)
            self.assertEqual(log.checkpoint("c"), 4)
            log.ack("c", o2)
            # Same value retry has no effect; rollback is refused.
            log.ack("c", o2)
            with self.assertRaises(ValueError):
                log.ack("c", o1)
            self.assertEqual(log.checkpoint("c"), o2)
            # Acking end is allowed.
            log.ack("c", end)
            self.assertEqual(log.checkpoint("c"), end)

    def test_drop_and_reregister_and_persistence(self):
        with RetainedJournal(self.dir) as log:
            o1 = log.append(b"a")
            o2 = log.append(b"b")
            log.register("c")
            log.ack("c", o2)
            self.assertTrue(log.drop("c"))
            self.assertFalse(log.drop("c"))
            with self.assertRaises(KeyError):
                log.checkpoint("c")
            # No consumers: prune must not advance the floor.
            self.assertEqual(log.prune(), 4)
            # Re-register starts at the *current* floor.
            self.assertEqual(log.register("c"), 4)
        with RetainedJournal(self.dir) as log:
            self.assertEqual(log.checkpoint("c"), 4)
            self.assertTrue(log.drop("c"))
        with RetainedJournal(self.dir) as log:
            with self.assertRaises(KeyError):
                log.checkpoint("c")


class PruneSemanticsTest(unittest.TestCase):
    def setUp(self):
        self.root = TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.dir = Path(self.root.name) / "journal"

    def test_two_consumers_slow_controls_floor(self):
        with RetainedJournal(self.dir) as log:
            offsets = [log.append(bytes([i % 256]) * 1000) for i in range(12)]
            offsets.append(log.append(b""))  # empty payload frame
            offsets.append(log.append(b"last"))
            log.register("slow")
            log.register("fast")
            log.ack("fast", offsets[11])
            log.ack("slow", offsets[3])
            # Acking never moves the floor by itself.
            self.assertEqual(log.floor, 4)
            before = tree_size(self.dir)
            log.prune()
            self.assertEqual(log.floor, offsets[3])
            self.assertLess(tree_size(self.dir), before)
            # Fast consumer cursor still valid and reads identical records.
            fast_rows = log.read(offsets[8], limit=100)
            self.assertEqual(boundaries(fast_rows), offsets[8:])
            # Old cursor is explicitly expired.
            with self.assertRaises(ExpiredOffset):
                log.read(offsets[0])
            with self.assertRaises(ExpiredOffset):
                log.read(offsets[2])
            # Advance only the slow consumer; next prune releases more.
            log.ack("slow", offsets[9])
            log.prune()
            self.assertEqual(log.floor, offsets[9])
            with self.assertRaises(ExpiredOffset):
                log.read(offsets[8])
            self.assertEqual(
                [r.data for r in log.read(offsets[9], limit=100)],
                [r.data for r in read_all(log)],
            )
            # Acks are unchanged by pruning.
            self.assertEqual(log.checkpoint("slow"), offsets[9])
            self.assertEqual(log.checkpoint("fast"), offsets[11])

    def test_frame_internal_ack_rejected(self):
        with RetainedJournal(self.dir) as log:
            o1 = log.append(b"abcdef")
            log.register("c")
            with self.assertRaises(ValueError):
                log.ack("c", o1 + 5)
            self.assertEqual(log.checkpoint("c"), 4)
            self.assertEqual(log.prune(), 4)

    def test_prune_to_end_then_reopen_and_append(self):
        with RetainedJournal(self.dir) as log:
            o1 = log.append(b"a")
            o2 = log.append(b"b")
            old_end = log.end
            log.register("c")
            log.ack("c", old_end)
            self.assertEqual(log.prune(), old_end)
            self.assertEqual(log.floor, log.end)
            # Next frame is appended at the old end; offsets are not reused.
            nxt = log.append(b"c")
            self.assertEqual(nxt, old_end)
            self.assertGreater(nxt, o2)
            self.assertEqual([r.data for r in log.read()], [b"c"])
            with self.assertRaises(ExpiredOffset):
                log.read(o1)
        with RetainedJournal(self.dir) as log:
            self.assertEqual(log.floor, old_end)
            self.assertEqual([r.data for r in read_all(log)], [b"c"])
            again = log.append(b"d")
            self.assertEqual(again, old_end + _HEADER.size + 1)
            self.assertEqual([r.data for r in read_all(log)], [b"c", b"d"])
            self.assertEqual(log.checkpoint("c"), old_end)

    def test_hook_called_twice_for_non_empty_prune_only(self):
        calls = []
        with RetainedJournal(self.dir, prune_hook=calls.append) as log:
            log.append(b"a")
            log.register("c")
            log.prune()  # nothing to release
            self.assertEqual(calls, [])
            log.ack("c", log.end)
            log.prune()
            self.assertEqual(calls, ["before_publish", "after_publish"])


class ReopenRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.root = TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.dir = Path(self.root.name) / "journal"

    def test_torn_tail_is_truncated(self):
        with RetainedJournal(self.dir) as log:
            first = log.append(b"x")
            second = log.append(b"abcdefgh")
            log.register("c")
        seg = next(Path(self.dir).glob("seg-*.aj"))
        valid = seg.read_bytes()
        for tail_size in (1, 11, 12, 16):
            with self.subTest(tail_size=tail_size):
                seg.write_bytes(valid[:second + tail_size])
                with RetainedJournal(self.dir) as log:
                    self.assertEqual(log.recovered_bytes, tail_size)
                    self.assertEqual(log.floor, 4)
                    self.assertEqual([r.data for r in log.read()], [b"x"])
                    self.assertEqual(log.append(b"new"), second)
                    self.assertEqual(log.checkpoint("c"), 4)
                with RetainedJournal(self.dir) as log:
                    self.assertEqual(log.recovered_bytes, 0)
                    self.assertEqual([r.data for r in read_all(log)],
                                     [b"x", b"new"])
            seg.write_bytes(valid)  # restore for the next subtest

    def test_complete_bad_frame_is_corruption(self):
        with RetainedJournal(self.dir) as log:
            log.append(b"payload")
        seg = next(Path(self.dir).glob("seg-*.aj"))
        valid = seg.read_bytes()
        for flip in (4, len(valid) - 1):
            corrupt = bytearray(valid)
            corrupt[flip] ^= 1
            seg.write_bytes(corrupt)
            with self.assertRaises(Corruption):
                RetainedJournal(self.dir)
            # The file is left untouched; nothing is truncated.
            self.assertEqual(seg.read_bytes(), bytes(corrupt))

    def test_stale_tmp_files_are_cleaned_on_open(self):
        with RetainedJournal(self.dir) as log:
            log.append(b"a")
            log.register("c")
        (Path(self.dir) / "seg-99999999999.aj.tmp").write_bytes(b"junk")
        with RetainedJournal(self.dir) as log:
            self.assertEqual([r.data for r in read_all(log)], [b"a"])
        names = {p.name for p in Path(self.dir).iterdir()}
        self.assertNotIn("seg-99999999999.aj.tmp", names)

    def test_torn_tail_across_many_segments(self):
        with RetainedJournal(self.dir) as log:
            offsets = [log.append(bytes([i % 256]) * 70000) for i in range(20)]
            self.assertGreater(len(list(Path(self.dir).glob("seg-*.aj"))), 1)
            last_off = offsets[-1]
        tail_seg = sorted(Path(self.dir).glob("seg-*.aj"))[-1]
        valid = tail_seg.read_bytes()
        frame_len = _HEADER.size + 70000
        # Cut 7 bytes out of the final segment's last frame: the header is
        # still complete but the payload is short, so the whole frame goes.
        tail_seg.write_bytes(valid[:-7])
        with RetainedJournal(self.dir) as log:
            self.assertEqual(log.recovered_bytes, frame_len - 7)
            self.assertEqual(boundaries(read_all(log)), offsets[:-1])
            # The next frame reuses the torn frame's logical start.
            self.assertEqual(log.append(b"repaired"), last_off)

    def test_corruption_inside_non_last_segment_is_not_truncated(self):
        with RetainedJournal(self.dir) as log:
            offsets = [log.append(bytes([i % 256]) * 70000) for i in range(20)]
            self.assertGreater(len(list(Path(self.dir).glob("seg-*.aj"))), 1)
        middle = sorted(Path(self.dir).glob("seg-*.aj"))[1]
        raw = bytearray(middle.read_bytes())
        raw[len(MAGIC) + 3] ^= 0xFF  # inside a complete frame header
        middle.write_bytes(bytes(raw))
        with self.assertRaises(Corruption):
            RetainedJournal(self.dir)

    def test_expired_offset_survives_reopen(self):
        with RetainedJournal(self.dir) as log:
            o1 = log.append(b"a")
            o2 = log.append(b"b")
            log.register("c")
            log.ack("c", o2)
            log.prune()
        with RetainedJournal(self.dir) as log:
            self.assertEqual(log.floor, o2)
            with self.assertRaises(ExpiredOffset):
                log.read(o1)
            # Re-registering a dropped consumer starts at the current floor.
            self.assertEqual(log.drop("c"), True)
            self.assertEqual(log.register("c"), o2)


# --- crash-during-prune test support --------------------------------------

CHILD_SCRIPT = r"""
import os, sys
sys.path.insert(0, os.environ["PROJECT_DIR"])
from journal import RetainedJournal

directory, phase = sys.argv[1], sys.argv[2]

def hook(observed):
    if observed == phase:
        os.kill(os.getpid(), 9)

with RetainedJournal(directory, prune_hook=hook) as log:
    log.prune()
"""


class _BigLogMixin:
    PAYLOAD = 48 * 1024
    N_FRAMES = 500  # 500 * (48 KiB + 12) ~= 23.4 MiB

    def _build(self, directory, slow_index=2):
        with RetainedJournal(directory) as log:
            offsets = [log.append(bytes([i % 256]) * self.PAYLOAD)
                       for i in range(self.N_FRAMES)]
            log.register("slow")
            log.register("fast")
            log.ack("slow", offsets[slow_index])
            log.ack("fast", offsets[400])
            return offsets


class SpaceAndCrashTest(unittest.TestCase, _BigLogMixin):
    def setUp(self):
        self.root = TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.dir = Path(self.root.name) / "journal"
        self.script = Path(self.root.name) / "child.py"
        self.script.write_text(CHILD_SCRIPT)

    def _run_child(self, phase):
        env = dict(os.environ, PROJECT_DIR=PROJECT_DIR)
        proc = subprocess.run(
            [sys.executable, str(self.script), str(self.dir), phase],
            env=env, capture_output=True, text=True,
        )
        self.assertEqual(proc.returncode, -9, proc.stderr)

    def test_space_budgets_and_two_step_prune(self):
        offsets = self._build(self.dir)
        end = offsets[-1] + _HEADER.size + self.PAYLOAD
        readable = end - 4
        self.assertGreater(readable, 20 * 1024 * 1024)
        self.assertLessEqual(tree_size(self.dir), readable + 192 * 1024)
        self.assertLess(file_count(self.dir), 4096)

        # First prune: release only about 200 KiB (4 of the 5 frames in the
        # head segment), measuring extra bytes used while prune is in flight.
        observed = {}
        start_size = tree_size(self.dir)

        def hook(phase):
            if phase == "before_publish":
                observed["extra"] = tree_size(self.dir) - start_size

        with RetainedJournal(self.dir, prune_hook=hook) as log:
            target = offsets[4]
            log.ack("slow", target)
            log.prune()
            self.assertEqual(log.floor, target)
        self.assertLessEqual(observed["extra"], 3 * 1024 * 1024)
        # Resident space stays within readable bytes + 192 KiB.
        readable = end - target
        self.assertLessEqual(tree_size(self.dir), readable + 192 * 1024)
        # Roughly 200 KiB was actually released from the directory: the head
        # segment was rewritten without its first four frames.
        released = start_size - tree_size(self.dir)
        self.assertGreater(released, 150 * 1024)
        self.assertLess(released, 250 * 1024)

        # Second prune: drop a large prefix.
        start_size = tree_size(self.dir)
        observed.clear()

        def hook2(phase):
            if phase == "before_publish":
                observed["before"] = tree_size(self.dir) - start_size
            if phase == "after_publish":
                observed["after"] = tree_size(self.dir) - start_size

        with RetainedJournal(self.dir, prune_hook=hook2) as log:
            log.ack("slow", offsets[200])
            log.prune()
            self.assertEqual(log.floor, offsets[200])
        self.assertLessEqual(observed["before"], 3 * 1024 * 1024)
        self.assertLessEqual(observed["after"], 3 * 1024 * 1024)
        readable = end - offsets[200]
        self.assertLessEqual(tree_size(self.dir), readable + 192 * 1024)
        self.assertLess(file_count(self.dir), 4096)

        # Successful prune: reading and appending continue to work.
        with RetainedJournal(self.dir) as log:
            rows = log.read(offsets[200], limit=1000)
            self.assertEqual(boundaries(rows), offsets[200:])
            new_off = log.append(b"post-prune")
            self.assertEqual(new_off, end)
            self.assertEqual(log.read(end)[0].data, b"post-prune")
            self.assertEqual(log.checkpoint("slow"), offsets[200])
            self.assertEqual(log.checkpoint("fast"), offsets[400])

    def test_kill_before_publish_keeps_old_cut_point(self):
        offsets = self._build(self.dir, slow_index=200)
        end = offsets[-1] + _HEADER.size + self.PAYLOAD
        self._run_child("before_publish")
        # Reopen: old cut point, all old frames, acks intact, no temp files.
        with RetainedJournal(self.dir) as log:
            self.assertEqual(log.floor, 4)
            self.assertEqual(log.end, end)
            self.assertEqual(boundaries(read_all(log)), offsets)
            self.assertEqual(log.checkpoint("slow"), offsets[200])
            self.assertEqual(log.checkpoint("fast"), offsets[400])
            self.assertFalse(list(Path(self.dir).glob("*.tmp")))
            self.assertLessEqual(tree_size(self.dir),
                                 (end - 4) + 192 * 1024)
            # Life goes on: append and a successful prune.
            more = log.append(b"after-crash")
            self.assertEqual(more, end)
            log.prune()
            self.assertEqual(log.floor, offsets[200])
        self.assertFalse(list(Path(self.dir).glob("*.tmp")))

    def test_kill_after_publish_keeps_new_cut_point(self):
        offsets = self._build(self.dir, slow_index=200)
        end = offsets[-1] + _HEADER.size + self.PAYLOAD
        self._run_child("after_publish")
        # Reopen: new cut point, complete surviving suffix, acks intact,
        # and old segments / temp files already cleaned up.
        with RetainedJournal(self.dir) as log:
            self.assertEqual(log.floor, offsets[200])
            self.assertEqual(log.end, end)
            rows = read_all(log)
            self.assertEqual(boundaries(rows), offsets[200:])
            self.assertEqual(rows[0].data,
                             bytes([200 % 256]) * self.PAYLOAD)
            self.assertEqual(log.checkpoint("slow"), offsets[200])
            self.assertEqual(log.checkpoint("fast"), offsets[400])
            self.assertFalse(list(Path(self.dir).glob("*.tmp")))
            self.assertLessEqual(
                tree_size(self.dir),
                (end - offsets[200]) + 192 * 1024,
            )
            # Continue appending and pruning.
            more = log.append(b"after-crash")
            self.assertEqual(more, end)
            new_end = log.end
            log.ack("slow", new_end)
            log.ack("fast", new_end)
            log.prune()
            # Pruned fully to end: the journal is logically empty, no segment
            # files remain, and a subsequent append resumes at the old end.
            self.assertEqual(log.floor, log.end)
            self.assertEqual(read_all(log), [])
            self.assertFalse(list(Path(self.dir).glob("seg-*.aj")))
            again = log.append(b"again")
            self.assertEqual(again, new_end)
            self.assertEqual([r.data for r in read_all(log)], [b"again"])
        self.assertFalse(list(Path(self.dir).glob("*.tmp")))
        self.assertLessEqual(tree_size(self.dir), 192 * 1024)


class ManifestContentTest(unittest.TestCase):
    def setUp(self):
        self.root = TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.dir = Path(self.root.name) / "journal"

    def test_manifest_records_state(self):
        with RetainedJournal(self.dir) as log:
            o1 = log.append(b"a")
            o2 = log.append(b"b")
            o3 = log.append(b"c")
            log.register("c")
            log.ack("c", o2)
            log.prune()
        manifest = json.loads(
            (Path(self.dir) / "manifest.json").read_text())
        self.assertEqual(manifest["version"], 1)
        self.assertEqual(manifest["floor"], o2)
        self.assertEqual(manifest["consumers"], {"c": o2})
        # Segments are persisted as [file name, logical start] pairs, and the
        # surviving segment starts exactly at the new floor.
        self.assertTrue(
            all(
                isinstance(entry, list) and isinstance(entry[0], str)
                and isinstance(entry[1], int)
                for entry in manifest["segments"]
            )
        )
        self.assertEqual(manifest["segments"][0][1], o2)
        self.assertGreaterEqual(manifest["next_seq"], len(manifest["segments"]))
        # Pruning to end empties the segment list but keeps floor/end stable.
        with RetainedJournal(self.dir) as log:
            log.ack("c", log.end)
            log.prune()
        manifest = json.loads(
            (Path(self.dir) / "manifest.json").read_text())
        self.assertEqual(manifest["segments"], [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
