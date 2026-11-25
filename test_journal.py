from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from journal import Corruption, Journal, MAX_RECORD


class JournalTest(unittest.TestCase):
    def setUp(self):
        root = TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.file = Path(root.name) / "log.aj"

    def test_reopen_and_read_from_checkpoint(self):
        with Journal(self.file) as log:
            self.assertEqual(log.append(b"A"), 4)
            second = log.append(b"")
            log.append(b"C")
        with Journal(self.file) as log:
            self.assertEqual(log.recovered_bytes, 0)
            first = log.read(limit=1)[0]
            self.assertEqual(first.next_offset, second)
            self.assertEqual([r.data for r in log.read(second)], [b"", b"C"])
            with self.assertRaisesRegex(ValueError, "frame boundary"):
                log.read(second + 1)
            with self.assertRaises(ValueError):
                log.append(b"X" * (MAX_RECORD + 1))

    def test_partial_header_and_partial_payload_recover_exact_prefix(self):
        with Journal(self.file) as log:
            log.append(b"x")
            second = log.append(b"abcdefgh")
        valid = self.file.read_bytes()
        for tail_size in (1, 11, 12, 16):
            with self.subTest(tail_size=tail_size):
                self.file.write_bytes(valid[:second + tail_size])
                with Journal(self.file) as log:
                    self.assertEqual(log.recovered_bytes, tail_size)
                    self.assertEqual([r.data for r in log.read()], [b"x"])
                    self.assertEqual(log.append(b"new"), second)
                with Journal(self.file) as log:
                    self.assertEqual([r.data for r in log.read()], [b"x", b"new"])

    def test_corrupted_complete_frame_is_never_truncated(self):
        with Journal(self.file) as log:
            log.append(b"payload")
        valid = self.file.read_bytes()
        for flip in (4, len(valid) - 1):
            corrupt = bytearray(valid)
            corrupt[flip] ^= 1
            self.file.write_bytes(corrupt)
            with self.assertRaises(Corruption):
                Journal(self.file)
            self.assertEqual(self.file.read_bytes(), corrupt)
