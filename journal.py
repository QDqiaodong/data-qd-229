"""Single-writer append journal with explicit frame boundaries."""
from bisect import bisect_left
from dataclasses import dataclass
import os
from struct import Struct
from zlib import crc32

MAGIC = b"AJ1\n"
MAX_RECORD = 1024 * 1024
_FIELDS = Struct(">II")
_HEADER = Struct(">III")


class Corruption(ValueError):
    pass


@dataclass(frozen=True)
class Record:
    offset: int
    next_offset: int
    data: bytes


def _decode_header(header, offset):
    length, body_crc, header_crc = _HEADER.unpack(header)
    if crc32(header[:_FIELDS.size]) != header_crc or length > MAX_RECORD:
        raise Corruption(f"invalid frame header at byte {offset}")
    return length, body_crc


class Journal:
    def __init__(self, path):
        try:
            self._file = open(path, "r+b")
            new = False
        except FileNotFoundError:
            self._file = open(path, "x+b")
            new = True
        self._offsets = []
        self._end = len(MAGIC)
        self.recovered_bytes = 0
        self._bad_write = False
        try:
            if new:
                self._file.write(MAGIC)
                self._sync()
            self._scan()
        except BaseException:
            self.close()
            raise

    def _sync(self):
        self._file.flush()
        os.fsync(self._file.fileno())

    def _read_frame(self, offset):
        header = self._file.read(_HEADER.size)
        if not header:
            return None
        if len(header) < _HEADER.size:
            raise EOFError
        size, expected = _decode_header(header, offset)
        data = self._file.read(size)
        if len(data) < size:
            raise EOFError
        if crc32(data) != expected:
            raise Corruption(f"payload checksum mismatch at byte {offset}")
        return Record(offset, self._file.tell(), data)

    def _scan(self):
        self._file.seek(0)
        if self._file.read(len(MAGIC)) != MAGIC:
            raise Corruption("missing or unknown journal header")
        while True:
            try:
                row = self._read_frame(self._end)
            except EOFError:
                self._file.seek(0, os.SEEK_END)
                self.recovered_bytes = self._file.tell() - self._end
                self._file.truncate(self._end)
                self._sync()
                return
            if row is None:
                return
            self._offsets.append(row.offset)
            self._end = row.next_offset

    def _require_open(self):
        if self._file.closed or self._bad_write:
            raise RuntimeError("journal is closed or must be reopened after a write failure")

    def append(self, data):
        self._require_open()
        if not isinstance(data, bytes) or len(data) > MAX_RECORD:
            raise ValueError(f"record must be bytes, at most {MAX_RECORD} bytes")
        fields = _FIELDS.pack(len(data), crc32(data))
        header = fields + crc32(fields).to_bytes(4, "big")
        start = self._end
        try:
            self._file.seek(start)
            self._file.write(header + data)
            self._sync()
        except OSError:
            self._bad_write = True
            raise
        self._offsets.append(start)
        self._end += len(header) + len(data)
        return start

    def read(self, offset=len(MAGIC), *, limit=16):
        self._require_open()
        if type(offset) is not int or type(limit) is not int or limit < 1:
            raise ValueError("integer offset and positive integer limit required")
        index = bisect_left(self._offsets, offset)
        if offset == self._end:
            return []
        if index == len(self._offsets) or self._offsets[index] != offset:
            raise ValueError("offset is not a frame boundary")
        self._file.seek(offset)
        rows = []
        for expected_offset in self._offsets[index:index + limit]:
            try:
                row = self._read_frame(expected_offset)
            except EOFError as exc:
                raise Corruption(f"file changed at byte {expected_offset}") from exc
            if row is None:
                raise Corruption(f"file changed at byte {expected_offset}")
            rows.append(row)
        return rows

    def close(self):
        self._file.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
