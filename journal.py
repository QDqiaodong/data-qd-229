"""Single-writer append journal with explicit frame boundaries."""
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
import fcntl
import json
import os
import re
from struct import Struct
from zlib import crc32

MAGIC = b"AJ1\n"
MAX_RECORD = 1024 * 1024
_FIELDS = Struct(">II")
_HEADER = Struct(">III")
INT64_MAX = (1 << 63) - 1


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



class ExpiredOffset(ValueError):
    """Raised when a read offset is below the retained journal's floor."""


class ReplicaConflict(ValueError):
    """An incoming frame overlaps a stored frame with different content."""


class ReplicaBackpressure(ValueError):
    """Accepting a batch would exceed the replica's pending-byte budget."""


# --- ReplicaJournal out-of-order receiver ----------------------------------
# Directory layout (implementation details):
#   replica.json                 source identity and pending byte budget
#   frame-<offset:012d>.aj       one complete AJ1-encoded frame per file
#
# Sparse logical offsets are represented by file names, never by seeking a
# shared file to a large offset and leaving a solid hole.  Only small metadata
# stays in memory; payloads are read back from their individual frame files.

_REPLICA_CONFIG_NAME = "replica.json"
_REPLICA_FRAME_RE = re.compile(r"frame-(\d+)\.aj\Z")
_REPLICA_SOURCE_RE = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")
_REPLICA_START = len(MAGIC)
_REPLICA_MAX_SOURCE_BYTES = 64 * 1024 * 1024
_REPLICA_MAX_FRAMES = 65536
_REPLICA_MAX_PENDING_BYTES = 64 * 1024 * 1024
_REPLICA_MAX_BATCH_RECORDS = 256
_REPLICA_MAX_BATCH_BYTES = 4 * 1024 * 1024
_REPLICA_MAX_READ_LIMIT = 8
_REPLICA_CONFIG_VERSION = 1


class ReplicaJournal:
    """Local receiver for out-of-order, duplicated, and batched source frames.

    Accepted frames are stored individually under ``directory``.  ``end`` is
    always the end of the longest contiguous prefix beginning at offset 4;
    frames after a hole are durable and count against ``max_pending_bytes`` but
    are not readable until the hole is filled.
    """

    def __init__(self, directory, *, source_id, max_pending_bytes):
        if not isinstance(source_id, str) or not _REPLICA_SOURCE_RE.fullmatch(
            source_id
        ):
            raise ValueError(
                "source_id must be 1-64 ASCII letters, digits, '_' or '-'"
            )
        if (
            type(max_pending_bytes) is not int
            or not 0 <= max_pending_bytes <= _REPLICA_MAX_PENDING_BYTES
        ):
            raise ValueError(
                "max_pending_bytes must be an integer from 0 through 64 MiB"
            )

        self._directory = os.fspath(directory)
        self._source_id = source_id
        self._max_pending_bytes = max_pending_bytes
        self._config_path = os.path.join(self._directory, _REPLICA_CONFIG_NAME)
        self._closed = False
        self._next_by_offset = {}
        self._offsets = []

        os.makedirs(self._directory, exist_ok=True)
        self._open_config()
        self._load_frames()
        self._end = self._contiguous_end(self._next_by_offset, _REPLICA_START)
        self._pending_bytes = self._count_pending(
            self._next_by_offset, self._end
        )

    # -- setup / persistence ------------------------------------------------

    def _fsync_directory(self):
        dir_fd = os.open(self._directory, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    def _write_config(self):
        payload = json.dumps(
            {
                "version": _REPLICA_CONFIG_VERSION,
                "source_id": self._source_id,
                "max_pending_bytes": self._max_pending_bytes,
            },
            sort_keys=True,
        ).encode("utf-8")
        tmp_path = self._config_path + ".tmp"
        with open(tmp_path, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, self._config_path)
        self._fsync_directory()

    def _open_config(self):
        try:
            with open(self._config_path, "rb") as handle:
                raw = handle.read()
        except FileNotFoundError:
            self._write_config()
            return

        try:
            config = json.loads(raw.decode("utf-8"))
            stored_source = config["source_id"]
            stored_budget = config["max_pending_bytes"]
            version = config["version"]
        except (ValueError, UnicodeDecodeError, KeyError, TypeError) as exc:
            raise Corruption("invalid replica.json") from exc
        if version != _REPLICA_CONFIG_VERSION:
            raise Corruption("unsupported replica.json version")
        if (
            not isinstance(stored_source, str)
            or type(stored_budget) is not int
        ):
            raise Corruption("malformed replica.json")

        # A configuration mismatch must never rewrite or reset the directory.
        if stored_source != self._source_id:
            raise ValueError(
                f"directory belongs to source_id {stored_source!r}, "
                f"not {self._source_id!r}"
            )
        if stored_budget != self._max_pending_bytes:
            raise ValueError(
                "max_pending_bytes does not match the value used when the "
                "replica directory was created"
            )

    def _frame_path(self, offset):
        return os.path.join(self._directory, f"frame-{offset:012d}.aj")

    def _load_frames(self):
        for name in os.listdir(self._directory):
            match = _REPLICA_FRAME_RE.fullmatch(name)
            if match is None:
                continue
            offset = int(match.group(1))
            path = os.path.join(self._directory, name)
            if os.path.islink(path) or not os.path.isfile(path):
                raise Corruption(f"unexpected replica frame entry: {name}")
            encoded_length = os.stat(path).st_size
            if (
                offset < _REPLICA_START
                or encoded_length < _HEADER.size
                or offset + encoded_length
                > _REPLICA_START + _REPLICA_MAX_SOURCE_BYTES
            ):
                raise Corruption(f"replica frame {name} is out of range")
            if offset in self._next_by_offset:
                raise Corruption(f"duplicate replica frame entry for {offset}")

            # Validate only the fixed header during open.  Payload checksums are
            # checked when a frame is read; this keeps reopening linear and
            # avoids retaining payload memory.
            with open(path, "rb") as handle:
                header = handle.read(_HEADER.size)
            if len(header) != _HEADER.size:
                raise Corruption(f"short replica frame header in {name}")
            length, _body_crc = _decode_header(header, offset)
            if _HEADER.size + length != encoded_length:
                raise Corruption(f"replica frame length mismatch in {name}")

            self._next_by_offset[offset] = offset + encoded_length

        self._offsets = sorted(self._next_by_offset)

    @staticmethod
    def _contiguous_end(next_by_offset, start):
        end = start
        while True:
            next_offset = next_by_offset.get(end)
            if next_offset is None:
                return end
            end = next_offset

    @staticmethod
    def _count_pending(next_by_offset, end):
        total = 0
        for offset, next_offset in next_by_offset.items():
            # Accepted intervals never overlap.  Every frame below the maximal
            # prefix end is part of that prefix; all remaining frames wait on
            # one or more holes.
            if offset >= end:
                total += next_offset - offset
        return total

    # -- state --------------------------------------------------------------

    @property
    def end(self):
        return self._end

    @property
    def pending_bytes(self):
        return self._pending_bytes

    @property
    def source_id(self):
        return self._source_id

    @property
    def max_pending_bytes(self):
        return self._max_pending_bytes

    def _require_open(self):
        if self._closed:
            raise RuntimeError("ReplicaJournal is closed")

    def close(self):
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()

    # -- ingestion ----------------------------------------------------------

    @staticmethod
    def _validate_batch(records):
        if not isinstance(records, (list, tuple)):
            raise ValueError("records must be a non-empty list or tuple")
        if not records or len(records) > _REPLICA_MAX_BATCH_RECORDS:
            raise ValueError(
                "a batch must contain 1 through 256 Record objects"
            )

        batch_bytes = 0
        expected_offset = None
        for index, record in enumerate(records):
            if not isinstance(record, Record):
                raise ValueError("every batch item must be a Record")
            if type(record.offset) is not int:
                raise ValueError("Record.offset must be a true integer")
            if type(record.next_offset) is not int:
                raise ValueError("Record.next_offset must be a true integer")
            if not isinstance(record.data, bytes):
                raise ValueError("Record.data must be bytes")
            if len(record.data) > MAX_RECORD:
                raise ValueError(
                    f"record payload exceeds {MAX_RECORD} bytes"
                )

            encoded_length = _HEADER.size + len(record.data)
            if record.next_offset != record.offset + encoded_length:
                raise ValueError(
                    "Record.next_offset must equal offset + 12 + len(data)"
                )
            if (
                record.offset < _REPLICA_START
                or record.next_offset
                > _REPLICA_START + _REPLICA_MAX_SOURCE_BYTES
            ):
                raise ValueError(
                    "record lies outside the source's first 64 MiB of "
                    "encoded frames"
                )
            if index and record.offset != expected_offset:
                raise ValueError("frames in a batch must be contiguous")

            expected_offset = record.next_offset
            batch_bytes += encoded_length

        if batch_bytes > _REPLICA_MAX_BATCH_BYTES:
            raise ValueError("encoded batch total exceeds 4 MiB")

    def _payload_matches(self, offset, data):
        with open(self._frame_path(offset), "rb") as handle:
            header = handle.read(_HEADER.size)
            if len(header) != _HEADER.size:
                raise Corruption(f"short replica frame header at {offset}")
            # The endpoints already establish the payload length; compare the
            # bytes directly so a CRC collision cannot turn a conflict into a
            # duplicate.
            return handle.read() == data

    def _check_overlap(self, record):
        """Return True for an exact duplicate, otherwise raise conflict."""
        offset = record.offset
        end = record.next_offset
        index = bisect_right(self._offsets, offset) - 1

        if index >= 0:
            old_offset = self._offsets[index]
            old_end = self._next_by_offset[old_offset]
            if offset < old_end:  # old_offset <= offset < old_end
                if (
                    old_offset == offset
                    and old_end == end
                    and self._payload_matches(old_offset, record.data)
                ):
                    return True
                raise ReplicaConflict(
                    f"incoming frame at {offset} conflicts with stored frame "
                    f"at {old_offset}"
                )

        # index + 1 is the first stored start strictly after the rightmost
        # start <= offset.  Every such start below end overlaps this record.
        j = index + 1
        while j < len(self._offsets) and self._offsets[j] < end:
            old_offset = self._offsets[j]
            raise ReplicaConflict(
                f"incoming frame at {offset} conflicts with stored frame at "
                f"{old_offset}"
            )
        return False

    def ingest(self, records):
        self._require_open()
        self._validate_batch(records)

        # Conflict detection has priority over the pending-byte budget and must
        # be all-or-nothing, so do it before creating any frame file.
        new_records = []
        for record in records:
            if not self._check_overlap(record):
                new_records.append(record)

        if (
            len(self._next_by_offset) + len(new_records)
            > _REPLICA_MAX_FRAMES
        ):
            raise ValueError("replica cannot hold more than 65,536 frames")

        # Compute the prospective prefix without copying the whole index.
        # New frames below the current prefix can only be exact duplicates and
        # were filtered out above, so chain extension starts at self._end.
        new_next_by_offset = {
            record.offset: record.next_offset for record in new_records
        }
        new_bytes = sum(
            next_offset - offset
            for offset, next_offset in new_next_by_offset.items()
        )
        new_end = self._end
        connected_bytes = 0
        while True:
            if new_end in new_next_by_offset:
                next_offset = new_next_by_offset[new_end]
            else:
                next_offset = self._next_by_offset.get(new_end)
                if next_offset is None:
                    break
            connected_bytes += next_offset - new_end
            new_end = next_offset

        # Old pending bytes + all genuinely new bytes - every frame in the
        # newly connected chain (some old pending, some newly supplied).
        new_pending_bytes = (
            self._pending_bytes + new_bytes - connected_bytes
        )

        if new_pending_bytes > self._max_pending_bytes:
            raise ReplicaBackpressure(
                f"pending frames need {new_pending_bytes} bytes; budget is "
                f"{self._max_pending_bytes}"
            )

        # All rejection checks are complete.  Persist each new frame as its
        # own allocated file.  The file context flushes its buffers before
        # ingest returns; crash durability is outside this receiver's scope.
        for record in new_records:
            path = self._frame_path(record.offset)
            fields = _FIELDS.pack(
                len(record.data), crc32(record.data)
            )
            header = fields + crc32(fields).to_bytes(4, "big")
            with open(path, "xb") as handle:
                handle.write(header)
                handle.write(record.data)

        if new_records:
            for record in new_records:
                self._next_by_offset[record.offset] = record.next_offset
                self._offsets.append(record.offset)
            self._offsets.sort()

        self._end = new_end
        self._pending_bytes = new_pending_bytes
        return self._end

    # -- replay -------------------------------------------------------------

    def read(self, offset=_REPLICA_START, *, limit=4):
        self._require_open()
        if type(offset) is not int:
            raise ValueError("offset must be an integer")
        if type(limit) is not int or not 1 <= limit <= _REPLICA_MAX_READ_LIMIT:
            raise ValueError("limit must be an integer from 1 through 8")
        if offset < _REPLICA_START or offset > self._end:
            raise ValueError(
                "offset must be at least 4 and no greater than the contiguous "
                "replica end"
            )
        if offset == self._end:
            return []

        index = bisect_left(self._offsets, offset)
        if index >= len(self._offsets) or self._offsets[index] != offset:
            raise ValueError("offset is not a received frame boundary")

        rows = []
        current = offset
        while len(rows) < limit and current < self._end:
            if (
                index >= len(self._offsets)
                or self._offsets[index] != current
            ):
                raise Corruption("replica prefix does not follow frame boundaries")
            next_offset = self._next_by_offset[current]
            path = self._frame_path(current)
            with open(path, "rb") as handle:
                header = handle.read(_HEADER.size)
                if len(header) != _HEADER.size:
                    raise Corruption(f"short replica frame header at {current}")
                length, expected_crc = _decode_header(header, current)
                if current + _HEADER.size + length != next_offset:
                    raise Corruption(
                        f"replica frame metadata mismatch at {current}"
                    )
                data = handle.read(length)
                if len(data) < length:
                    raise Corruption(f"short replica frame payload at {current}")
                if crc32(data) != expected_crc:
                    raise Corruption(
                        f"payload checksum mismatch at byte {current}"
                    )

            rows.append(Record(current, next_offset, data))
            current = next_offset
            index += 1

        return rows


# --- RetainedJournal on-disk layout ---------------------------------------
# directory/
#   lock                       single active handle (flock; zero bytes)
#   manifest.json              committed cut point, segments, acks
#   seg-NNNNNNNNNNN.aj         data segment: MAGIC + AJ1 frames
#   *.tmp                      staged files, cleaned on open
#
# Each segment's logical start offset is persisted in the manifest, so head
# rewrites never change surviving frame offsets and reopening never has to
# infer a start from current file sizes. The manifest rename is the only
# commit point, so a crash leaves either the complete old cut point or the
# complete new cut point on disk.

_MANIFEST_NAME = "manifest.json"
_LOCK_NAME = "lock"
_TMP_SUFFIX = ".tmp"
_SEG_RE = re.compile(r"seg-(\d+)\.aj\Z")
_NAME_RE = re.compile(r"[A-Za-z0-9_-]{1,32}\Z")
_MAX_CONSUMERS = 16
_MAX_FRAMES = 65536
_MAX_READABLE_BYTES = 64 * 1024 * 1024
# Head rewrite stages at most one segment worth of surviving suffix; keeping
# segments small bounds the extra space used while a prune is in progress.
_SEGMENT_CAP = 256 * 1024
_MANIFEST_VERSION = 1


class RetainedJournal:
    """Directory-backed journal with consumer-cursor-driven physical pruning.

    Frames use the same AJ1 encoding as :class:`Journal`. Logical byte offsets
    are stable for the whole lifetime of the directory: the first frame sits at
    offset 4 and pruning only advances ``floor``; it never reuses offsets.
    """

    def __init__(self, directory, *, prune_hook=None):
        self._dir = os.fspath(directory)
        if prune_hook is not None and not callable(prune_hook):
            raise ValueError("prune_hook must be callable")
        self._prune_hook = prune_hook
        self._closed = False
        os.makedirs(self._dir, exist_ok=True)
        self._lock_file = open(os.path.join(self._dir, _LOCK_NAME), "a+b")
        try:
            try:
                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError(
                    "another active RetainedJournal handle already exists"
                ) from exc
            self._open()
        except BaseException:
            self._lock_file.close()
            raise

    # -- opening / recovery ------------------------------------------------

    def _open(self):
        manifest_path = os.path.join(self._dir, _MANIFEST_NAME)
        try:
            with open(manifest_path, "rb") as handle:
                raw = handle.read()
        except FileNotFoundError:
            manifest = None
        else:
            try:
                manifest = json.loads(raw.decode("utf-8"))
            except (ValueError, UnicodeDecodeError) as exc:
                raise Corruption("manifest is not valid JSON") from exc
            if not isinstance(manifest, dict) or manifest.get("version") != _MANIFEST_VERSION:
                raise Corruption("unsupported manifest version")

        self._cleanup_stale(manifest)

        if manifest is None:
            self._floor = len(MAGIC)
            self._consumers = {}
            # Each entry is (file name, logical offset of its first frame).
            self._segments = []
            self._next_seq = 0
        else:
            try:
                self._floor = int(manifest["floor"])
                self._consumers = {
                    str(name): int(pos)
                    for name, pos in dict(manifest["consumers"]).items()
                }
                self._segments = [
                    (str(entry[0]), int(entry[1]))
                    for entry in list(manifest["segments"])
                ]
                self._next_seq = int(manifest["next_seq"])
            except (KeyError, TypeError, ValueError, IndexError) as exc:
                raise Corruption("malformed manifest") from exc
            if not all(
                _SEG_RE.fullmatch(name) and isinstance(start, int)
                and start >= self._floor
                for name, start in self._segments
            ):
                raise Corruption("malformed segment list in manifest")
            if not all(_NAME_RE.fullmatch(n) for n in self._consumers):
                raise Corruption("malformed consumer names in manifest")

        self.recovered_bytes = 0
        self._offsets = []
        self._end = self._floor
        self._scan()
        self._seg_file = None
        if self._segments:
            self._seg_file = open(
                os.path.join(self._dir, self._segments[-1][0]), "r+b"
            )

    def _cleanup_stale(self, manifest):
        """Remove staging leftovers from an interrupted prune; keep everything
        the committed manifest references so either cut point stays intact."""
        live = {_LOCK_NAME}
        if manifest is not None:
            live.add(_MANIFEST_NAME)
            try:
                live.update(str(entry[0]) for entry in manifest["segments"])
            except (KeyError, TypeError, IndexError):
                raise Corruption("malformed manifest")
        for name in sorted(os.listdir(self._dir)):
            if name in live:
                continue
            path = os.path.join(self._dir, name)
            if os.path.islink(path) or not os.path.isfile(path):
                raise Corruption(f"unexpected directory entry: {name}")
            os.unlink(path)
        self._fsync_dir()

    def _scan(self):
        last = len(self._segments) - 1
        logical = self._floor
        for index, (seg_name, seg_start) in enumerate(self._segments):
            if seg_start != logical:
                raise Corruption(
                    f"segment {seg_name} starts at {seg_start}, expected {logical}"
                )
            path = os.path.join(self._dir, seg_name)
            try:
                handle = open(path, "r+b")
            except FileNotFoundError as exc:
                raise Corruption(f"missing segment {seg_name}") from exc
            with handle:
                if handle.read(len(MAGIC)) != MAGIC:
                    raise Corruption(
                        f"missing or unknown segment header in {seg_name}"
                    )
                physical = len(MAGIC)
                while True:
                    header = handle.read(_HEADER.size)
                    if not header:
                        break
                    if len(header) < _HEADER.size:
                        if index != last:
                            raise Corruption(
                                f"partial frame header inside {seg_name}"
                            )
                        self._truncate_tail(handle, physical, logical)
                        break
                    # A fully present header that fails validation is real
                    # corruption, never a torn append (mirrors Journal).
                    size, body_crc = _decode_header(header, logical)
                    data = handle.read(size)
                    if len(data) < size:
                        if index != last:
                            raise Corruption(f"partial payload inside {seg_name}")
                        self._truncate_tail(handle, physical, logical)
                        break
                    # A complete frame with a bad checksum is corruption and
                    # must never be silently treated as a torn tail.
                    if crc32(data) != body_crc:
                        raise Corruption(
                            f"payload checksum mismatch at byte {logical}"
                        )
                    self._offsets.append(logical)
                    frame_len = _HEADER.size + size
                    physical += frame_len
                    logical += frame_len
                handle.flush()
            self._end = logical

    def _truncate_tail(self, handle, physical, logical):
        handle.seek(0, os.SEEK_END)
        tail = handle.tell() - physical
        if tail < 0:
            raise Corruption("segment shrank below scanned prefix")
        handle.seek(physical)
        handle.truncate()
        handle.flush()
        os.fsync(handle.fileno())
        self.recovered_bytes += tail
        self._end = logical
        self._fsync_dir()

    def _fsync_dir(self):
        dir_fd = os.open(self._dir, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    def _manifest_dict(self):
        return {
            "version": _MANIFEST_VERSION,
            "floor": self._floor,
            "end": self._end,
            "consumers": dict(self._consumers),
            "segments": list(self._segments),
            "next_seq": self._next_seq,
        }

    def _write_manifest(self, target_path, payload):
        tmp_path = target_path + _TMP_SUFFIX
        with open(tmp_path, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.rename(tmp_path, target_path)
        self._fsync_dir()

    def _commit_manifest(self):
        self._write_manifest(
            os.path.join(self._dir, _MANIFEST_NAME),
            json.dumps(self._manifest_dict(), sort_keys=True).encode("utf-8"),
        )

    def _unlink(self, name):
        try:
            os.unlink(os.path.join(self._dir, name))
        except FileNotFoundError:
            pass

    # -- state -------------------------------------------------------------

    @property
    def floor(self):
        self._require_open()
        return self._floor

    @property
    def end(self):
        self._require_open()
        return self._end

    def _require_open(self):
        if self._closed:
            raise RuntimeError("journal is closed")

    def _segment_starts(self):
        return [start for _name, start in self._segments]

    def _segment_name(self, index):
        return self._segments[index][0]

    def _is_boundary(self, offset):
        if offset == self._end:
            return True
        index = bisect_left(self._offsets, offset)
        return index < len(self._offsets) and self._offsets[index] == offset

    # -- append ------------------------------------------------------------

    def append(self, data):
        self._require_open()
        if not isinstance(data, bytes) or len(data) > MAX_RECORD:
            raise ValueError(f"record must be bytes, at most {MAX_RECORD} bytes")
        frame_len = _HEADER.size + len(data)
        if self._end + frame_len > INT64_MAX:
            raise ValueError("logical end would exceed signed 64-bit range")
        if len(self._offsets) >= _MAX_FRAMES:
            raise ValueError("readable frame limit (65536) reached; prune first")
        if self._end + frame_len - self._floor > _MAX_READABLE_BYTES:
            raise ValueError("readable byte limit (64 MiB) reached; prune first")
        if self._seg_file is None:
            self._rollover()
        else:
            # Use the on-disk size, not the handle position: a freshly opened
            # segment handle starts at offset 0 even when the segment is full.
            size = os.fstat(self._seg_file.fileno()).st_size
            tail_start = self._segments[-1][1]
            no_room = size + frame_len > _SEGMENT_CAP
            # A segment whose first frame lies below floor holds dead prefix
            # bytes. Appending into it would place a live frame after dead
            # bytes, which a later head rewrite cannot preserve, so start a
            # clean segment instead.
            has_dead_prefix = tail_start < self._floor
            if no_room or has_dead_prefix:
                self._rollover()
        self._seg_file.seek(0, os.SEEK_END)
        fields = _FIELDS.pack(len(data), crc32(data))
        header = fields + crc32(fields).to_bytes(4, "big")
        start = self._end
        try:
            self._seg_file.write(header + data)
            self._seg_file.flush()
            os.fsync(self._seg_file.fileno())
        except OSError:
            self._closed = True
            raise
        self._offsets.append(start)
        self._end += frame_len
        return start

    def _rollover(self):
        if self._seg_file is not None:
            self._seg_file.close()
        name = f"seg-{self._next_seq:011d}.aj"
        self._next_seq += 1
        path = os.path.join(self._dir, name)
        with open(path, "xb") as handle:
            handle.write(MAGIC)
            handle.flush()
            os.fsync(handle.fileno())
        self._fsync_dir()
        # The new segment's first frame will begin at the current logical end.
        self._segments.append((name, self._end))
        self._seg_file = open(path, "r+b")
        self._commit_manifest()

    # -- read --------------------------------------------------------------

    def read(self, offset=None, *, limit=16):
        self._require_open()
        if offset is None:
            offset = self._floor
        if type(offset) is not int or type(limit) is not int or limit < 1:
            raise ValueError("integer offset and positive integer limit required")
        if offset < self._floor:
            raise ExpiredOffset(
                f"offset {offset} is below retained floor {self._floor}"
            )
        if offset > self._end:
            raise ValueError("offset is beyond end of journal")
        if offset == self._end:
            return []
        index = bisect_left(self._offsets, offset)
        if index == len(self._offsets) or self._offsets[index] != offset:
            raise ValueError("offset is not a frame boundary")

        rows = []
        starts = self._segment_starts()
        seg_index = bisect_right(starts, offset) - 1
        physical = offset - starts[seg_index] + len(MAGIC)
        handle = open(
            os.path.join(self._dir, self._segment_name(seg_index)), "rb"
        )
        try:
            handle.seek(physical)
            while len(rows) < limit and index < len(self._offsets):
                expected = self._offsets[index]
                header = handle.read(_HEADER.size)
                if len(header) < _HEADER.size:
                    # Roll over to the next segment; frames never straddle one.
                    handle.close()
                    seg_index += 1
                    if seg_index >= len(self._segments):
                        raise Corruption(f"segment changed at byte {expected}")
                    handle = open(
                        os.path.join(
                            self._dir, self._segment_name(seg_index)
                        ),
                        "rb",
                    )
                    handle.seek(len(MAGIC))
                    header = handle.read(_HEADER.size)
                    if len(header) < _HEADER.size:
                        raise Corruption(f"segment changed at byte {expected}")
                size, body_crc = _decode_header(header, expected)
                data = handle.read(size)
                if len(data) < size or crc32(data) != body_crc:
                    raise Corruption(f"payload checksum mismatch at byte {expected}")
                next_offset = expected + _HEADER.size + size
                rows.append(Record(expected, next_offset, data))
                index += 1
                if (
                    index < len(self._offsets)
                    and self._offsets[index] != next_offset
                ):
                    raise Corruption("frame offsets are not contiguous")
        finally:
            handle.close()
        return rows

    # -- consumers ---------------------------------------------------------

    def register(self, name):
        self._require_open()
        self._validate_name(name)
        if name in self._consumers:
            return self._consumers[name]
        if len(self._consumers) >= _MAX_CONSUMERS:
            raise ValueError("consumer limit (16) reached")
        self._consumers[name] = self._floor
        self._commit_manifest()
        return self._floor

    def checkpoint(self, name):
        self._require_open()
        self._validate_name(name)
        try:
            return self._consumers[name]
        except KeyError:
            raise KeyError(name)

    def ack(self, name, next_offset):
        self._require_open()
        self._validate_name(name)
        if type(next_offset) is not int:
            raise ValueError("next_offset must be an integer frame boundary or end")
        if name not in self._consumers:
            raise KeyError(name)
        if next_offset < self._floor or next_offset > self._end:
            raise ValueError("next_offset outside retained range")
        if next_offset != self._end and next_offset not in self._offsets:
            raise ValueError("next_offset is not a frame boundary")
        current = self._consumers[name]
        if next_offset < current:
            raise ValueError("acknowledgements can only move forward")
        if next_offset == current:
            return
        self._consumers[name] = next_offset
        self._commit_manifest()

    def drop(self, name):
        self._require_open()
        self._validate_name(name)
        if name not in self._consumers:
            return False
        del self._consumers[name]
        self._commit_manifest()
        return True

    @staticmethod
    def _validate_name(name):
        if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
            raise ValueError(
                "consumer name must be 1-32 ASCII letters, digits, _ or -"
            )

    # -- pruning -----------------------------------------------------------

    def prune(self):
        self._require_open()
        if self._consumers:
            new_floor = min(self._consumers.values())
        else:
            new_floor = self._floor
        return self._prune_to(new_floor)

    def _prune_to(self, new_floor):
        """Physically release the prefix below ``new_floor``.

        ``new_floor`` must be a frame boundary at or below ``end`` and must not
        precede the current floor.  Exposed internally so cursor-tracking
        journals can prune to a computed cut point without inventing a consumer.
        """
        self._require_open()
        if new_floor <= self._floor:
            return self._floor
        if new_floor > self._end or (
            new_floor != self._end and new_floor not in self._offsets
        ):
            raise Corruption("computed cut point is not a frame boundary")

        starts = self._segment_starts()
        # Segment containing the cut point. bisect_right - 1 also handles the
        # end-of-log case where the cut equals the end of the last segment.
        cut_seg = bisect_right(starts, new_floor) - 1
        old_segments = list(self._segments)
        cut_seg_start = starts[cut_seg]
        # Bytes inside the cut segment to drop. Inside one segment the logical
        # offset delta and the physical position delta coincide, so this is
        # both the logical cut and the physical skip length.
        physical_skip = new_floor - cut_seg_start
        # Rewrite only when there are surviving frames to preserve in the cut
        # segment. When the cut removes the segment's last frame, the whole
        # segment becomes unreachable and is simply discarded (never rewritten
        # into an empty segment that would swallow the next appended frame).
        rewrite = physical_skip > 0 and new_floor < self._end

        staged = None
        published = False
        old_seg_file = self._seg_file
        obsolete_names = []
        try:
            if rewrite:
                seq = self._next_seq
                self._next_seq += 1
                staged = f"seg-{seq:011d}.aj"
                # Stage the surviving suffix of the cut segment first, while
                # every committed file stays untouched.
                self._stage_head(
                    os.path.join(self._dir, old_segments[cut_seg][0]),
                    physical_skip,
                    staged,
                )
                # The replacement segment keeps the cut point as its logical
                # start; following segments keep their recorded starts.
                new_segments = [(staged, new_floor)] + old_segments[cut_seg + 1:]
                # Whole earlier segments and the rewritten cut segment's old
                # incarnation become unreachable once the manifest commits.
                obsolete_names = [name for name, _s in old_segments[:cut_seg]]
                obsolete_names.append(old_segments[cut_seg][0])
            else:
                if new_floor >= self._end:
                    # Nothing readable remains: the cut segment and every
                    # earlier one are discarded; a later append starts fresh.
                    new_segments = []
                    obsolete_names = [name for name, _s in old_segments]
                else:
                    # Cut lands exactly on a segment boundary: keep the cut
                    # segment and everything after it.
                    new_segments = list(old_segments[cut_seg:])
                    obsolete_names = [name for name, _s in old_segments[:cut_seg]]

            # Ready to publish, but the old cut point is still the committed
            # state. A kill here must reopen onto the old cut point.
            if self._prune_hook is not None:
                self._prune_hook("before_publish")

            if rewrite:
                staged_path = os.path.join(self._dir, staged)
                os.rename(staged_path + _TMP_SUFFIX, staged_path)
                self._fsync_dir()
            old_seg_file.close()
            self._floor = new_floor
            self._segments = new_segments
            # Atomic switch: after this rename the new cut point is the only
            # visible state. A kill here must reopen onto the new cut point.
            self._commit_manifest()
            published = True
            cut = bisect_left(self._offsets, new_floor)
            del self._offsets[:cut]
            if self._segments:
                self._seg_file = open(
                    os.path.join(
                        self._dir,
                        self._segment_name(len(self._segments) - 1),
                    ),
                    "r+b",
                )
            else:
                # Everything was pruned to end; the next append rolls over.
                self._seg_file = None

            # New cut point committed; old resources are still on disk.
            if self._prune_hook is not None:
                self._prune_hook("after_publish")

            for name in obsolete_names:
                self._unlink(name)
            self._fsync_dir()
        except BaseException:
            # A failure while publishing leaves on-disk state ambiguous; the
            # next open recovers either complete cut point. This handle is done.
            self._closed = True
            if not published and staged is not None:
                self._unlink(staged + _TMP_SUFFIX)
                self._unlink(staged)
            raise
        return self._floor

    def _stage_head(self, old_head_path, skip_bytes, staged):
        """Copy the cut segment's suffix (frames at/after the new floor) into
        ``staged.tmp``; skip_bytes is the segment-relative byte count dropped."""
        tmp_path = os.path.join(self._dir, staged + _TMP_SUFFIX)
        with open(old_head_path, "rb") as src, open(tmp_path, "wb") as dst:
            if src.read(len(MAGIC)) != MAGIC:
                raise Corruption("missing or unknown segment header")
            dst.write(MAGIC)
            skipped = 0
            while skipped < skip_bytes:
                chunk = src.read(min(1024 * 1024, skip_bytes - skipped))
                if not chunk:
                    raise Corruption("segment ended before staged cut point")
                skipped += len(chunk)
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)
            dst.flush()
            os.fsync(dst.fileno())

    # -- lifecycle ---------------------------------------------------------

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            if self._seg_file is not None:
                self._seg_file.close()
        finally:
            try:
                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
            finally:
                self._lock_file.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


# --- LeasedWorkJournal: per-device ordered task dispatch ------------------
# Directory layout (implementation details):
#   directory/
#     events/           authoritative RetainedJournal, one AJ1 frame per task
#     leases.json       receipt counter, current leases, finished offsets
#     leases.json.tmp   atomic staging file for leases.json
#
# The events journal is the sole authority for task payloads and offsets.
# leases.json only remembers dispatch state; on open it is reconciled against
# a full streaming scan of events, so a process killed between the durable
# append and the next state write loses nothing.
#
# Task frame payload encoding (inside the AJ1 record body):
#   1 byte  key length k (1..32)
#   k bytes ASCII key
#   n bytes task data (n may be 0; k + 1 + n <= 1,048,576)

_LEASED_EVENTS_DIR = "events"
_LEASED_STATE_NAME = "leases.json"
_LEASED_STATE_TMP = "leases.json.tmp"
_LEASED_CONSUMER = "leased-work"
_LEASED_KEY_RE = re.compile(r"[A-Za-z0-9_-]{1,32}\Z")
_LEASED_MAX_DATA = 1_000_000
_LEASED_MAX_TTL = 86_400
_LEASED_NOW_MAX = (1 << 62) - 1
_LEASED_STATE_VERSION = 1
_LEASED_SCAN_BATCH = 4


@dataclass(frozen=True)
class Dispatch:
    """One leased task: where it lives, which device it belongs to, the
    caller's payload, and the receipt that alone can finish it."""

    offset: int
    key: str
    data: bytes
    receipt: str
    deadline: int


class _LeasedTask:
    __slots__ = ("offset", "key", "done", "receipt", "deadline")

    def __init__(self, offset, key):
        self.offset = offset
        self.key = key
        self.done = False
        self.receipt = None
        self.deadline = None


def _encode_task(key, data):
    raw_key = key.encode("ascii")
    return bytes((len(raw_key),)) + raw_key + data


def _split_task(payload):
    if not payload:
        raise Corruption("empty task frame")
    key_length = payload[0]
    if key_length < 1 or 1 + key_length > len(payload):
        raise Corruption("malformed task frame")
    try:
        key = payload[1:1 + key_length].decode("ascii")
    except UnicodeDecodeError as exc:
        raise Corruption("malformed task key") from exc
    return key, payload[1 + key_length:]


class LeasedWorkJournal:
    """Dispatch durable per-device task queues with expiring leases.

    One task is one frame in a :class:`RetainedJournal` under
    ``directory/events``; tasks for the same key are dispatched strictly in
    submission order, while a lost worker only blocks its own device.  The
    caller performs the actual device operation: this class handles dispatch,
    acknowledgement, and prefix reclamation, and does not promise exactly-once
    execution of the external operation.
    """

    def __init__(self, directory, *, hook=None):
        if hook is not None and not callable(hook):
            raise ValueError("hook must be callable")
        self._dir = os.fspath(directory)
        self._hook = hook
        self._closed = False
        os.makedirs(self._dir, exist_ok=True)
        # The events journal's own flock provides the single-active-handle
        # guarantee for the whole queue.
        self._journal = RetainedJournal(
            os.path.join(self._dir, _LEASED_EVENTS_DIR)
        )
        try:
            self._rebuild()
        except BaseException:
            self._journal.close()
            raise

    # -- opening / state reconstruction ------------------------------------

    def _rebuild(self):
        # In-memory index.  Payloads are streamed in small batches and only
        # the device keys are retained, so reopening a multi-ten-MiB backlog
        # never holds the payloads in memory.
        self._tasks = {}        # offset -> _LeasedTask
        self._order = []        # frame offsets at/after floor, sorted
        self._by_key = {}       # key -> list[offset] in submission order
        self._head_pos = {}     # key -> first possibly unfinished index
        self._receipts = {}     # receipt -> offset
        self._counter = 0

        journal = self._journal
        offset = journal.floor
        end = journal.end
        while offset < end:
            rows = journal.read(offset, limit=_LEASED_SCAN_BATCH)
            if not rows:
                raise Corruption("task scan ended before the journal end")
            for row in rows:
                key, _data = _split_task(row.data)
                self._tasks[row.offset] = _LeasedTask(row.offset, key)
                self._order.append(row.offset)
                self._by_key.setdefault(key, []).append(row.offset)
                offset = row.next_offset

        state = self._load_state()
        if state is not None:
            self._counter = state["counter"]
            for finished in state["done"]:
                task = self._tasks.get(finished)
                if task is not None:
                    task.done = True
            for leased_offset, (receipt, deadline) in state["leases"].items():
                task = self._tasks.get(leased_offset)
                # Entries below the retained floor or beyond the log are
                # stale reconciliation leftovers; silently drop them.
                if task is None or task.done:
                    continue
                task.receipt = receipt
                task.deadline = deadline
                self._receipts[receipt] = leased_offset

        for key, offsets in self._by_key.items():
            position = 0
            while position < len(offsets) and self._tasks[
                offsets[position]
            ].done:
                position += 1
            self._head_pos[key] = position

    def _load_state(self):
        tmp_path = os.path.join(self._dir, _LEASED_STATE_TMP)
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass
        state_path = os.path.join(self._dir, _LEASED_STATE_NAME)
        try:
            with open(state_path, "rb") as handle:
                raw = handle.read()
        except FileNotFoundError:
            return None
        try:
            state = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            raise Corruption("leases state is not valid JSON") from exc
        if not isinstance(state, dict) or state.get(
            "version"
        ) != _LEASED_STATE_VERSION:
            raise Corruption("unsupported leases state version")
        try:
            counter = state["counter"]
            if type(counter) is not int:
                raise Corruption("malformed lease counter")
            done = []
            for offset in state["done"]:
                if type(offset) is not int:
                    raise Corruption("malformed finished offset")
                done.append(offset)
            leases = {}
            for offset_raw, entry in dict(state["leases"]).items():
                if not isinstance(entry, list) or len(entry) != 2:
                    raise Corruption("malformed lease entry")
                receipt, deadline = entry
                if not isinstance(receipt, str) or not receipt:
                    raise Corruption("malformed lease receipt")
                if type(deadline) is not int:
                    raise Corruption("malformed lease deadline")
                leases[int(offset_raw)] = (receipt, deadline)
        except (KeyError, TypeError, ValueError) as exc:
            raise Corruption("malformed leases state") from exc
        if counter < 0 or any(offset < len(MAGIC) for offset in done):
            raise Corruption("malformed leases state")
        return {"counter": counter, "done": done, "leases": leases}

    def _persist_state(self):
        leases = {}
        done = []
        for offset in self._order:
            task = self._tasks[offset]
            if task.done:
                done.append(offset)
            elif task.receipt is not None:
                leases[str(offset)] = [task.receipt, task.deadline]
        payload = json.dumps(
            {
                "version": _LEASED_STATE_VERSION,
                "counter": self._counter,
                "leases": leases,
                "done": done,
            },
            sort_keys=True,
        ).encode("utf-8")
        tmp_path = os.path.join(self._dir, _LEASED_STATE_TMP)
        state_path = os.path.join(self._dir, _LEASED_STATE_NAME)
        with open(tmp_path, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, state_path)
        self._fsync_dir()

    def _fsync_dir(self):
        dir_fd = os.open(self._dir, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    # -- validation --------------------------------------------------------

    @staticmethod
    def _validate_key(key):
        if not isinstance(key, str) or not _LEASED_KEY_RE.fullmatch(key):
            raise ValueError(
                "key must be 1-32 case-sensitive ASCII letters, digits, "
                "'_' or '-'"
            )

    @staticmethod
    def _validate_data(data):
        if type(data) is not bytes or len(data) > _LEASED_MAX_DATA:
            raise ValueError(
                f"data must be bytes of at most {_LEASED_MAX_DATA} bytes"
            )

    @staticmethod
    def _validate_clock(now, ttl):
        if type(now) is not int or not 0 <= now <= _LEASED_NOW_MAX:
            raise ValueError("now must be a true integer in [0, 2**62-1]")
        if type(ttl) is not int or not 1 <= ttl <= _LEASED_MAX_TTL:
            raise ValueError("ttl must be a true integer in [1, 86400]")

    def _require_open(self):
        if self._closed:
            raise RuntimeError("LeasedWorkJournal is closed")

    # -- state -------------------------------------------------------------

    @property
    def floor(self):
        self._require_open()
        return self._journal.floor

    @property
    def end(self):
        self._require_open()
        return self._journal.end

    # -- enqueue -----------------------------------------------------------

    def submit(self, key, data):
        """Append one task and return its frame offset (its task number)."""
        self._require_open()
        self._validate_key(key)
        self._validate_data(data)
        # RetainedJournal rejects frame/byte/int64 quota breaches itself,
        # always before appending anything, so no partial task is created.
        offset = self._journal.append(_encode_task(key, data))
        self._tasks[offset] = _LeasedTask(offset, key)
        self._order.append(offset)
        self._by_key.setdefault(key, []).append(offset)
        if key not in self._head_pos:
            self._head_pos[key] = 0
        if self._hook is not None:
            # The payload append is already synced at this point.
            self._hook("after_payload_sync")
        return offset

    # -- dispatch ----------------------------------------------------------

    def claim(self, now, ttl):
        """Lease the smallest-offset claimable per-device head, or None."""
        self._require_open()
        self._validate_clock(now, ttl)
        best = None
        for key, position in self._head_pos.items():
            offsets = self._by_key[key]
            if position >= len(offsets):
                continue  # every task for this device is finished
            task = self._tasks[offsets[position]]
            if task.receipt is not None and now < task.deadline:
                continue  # valid lease blocks this whole device
            if best is None or task.offset < best.offset:
                best = task
        if best is None:
            return None

        if best.receipt is not None:
            # The previous lease has expired; retire its receipt before the
            # replacement is issued so the late acknowledgement can never
            # resolve to this task again.
            self._receipts.pop(best.receipt, None)
        self._counter += 1
        receipt = f"R{self._counter:010d}-{best.offset}"
        deadline = now + ttl
        best.receipt = receipt
        best.deadline = deadline
        self._receipts[receipt] = best.offset
        self._persist_state()

        rows = self._journal.read(best.offset, limit=1)
        if not rows:
            raise Corruption("claimed task frame vanished")
        _key, data = _split_task(rows[0].data)
        return Dispatch(best.offset, best.key, data, receipt, deadline)

    def finish(self, receipt, now):
        """Finish a task only with its current, unexpired receipt."""
        self._require_open()
        if not isinstance(receipt, str) or not receipt:
            raise ValueError("receipt must be a non-empty string")
        if type(now) is not int or not 0 <= now <= _LEASED_NOW_MAX:
            raise ValueError("now must be a true integer in [0, 2**62-1]")

        offset = self._receipts.get(receipt)
        if offset is None:
            return False
        task = self._tasks[offset]
        # The lease interval excludes the deadline itself.
        if task.done or task.receipt != receipt or now >= task.deadline:
            return False

        task.done = True
        task.receipt = None
        task.deadline = None
        del self._receipts[receipt]
        self._advance_head(task.key)
        self._persist_state()
        if self._hook is not None:
            # The completion decision is already durable at this point.
            self._hook("after_finish_commit")
        return True

    def _advance_head(self, key):
        offsets = self._by_key[key]
        position = self._head_pos[key]
        while position < len(offsets) and self._tasks[
            offsets[position]
        ].done:
            position += 1
        self._head_pos[key] = position

    # -- reclamation -------------------------------------------------------

    def reclaim(self):
        """Physically release the contiguous finished prefix; return floor."""
        self._require_open()
        floor = self._journal.floor
        target = floor
        for index, offset in enumerate(self._order):
            task = self._tasks[offset]
            if not task.done:
                break
            if index + 1 < len(self._order):
                target = self._order[index + 1]
            else:
                target = self._journal.end
        if target == floor:
            return floor

        # Drive the underlying journal through its own consumer cursor so the
        # cut point stays a real, independently inspectable RetainedJournal
        # commit.  A finished later device can never jump an unfinished older
        # frame because target is built from a strict prefix walk.
        self._journal.register(_LEASED_CONSUMER)
        self._journal.ack(_LEASED_CONSUMER, target)
        new_floor = self._journal.prune()

        self._order = [
            offset for offset in self._order if offset >= new_floor
        ]
        for offset in list(self._tasks):
            if offset < new_floor:
                del self._tasks[offset]
        by_key = {}
        for offset in self._order:
            by_key.setdefault(self._tasks[offset].key, []).append(offset)
        self._by_key = by_key
        self._head_pos = {}
        for key, offsets in by_key.items():
            position = 0
            while position < len(offsets) and self._tasks[
                offsets[position]
            ].done:
                position += 1
            self._head_pos[key] = position
        self._persist_state()
        return new_floor

    # -- lifecycle ---------------------------------------------------------

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._journal.close()

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()


# --- CheckpointJournal: full-state baseline plus retained deltas -----------
# Directory layout (implementation details):
#   directory/
#     events/                    RetainedJournal: one AJ1 frame per change
#     checkpoint.json            committed baseline generation and cut
#     state-NNNNNNNNNNNN.ckpt    complete key/value state at that cut
#     state-*.ckpt.tmp           staged baseline, cleaned on open
#     checkpoint.json.tmp        atomic staging file for checkpoint.json
#
# Every change is one AJ1 frame whose body encodes the operation, the key, and
# (for a put) the value.  The baseline is a separate streamed file written in
# ascending key order.  Publishing never copies the delta log; it merges the
# previous baseline with the changed keys of [old_cut, new_cut) by streaming
# values from disk, so a publish writes at most one new baseline plus staging
# metadata.
#
# Frame payload encoding (inside the AJ1 record body):
#   1 byte  kind: 0 = put, 1 = delete
#   1 byte  key length k (1..32)
#   k bytes ASCII key
#   put only: 4-byte big-endian value length n, then n value bytes
#
# Baseline body encoding (after a single JSON header line ending in "\n"):
#   repeated rows in ASCII-key order, each:
#   1 byte key length, key bytes, 4-byte value length, value bytes

_CK_CHECKPOINT_NAME = "checkpoint.json"
_CK_CHECKPOINT_TMP = "checkpoint.json.tmp"
_CK_EVENTS_DIR = "events"
_CK_STATE_PREFIX = "state-"
_CK_STATE_SUFFIX = ".ckpt"
_CK_STATE_TMP_SUFFIX = ".tmp"
_CK_VERSION = 1
_CK_KEY_RE = _NAME_RE
_CK_VALUE_MAX = 1_000_000
_CK_MAX_KEYS = 65536
_CK_MAX_LIVE_BYTES = 64 * 1024 * 1024
_CK_READ_LIMIT_MAX = 16
_CK_SCAN_LIMIT = 4
_CK_COPY_CHUNK = 256 * 1024
_CK_KIND_PUT = 0
_CK_KIND_DELETE = 1


def _ck_encode(key, value):
    raw_key = key.encode("ascii")
    if value is None:
        return bytes((_CK_KIND_DELETE, len(raw_key))) + raw_key
    return (
        bytes((_CK_KIND_PUT, len(raw_key)))
        + raw_key
        + len(value).to_bytes(4, "big")
        + value
    )


def _ck_decode(data):
    if len(data) < 2:
        raise Corruption("empty change frame")
    kind, key_length = data[0], data[1]
    if not 1 <= key_length <= 32 or len(data) < 2 + key_length:
        raise Corruption("malformed change frame")
    try:
        key = data[2:2 + key_length].decode("ascii")
    except UnicodeDecodeError as exc:
        raise Corruption("malformed change key") from exc
    if not _CK_KEY_RE.fullmatch(key):
        raise Corruption("malformed change key")
    rest_length = len(data) - 2 - key_length
    if kind == _CK_KIND_DELETE:
        if rest_length:
            raise Corruption("trailing bytes in delete frame")
        return key, None
    if kind != _CK_KIND_PUT:
        raise Corruption("unknown change kind")
    if rest_length < 4:
        raise Corruption("truncated value length")
    length = int.from_bytes(data[2 + key_length:2 + key_length + 4], "big")
    if length > _CK_VALUE_MAX or rest_length != 4 + length:
        raise Corruption("malformed value in change frame")
    start = 2 + key_length + 4
    return key, data[start:start + length]


@dataclass(frozen=True)
class Change:
    """One decoded journal change: put (value bytes) or delete (None)."""

    offset: int
    next_offset: int
    key: str
    value: bytes


def _ck_read_header(handle):
    line = handle.readline(4096)
    if not line.endswith(b"\n"):
        raise Corruption("missing baseline header")
    try:
        header = json.loads(line.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise Corruption("baseline header is not valid JSON") from exc
    if not isinstance(header, dict) or header.get("version") != _CK_VERSION:
        raise Corruption("unsupported baseline version")
    for field in ("seq", "cut", "count", "bytes"):
        if type(header.get(field)) is not int:
            raise Corruption("malformed baseline header")
    if header["seq"] < 0 or header["count"] < 0 or header["bytes"] < 0:
        raise Corruption("malformed baseline header")
    return header


def _ck_iter_baseline(path):
    """Yield (key, value_length, handle) per row, handle at the value start.

    The generator seeks past any value the caller does not consume, so rows
    can be inspected without loading their payloads.
    """
    with open(path, "rb") as handle:
        header = _ck_read_header(handle)
        for _ in range(header["count"]):
            raw_length = handle.read(1)
            if len(raw_length) != 1 or not 1 <= raw_length[0] <= 32:
                raise Corruption("malformed baseline row")
            key_length = raw_length[0]
            raw_key = handle.read(key_length)
            if len(raw_key) != key_length:
                raise Corruption("truncated baseline key")
            try:
                key = raw_key.decode("ascii")
            except UnicodeDecodeError as exc:
                raise Corruption("malformed baseline key") from exc
            if not _CK_KEY_RE.fullmatch(key):
                raise Corruption("malformed baseline key")
            raw_value_length = handle.read(4)
            if len(raw_value_length) != 4:
                raise Corruption("truncated baseline value length")
            value_length = int.from_bytes(raw_value_length, "big")
            if value_length > _CK_VALUE_MAX:
                raise Corruption("oversized baseline value")
            value_start = handle.tell()
            yield key, value_length, handle
            handle.seek(value_start + value_length)


class _BootstrapRows:
    """One-shot iterator over a baseline snapshot; closes its handle."""

    def __init__(self, handle, count):
        self._handle = handle
        self._remaining = count
        self._closed = False

    def __iter__(self):
        return self

    def __next__(self):
        if self._remaining <= 0:
            self.close()
            raise StopIteration
        handle = self._handle
        raw_length = handle.read(1)
        if len(raw_length) != 1 or not 1 <= raw_length[0] <= 32:
            raise Corruption("malformed baseline row")
        key_length = raw_length[0]
        raw_key = handle.read(key_length)
        if len(raw_key) != key_length:
            raise Corruption("truncated baseline key")
        try:
            key = raw_key.decode("ascii")
        except UnicodeDecodeError as exc:
            raise Corruption("malformed baseline key") from exc
        raw_value_length = handle.read(4)
        if len(raw_value_length) != 4:
            raise Corruption("truncated baseline value length")
        value_length = int.from_bytes(raw_value_length, "big")
        if value_length > _CK_VALUE_MAX:
            raise Corruption("oversized baseline value")
        value = handle.read(value_length)
        if len(value) != value_length:
            raise Corruption("truncated baseline value")
        self._remaining -= 1
        return key, value

    def close(self):
        if not self._closed:
            self._closed = True
            self._handle.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


class CheckpointJournal:
    """Configuration change stream with a full-state baseline for newcomers.

    Every :meth:`put`/:meth:`delete` appends one AJ1 frame to the underlying
    :class:`RetainedJournal` under ``directory/events``.  :meth:`publish`
    snapshots the complete key/value state at a chosen cut into a separate
    baseline file; a new subscriber installs that state with
    :meth:`bootstrap` and then continues from the same cut with :meth:`read`.
    :meth:`prune` physically releases only the prefix that both the published
    baseline and every external subscriber acknowledgement have passed.
    """

    def __init__(self, directory):
        self._dir = os.fspath(directory)
        self._closed = False
        os.makedirs(self._dir, exist_ok=True)
        self._checkpoint_path = os.path.join(self._dir, _CK_CHECKPOINT_NAME)
        self._events = RetainedJournal(os.path.join(self._dir, _CK_EVENTS_DIR))
        try:
            self._load_checkpoint()
            self._cleanup_stale()
        except BaseException:
            self._events.close()
            raise

    # -- persistence helpers ----------------------------------------------

    def _fsync_dir(self):
        dir_fd = os.open(self._dir, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    def _body_path(self, seq):
        return os.path.join(
            self._dir, f"{_CK_STATE_PREFIX}{seq:012d}{_CK_STATE_SUFFIX}"
        )

    def _load_checkpoint(self):
        try:
            with open(self._checkpoint_path, "rb") as handle:
                raw = handle.read()
        except FileNotFoundError:
            self._seq = -1
            self._cut = len(MAGIC)
            self._count = 0
            self._bytes = 0
            return
        try:
            checkpoint = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            raise Corruption("checkpoint.json is not valid JSON") from exc
        if not isinstance(checkpoint, dict) or checkpoint.get(
            "version"
        ) != _CK_VERSION:
            raise Corruption("unsupported checkpoint version")
        try:
            seq = checkpoint["seq"]
            cut = checkpoint["cut"]
            count = checkpoint["count"]
            live_bytes = checkpoint["bytes"]
        except KeyError as exc:
            raise Corruption("malformed checkpoint") from exc
        if not all(
            type(value) is int
            for value in (seq, cut, count, live_bytes)
        ) or seq < 0 or count < 0 or live_bytes < 0:
            raise Corruption("malformed checkpoint")
        path = self._body_path(seq)
        try:
            with open(path, "rb") as handle:
                header = _ck_read_header(handle)
        except FileNotFoundError as exc:
            raise Corruption("published baseline file is missing") from exc
        if (
            header["seq"] != seq
            or header["cut"] != cut
            or header["count"] != count
            or header["bytes"] != live_bytes
        ):
            raise Corruption("checkpoint and baseline header disagree")
        self._seq = seq
        self._cut = cut
        self._count = count
        self._bytes = live_bytes

    def _cleanup_stale(self):
        keep_body = self._body_path(self._seq) if self._seq >= 0 else None
        keep = {_CK_EVENTS_DIR, _CK_CHECKPOINT_NAME}
        if keep_body is not None:
            keep.add(os.path.basename(keep_body))
        for name in os.listdir(self._dir):
            if name in keep:
                continue
            is_junk = (
                name == _CK_CHECKPOINT_TMP
                or (
                    name.startswith(_CK_STATE_PREFIX)
                    and (
                        name.endswith(_CK_STATE_SUFFIX)
                        or name.endswith(
                            _CK_STATE_SUFFIX + _CK_STATE_TMP_SUFFIX
                        )
                    )
                )
            )
            if not is_junk:
                continue
            path = os.path.join(self._dir, name)
            if os.path.islink(path) or not os.path.isfile(path):
                raise Corruption(f"unexpected checkpoint entry: {name}")
            os.unlink(path)
        self._fsync_dir()

    def _commit_checkpoint(self, seq, cut, count, live_bytes):
        payload = json.dumps(
            {
                "version": _CK_VERSION,
                "seq": seq,
                "cut": cut,
                "count": count,
                "bytes": live_bytes,
            },
            sort_keys=True,
        ).encode("utf-8")
        tmp_path = os.path.join(self._dir, _CK_CHECKPOINT_TMP)
        with open(tmp_path, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, self._checkpoint_path)
        self._fsync_dir()

    def _require_open(self):
        if self._closed:
            raise RuntimeError("CheckpointJournal is closed")

    # -- state -------------------------------------------------------------

    @property
    def floor(self):
        self._require_open()
        return self._events.floor

    @property
    def end(self):
        self._require_open()
        return self._events.end

    @property
    def cut(self):
        """The cursor of the published baseline."""
        self._require_open()
        return self._cut

    # -- writes ------------------------------------------------------------

    @staticmethod
    def _validate_key(key):
        if not isinstance(key, str) or not _CK_KEY_RE.fullmatch(key):
            raise ValueError(
                "key must be 1-32 case-sensitive ASCII letters, digits, "
                "'_' or '-'"
            )

    def put(self, key, value):
        """Append a put change and return its frame's logical start offset."""
        self._require_open()
        self._validate_key(key)
        if type(value) is not bytes or len(value) > _CK_VALUE_MAX:
            raise ValueError(
                f"value must be bytes of at most {_CK_VALUE_MAX} bytes"
            )
        return self._events.append(_ck_encode(key, value))

    def delete(self, key):
        """Append a delete change (even for an absent key); return its offset."""
        self._require_open()
        self._validate_key(key)
        return self._events.append(_ck_encode(key, None))

    def read(self, offset, *, limit=16):
        """Return up to ``limit`` contiguous changes starting at ``offset``."""
        self._require_open()
        if type(offset) is not int:
            raise ValueError("offset must be a true integer")
        if (
            type(limit) is not int
            or not 1 <= limit <= _CK_READ_LIMIT_MAX
        ):
            raise ValueError("limit must be an integer from 1 through 16")
        rows = self._events.read(offset, limit=limit)
        changes = []
        for row in rows:
            key, value = _ck_decode(row.data)
            changes.append(Change(row.offset, row.next_offset, key, value))
        return changes

    # -- subscribers -------------------------------------------------------

    def register(self, name):
        self._require_open()
        return self._events.register(name)

    def checkpoint(self, name):
        self._require_open()
        return self._events.checkpoint(name)

    def ack(self, name, next_offset):
        self._require_open()
        self._events.ack(name, next_offset)

    def drop(self, name):
        self._require_open()
        return self._events.drop(name)

    # -- baseline publication ---------------------------------------------

    def _scan_deltas(self, cut):
        """Return {key: (last_frame_offset_or_None, value_length)} at ``cut``."""
        touched = {}
        offset = self._cut
        while offset < cut:
            rows = self._events.read(offset, limit=_CK_SCAN_LIMIT)
            if not rows:
                raise Corruption("delta scan reached end before the cut point")
            progressed = False
            for row in rows:
                if row.offset >= cut:
                    break
                key, value = _ck_decode(row.data)
                touched[key] = (
                    row.offset if value is not None else None,
                    len(value) if value is not None else 0,
                )
                offset = row.next_offset
                progressed = True
            if not progressed:
                raise Corruption("delta scan made no progress")
        if offset != cut:
            raise Corruption("delta scan did not land on the cut point")
        return touched

    def _write_merged(self, out, touched, old_body):
        """Write the complete state at the new cut, streaming large values."""
        keys = sorted(touched)
        index = 0
        rows = _ck_iter_baseline(old_body) if old_body is not None else iter(())
        old = next(rows, None)
        while old is not None or index < len(keys):
            if old is None:
                self._write_touched(out, touched, keys[index])
                index += 1
                continue
            if index >= len(keys):
                self._write_old_row(out, old)
                old = next(rows, None)
                continue
            old_key, _old_length, _handle = old
            key = keys[index]
            if old_key < key:
                self._write_old_row(out, old)
                old = next(rows, None)
            elif old_key == key:
                # The delta supersedes this old row; write it from the delta
                # (or omit it for a delete) and move past the old row.
                self._write_touched(out, touched, key)
                index += 1
                old = next(rows, None)
            else:
                # Touched key sorts before the old row; keep the old row.
                self._write_touched(out, touched, key)
                index += 1

    def _write_old_row(self, out, old):
        old_key, old_length, handle = old
        out.write(bytes((len(old_key),)) + old_key.encode("ascii"))
        out.write(old_length.to_bytes(4, "big"))
        remaining = old_length
        while remaining:
            chunk = handle.read(min(_CK_COPY_CHUNK, remaining))
            if not chunk:
                raise Corruption("truncated baseline value")
            out.write(chunk)
            remaining -= len(chunk)

    def _write_touched(self, out, touched, key):
        offset, _length = touched[key]
        if offset is None:
            return
        rows = self._events.read(offset, limit=1)
        if not rows:
            raise Corruption("touched frame vanished")
        frame_key, value = _ck_decode(rows[0].data)
        if frame_key != key or value is None:
            raise Corruption("touched frame does not match its key")
        raw_key = key.encode("ascii")
        out.write(bytes((len(raw_key),)) + raw_key)
        out.write(len(value).to_bytes(4, "big"))
        out.write(value)

    def publish(self, cut):
        """Publish the complete state at ``cut`` and return that cursor."""
        self._require_open()
        if type(cut) is not int:
            raise ValueError("cut must be a true integer offset")
        if cut < self._cut:
            raise ValueError("cut cannot move before the published baseline")
        end = self._events.end
        if cut > end:
            raise ValueError("cut is beyond the end of the journal")
        if cut != end and not self._events._is_boundary(cut):
            raise ValueError("cut is not a frame boundary")
        if cut == self._cut:
            return self._cut

        touched = self._scan_deltas(cut)
        old_body = self._body_path(self._seq) if self._seq >= 0 else None

        old_sizes = {}
        if old_body is not None:
            for key, length, _handle in _ck_iter_baseline(old_body):
                if key in touched:
                    old_sizes[key] = length

        new_count = self._count
        new_bytes = self._bytes
        for key, (offset, length) in touched.items():
            old_length = old_sizes.get(key)
            if old_length is not None:
                new_count -= 1
                new_bytes -= len(key) + old_length
            if offset is not None:
                new_count += 1
                new_bytes += len(key) + length
        if new_count > _CK_MAX_KEYS:
            raise ValueError("baseline would exceed 65,536 live keys")
        if new_bytes > _CK_MAX_LIVE_BYTES:
            raise ValueError("baseline would exceed 64 MiB of live values")

        seq = self._seq + 1
        path = self._body_path(seq)
        tmp_path = path + _CK_STATE_TMP_SUFFIX
        try:
            with open(tmp_path, "wb") as out:
                header = json.dumps(
                    {
                        "version": _CK_VERSION,
                        "seq": seq,
                        "cut": cut,
                        "count": new_count,
                        "bytes": new_bytes,
                    },
                    sort_keys=True,
                ).encode("utf-8")
                out.write(header + b"\n")
                self._write_merged(out, touched, old_body)
                out.flush()
                os.fsync(out.fileno())
            os.replace(tmp_path, path)
            self._fsync_dir()
        except BaseException:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass
            raise

        try:
            self._commit_checkpoint(seq, cut, new_count, new_bytes)
        except BaseException:
            # The rename inside _commit_checkpoint may or may not have taken
            # effect.  Never delete the staged body here: if the checkpoint
            # does reference it, deleting would corrupt the directory; if it
            # does not, the file is unreferenced and removed on the next open.
            # This handle is unusable after a failed commit.
            self._closed = True
            self._events.close()
            raise

        self._seq = seq
        self._cut = cut
        self._count = new_count
        self._bytes = new_bytes
        if old_body is not None:
            try:
                os.unlink(old_body)
            except FileNotFoundError:
                pass
            self._fsync_dir()
        return cut

    def bootstrap(self):
        """Return ``(cut, rows)``: the published cursor and its key/value rows.

        ``rows`` is a one-shot iterator of ``(key, value)`` pairs in ASCII key
        order.  Reading it does not advance any acknowledgement.  The caller
        must finish or discard it before issuing other calls on this handle.
        """
        self._require_open()
        if self._seq < 0:
            return self._cut, iter(())
        path = self._body_path(self._seq)
        handle = open(path, "rb")
        try:
            header = _ck_read_header(handle)
            if (
                header["seq"] != self._seq
                or header["cut"] != self._cut
                or header["count"] != self._count
            ):
                raise Corruption("baseline header disagrees with the checkpoint")
        except BaseException:
            handle.close()
            raise
        return self._cut, _BootstrapRows(handle, self._count)

    # -- reclamation -------------------------------------------------------

    def prune(self):
        """Physically release the prefix below the safe cut; return the floor."""
        self._require_open()
        boundary = self._cut
        for position in self._events._consumers.values():
            if position < boundary:
                boundary = position
        return self._events._prune_to(boundary)

    # -- lifecycle ---------------------------------------------------------

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._events.close()

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()


# --- RetirableJournal: erase one source without replaying others -----------
# Directory layout (implementation details):
#   directory/
#     events/                 authoritative RetainedJournal of descriptors
#     retirable.json          admitted sources, retirement and pack bases
#     body-<subject>.bin      raw concatenated bodies for one source
#     body-<subject>.bin.tmp  staging copy while pruning a body pack
#
# The events log never contains body bytes.  A source's bodies are concatenated
# in append order in its own pack, so retiring that source only unlinks its own
# files and can never delete another source's identical bytes.  Pruning the
# consumer prefix compacts each surviving source pack from its first surviving
# descriptor onward; the descriptor's body offset is stable and retirable.json
# records the physical base of the compacted pack.

_RETIRE_STATE_NAME = "retirable.json"
_RETIRE_BODY_TEMPLATE = "body-{}.bin"
_RETIRE_STATE_VERSION = 1
_RETIRE_MAX_SUBJECTS = 32
_RETIRE_MAX_DESCRIPTORS = 8192
_RETIRE_MAX_LIVE_BYTES = 64 * 1024 * 1024
_RETIRE_MAX_DATA = 1_000_000
_RETIRE_READ_LIMIT_MAX = 16
_RETIRE_SCAN_BATCH = 64
_RETIRE_COPY_CHUNK = 256 * 1024
_RETIRE_DESCRIPTOR_HEADER = Struct(">BBQI")


@dataclass(frozen=True)
class RetirableRecord:
    """One audit event: stable cursor, source, and bytes or retirement marker."""

    offset: int
    next_offset: int
    subject: str
    data: object


def _encode_retirable_descriptor(subject, body_offset, body_length):
    raw_subject = subject.encode("ascii")
    return (
        _RETIRE_DESCRIPTOR_HEADER.pack(
            _RETIRE_STATE_VERSION,
            len(raw_subject),
            body_offset,
            body_length,
        )
        + raw_subject
    )


def _decode_retirable_descriptor(payload):
    if (
        not isinstance(payload, bytes)
        or len(payload) < _RETIRE_DESCRIPTOR_HEADER.size
    ):
        raise Corruption("malformed retirable descriptor")
    version, subject_length, body_offset, body_length = (
        _RETIRE_DESCRIPTOR_HEADER.unpack_from(payload)
    )
    if (
        version != _RETIRE_STATE_VERSION
        or not 1 <= subject_length <= 32
        or body_length > _RETIRE_MAX_DATA
        or len(payload)
        != _RETIRE_DESCRIPTOR_HEADER.size + subject_length
    ):
        raise Corruption("malformed retirable descriptor")
    try:
        subject = payload[
            _RETIRE_DESCRIPTOR_HEADER.size:
        ].decode("ascii")
    except UnicodeDecodeError as exc:
        raise Corruption("malformed retirable subject") from exc
    if not _NAME_RE.fullmatch(subject):
        raise Corruption("malformed retirable subject")
    return subject, body_offset, body_length


class RetirableJournal:
    """Audit journal whose descriptors stay replayable after source erasure.

    The cursor/consumer implementation is the ordinary
    :class:`RetainedJournal` under ``directory/events``.  Descriptor frames
    contain source and body-location metadata only; raw bodies live in
    source-specific pack files in the surrounding directory.
    """

    def __init__(self, directory, *, hook=None):
        if hook is not None and not callable(hook):
            raise ValueError("hook must be callable")
        self._dir = os.fspath(directory)
        self._hook = hook
        self._closed = False
        os.makedirs(self._dir, exist_ok=True)

        # The events lock is also the single-active-handle lock for the whole
        # RetirableJournal directory.
        self._events = RetainedJournal(
            os.path.join(self._dir, "events")
        )
        try:
            self._state_path = os.path.join(self._dir, _RETIRE_STATE_NAME)
            self._subjects = {}
            self._heads = {}
            self._keep_from = {}
            self._subject_live = {}
            self._frame_count = 0
            self._live_bytes = 0
            self._load_state()
            self._finish_retirements()
            self._scan_descriptors()
            # Complete an interrupted descriptor-prefix/body compaction before
            # the handle becomes usable.  In a quiescent directory this is a
            # no-op.
            self._compact_body_packs()
            self._reconcile_body_files()
        except BaseException:
            self._events.close()
            raise

    # -- paths and small helpers -------------------------------------------

    def _body_path(self, subject):
        return os.path.join(
            self._dir, _RETIRE_BODY_TEMPLATE.format(subject)
        )

    def _body_name(self, subject):
        return _RETIRE_BODY_TEMPLATE.format(subject)

    def _fsync_dir(self):
        dir_fd = os.open(self._dir, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    def _require_open(self):
        if self._closed:
            raise RuntimeError("RetirableJournal is closed")

    @staticmethod
    def _validate_subject(subject):
        if not isinstance(subject, str) or not _NAME_RE.fullmatch(subject):
            raise ValueError(
                "subject must be 1-32 case-sensitive ASCII letters, digits, "
                "'_' or '-'"
            )

    def _invoke_hook(self, phase):
        if self._hook is not None:
            self._hook(phase)

    # -- state persistence --------------------------------------------------

    def _load_state(self):
        # Atomic state publication uses replace(); an leftover tmp is never the
        # committed state and can safely be removed under the active flock.
        try:
            os.unlink(self._state_path + ".tmp")
        except FileNotFoundError:
            pass
        try:
            with open(self._state_path, "rb") as handle:
                raw = handle.read()
        except FileNotFoundError:
            self._subjects = {}
            self._save_state()
            return

        try:
            state = json.loads(raw.decode("utf-8"))
            if (
                not isinstance(state, dict)
                or state.get("version") != _RETIRE_STATE_VERSION
                or not isinstance(state.get("subjects"), dict)
            ):
                raise Corruption("malformed retirable.json")
            subjects = state["subjects"]
            loaded = {}
            for subject, info in subjects.items():
                self._validate_subject(subject)
                if not isinstance(info, dict):
                    raise Corruption("malformed retirable subject state")
                retired = info.get("retired", False)
                base = info.get("base", 0)
                if type(retired) is not bool:
                    raise Corruption("malformed retired flag")
                if type(base) is not int or base < 0:
                    raise Corruption("malformed body pack base")
                loaded[subject] = {"retired": retired, "base": base}
        except (ValueError, UnicodeDecodeError, KeyError, TypeError) as exc:
            raise Corruption("invalid retirable.json") from exc
        self._subjects = loaded

    def _save_state(self):
        subjects = {
            subject: {
                "retired": bool(info["retired"]),
                "base": int(info["base"]),
            }
            for subject, info in self._subjects.items()
        }
        payload = json.dumps(
            {"version": _RETIRE_STATE_VERSION, "subjects": subjects},
            sort_keys=True,
        ).encode("utf-8")
        tmp_path = self._state_path + ".tmp"
        with open(tmp_path, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, self._state_path)
        self._fsync_dir()

    # -- startup scan and recovery -----------------------------------------

    def _remove_subject_files(self, subject):
        """Remove every body pack/staging file for exactly this subject."""
        exact_name = self._body_name(subject)
        staging_prefix = exact_name + "."
        removed = False
        for name in os.listdir(self._dir):
            if name != exact_name and not name.startswith(staging_prefix):
                continue
            path = os.path.join(self._dir, name)
            if os.path.islink(path) or os.path.isfile(path):
                os.unlink(path)
                removed = True
            elif os.path.isdir(path):
                raise Corruption(f"unexpected body directory entry: {name}")
        return removed

    def _finish_retirements(self):
        removed_any = False
        for subject, info in self._subjects.items():
            if info["retired"] and self._remove_subject_files(subject):
                removed_any = True
        if removed_any:
            self._fsync_dir()

    def _scan_descriptors(self):
        self._frame_count = 0
        self._live_bytes = 0
        self._heads = {}
        self._keep_from = {}
        self._subject_live = {}

        offset = self._events.floor
        end = self._events.end
        while offset < end:
            rows = self._events.read(offset, limit=_RETIRE_SCAN_BATCH)
            if not rows:
                raise Corruption("descriptor scan ended before journal end")
            for row in rows:
                subject, body_offset, body_length = (
                    _decode_retirable_descriptor(row.data)
                )
                self._frame_count += 1
                body_end = body_offset + body_length
                self._heads[subject] = max(
                    self._heads.get(subject, 0), body_end
                )
                self._keep_from[subject] = min(
                    self._keep_from.get(subject, body_offset), body_offset
                )

                info = self._subjects.get(subject)
                if info is None:
                    # Tolerate an interrupted append/state ordering without
                    # losing a successfully written descriptor frame.
                    info = {"retired": False, "base": 0}
                    self._subjects[subject] = info
                if not info["retired"]:
                    self._live_bytes += body_length
                    self._subject_live[subject] = (
                        self._subject_live.get(subject, 0) + body_length
                    )
            next_offset = rows[-1].next_offset
            if next_offset <= offset:
                raise Corruption("descriptor scan made no progress")
            offset = next_offset
        if offset != end:
            raise Corruption("descriptor scan did not reach journal end")

    def _reconcile_body_files(self):
        state_dirty = False
        for subject, info in self._subjects.items():
            if info["retired"]:
                continue
            path = self._body_path(subject)
            staging_path = path + ".tmp"
            try:
                os.unlink(staging_path)
                self._fsync_dir()
            except FileNotFoundError:
                pass
            head = self._heads.get(subject)
            if head is None:
                # No retained descriptor references this pack.
                if self._remove_subject_files(subject):
                    self._fsync_dir()
                if info["base"] != 0:
                    info["base"] = 0
                    state_dirty = True
                continue

            base = info["base"]
            expected_size = head - base
            if expected_size < 0:
                raise Corruption(f"body pack base is beyond {subject} head")
            try:
                if os.path.islink(path):
                    raise Corruption(f"body pack is a symlink: {subject}")
                actual_size = os.stat(path).st_size
            except FileNotFoundError:
                if expected_size:
                    raise Corruption(f"missing body pack for {subject}")
                continue
            if actual_size < expected_size:
                raise Corruption(f"short body pack for {subject}")
            if actual_size > expected_size:
                with open(path, "r+b") as handle:
                    handle.truncate(expected_size)
                    handle.flush()
                    os.fsync(handle.fileno())
                self._fsync_dir()
        if state_dirty:
            self._save_state()

    # -- boundaries ---------------------------------------------------------

    @property
    def floor(self):
        self._require_open()
        return self._events.floor

    @property
    def end(self):
        self._require_open()
        return self._events.end

    # -- appending and reading ---------------------------------------------

    def append(self, subject, data):
        """Append one descriptor plus its raw body; return the frame cursor."""
        self._require_open()
        self._validate_subject(subject)
        if not isinstance(data, bytes) or len(data) > _RETIRE_MAX_DATA:
            raise ValueError(
                f"data must be bytes of at most {_RETIRE_MAX_DATA} bytes"
            )

        info = self._subjects.get(subject)
        if info is not None and info["retired"]:
            raise ValueError(f"subject {subject!r} has been retired")
        if info is None and len(self._subjects) >= _RETIRE_MAX_SUBJECTS:
            raise ValueError(
                f"at most {_RETIRE_MAX_SUBJECTS} subjects are admitted"
            )
        if self._frame_count >= _RETIRE_MAX_DESCRIPTORS:
            raise ValueError(
                f"at most {_RETIRE_MAX_DESCRIPTORS} descriptor frames can be "
                "retained"
            )
        if self._live_bytes + len(data) > _RETIRE_MAX_LIVE_BYTES:
            raise ValueError(
                "unretired readable bodies would exceed 64 MiB"
            )

        body_offset = self._heads.get(subject, 0)
        if data:
            path = self._body_path(subject)
            # Opening in append mode writes at the current physical end.  After
            # compaction physical size is head - base, so this remains the
            # correct append point.
            with open(path, "ab") as handle:
                view = memoryview(data)
                written = 0
                while written < len(view):
                    count = handle.write(view[written:])
                    if not count:
                        raise OSError("short write to body pack")
                    written += count
                handle.flush()
                os.fsync(handle.fileno())

        descriptor = _encode_retirable_descriptor(
            subject, body_offset, len(data)
        )
        offset = self._events.append(descriptor)

        self._frame_count += 1
        body_end = body_offset + len(data)
        self._heads[subject] = max(self._heads.get(subject, 0), body_end)
        if info is None:
            info = {"retired": False, "base": 0}
            self._subjects[subject] = info
            self._save_state()
        self._live_bytes += len(data)
        self._subject_live[subject] = (
            self._subject_live.get(subject, 0) + len(data)
        )
        return offset

    def read(self, offset=None, *, limit=16):
        """Read up to 16 consecutive events without advancing consumers."""
        self._require_open()
        if offset is None:
            offset = self._events.floor
        if (
            type(offset) is not int
            or type(limit) is not int
            or not 1 <= limit <= _RETIRE_READ_LIMIT_MAX
        ):
            raise ValueError(
                "offset must be a true integer and limit must be a true "
                "integer from 1 through 16"
            )

        rows = self._events.read(offset, limit=limit)
        records = []
        handles = {}
        try:
            for row in rows:
                subject, body_offset, body_length = (
                    _decode_retirable_descriptor(row.data)
                )
                info = self._subjects[subject]
                if info["retired"]:
                    data = None
                elif body_length == 0:
                    data = b""
                else:
                    handle = handles.get(subject)
                    if handle is None:
                        handle = open(self._body_path(subject), "rb")
                        handles[subject] = handle
                    physical_offset = body_offset - info["base"]
                    handle.seek(physical_offset)
                    data = handle.read(body_length)
                    if len(data) != body_length:
                        raise Corruption(
                            f"short body read at journal offset {row.offset}"
                        )
                records.append(
                    RetirableRecord(
                        row.offset, row.next_offset, subject, data
                    )
                )
        finally:
            for handle in handles.values():
                handle.close()
        return records

    # -- retirement ---------------------------------------------------------

    def retire(self, subject):
        """Durably retire a source; return True only on its first retirement."""
        self._require_open()
        self._validate_subject(subject)
        info = self._subjects.get(subject)
        if info is not None and info["retired"]:
            return False
        if info is None and len(self._subjects) >= _RETIRE_MAX_SUBJECTS:
            raise ValueError(
                f"at most {_RETIRE_MAX_SUBJECTS} subjects are admitted"
            )

        has_bodies = self._subject_live.get(subject, 0) > 0
        if has_bodies:
            # No retirement marker or unlink has happened yet: a kill here
            # reopens as the fully writable original source.
            self._invoke_hook("before_erase")

        if info is None:
            info = {"retired": True, "base": 0}
            self._subjects[subject] = info
        else:
            info["retired"] = True

        released = self._subject_live.pop(subject, 0)
        self._live_bytes -= released
        # Retirement is now committed.  Reopening after any later exit must
        # finish erasure even if this call did not return.
        self._save_state()

        removed = self._remove_subject_files(subject)
        if removed:
            self._fsync_dir()
        if has_bodies:
            # All known copies have been unlinked.  A kill in the hook leaves
            # the committed retired marker for startup reconciliation.
            self._invoke_hook("after_first_removal")
        return True

    # -- consumer cursors and prefix pruning -------------------------------

    def register(self, name):
        self._require_open()
        return self._events.register(name)

    def checkpoint(self, name):
        self._require_open()
        return self._events.checkpoint(name)

    def ack(self, name, next_offset):
        self._require_open()
        self._events.ack(name, next_offset)

    def drop(self, name):
        self._require_open()
        return self._events.drop(name)

    def _copy_body_suffix(self, source_path, staging_path, skip_bytes):
        source_size = os.stat(source_path).st_size
        if skip_bytes > source_size:
            raise Corruption("body compaction starts beyond pack end")
        with open(source_path, "rb") as source, open(
            staging_path, "wb"
        ) as staging:
            source.seek(skip_bytes)
            while True:
                chunk = source.read(_RETIRE_COPY_CHUNK)
                if not chunk:
                    break
                staging.write(chunk)
            staging.flush()
            os.fsync(staging.fileno())

    def _compact_body_packs(self):
        state_dirty = False
        for subject, info in self._subjects.items():
            if info["retired"]:
                continue
            keep_from = self._keep_from.get(subject)
            old_base = info["base"]
            path = self._body_path(subject)

            if keep_from is None:
                self._remove_subject_files(subject)
                self._heads.pop(subject, None)
                if old_base != 0:
                    info["base"] = 0
                    state_dirty = True
                continue

            skip_bytes = keep_from - old_base
            if skip_bytes < 0:
                raise Corruption(
                    f"body compaction moved backwards for {subject}"
                )
            if skip_bytes == 0:
                continue

            staging_path = path + ".tmp"
            self._copy_body_suffix(path, staging_path, skip_bytes)
            os.replace(staging_path, path)
            self._fsync_dir()
            info["base"] = keep_from
            state_dirty = True

        if state_dirty:
            self._save_state()

    def prune(self):
        """Prune the consumer-safe descriptor prefix and its body bytes."""
        self._require_open()
        old_floor = self._events.floor
        new_floor = self._events.prune()
        if new_floor == old_floor:
            # Quiescent packs already correspond to old_floor; there is no new
            # prefix body data to remove.
            return new_floor
        # Rebuild only from surviving descriptors, then compact source packs to
        # the first surviving body offset for each source.
        self._scan_descriptors()
        self._compact_body_packs()
        return new_floor

    # -- lifecycle ----------------------------------------------------------

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._events.close()

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()
