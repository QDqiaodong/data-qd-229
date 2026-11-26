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
