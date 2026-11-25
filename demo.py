from pathlib import Path
from tempfile import TemporaryDirectory

from journal import Journal


if __name__ == "__main__":
    with TemporaryDirectory() as root:
        file = Path(root) / "stream.aj"
        donor = Path(root) / "donor.aj"
        with Journal(file) as log:
            assert log.append(b"started") == 4
            checkpoint = log.read()[0].next_offset
        with Journal(donor) as sample:
            offset = sample.append(b"unfinished")
        partial = donor.read_bytes()[offset:-2]
        with file.open("ab") as torn:
            torn.write(partial)
        with Journal(file) as log:
            assert log.recovered_bytes == len(partial)
            assert log.read(checkpoint) == []
            assert log.append(b"finished") == checkpoint
            rows = log.read()
            assert [r.data for r in rows] == [b"started", b"finished"]
            print("discarded tail bytes:", log.recovered_bytes)
            print("replayed:", [(r.offset, r.data.decode()) for r in rows])
