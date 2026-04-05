from pathlib import Path
import struct

HEADER_STRUCT = struct.Struct(">I")


class SegmentStore:
    """
    之前的doctable用来存索引信息
    这个专门用来处理具体info的读与存
    其格式应该是offset信息加上后续序列化后的乱七八糟的信息
    """
    def __init__(self, segment_path: Path) -> None:
        self.segment_path = segment_path
        self.segment_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.segment_path.exists():
            self.segment_path.write_bytes(b"")

    @staticmethod
    def record_size(info_len: int) -> int:
        return HEADER_STRUCT.size + info_len
    
    def size_bytes(self) -> int:
        return self.segment_path.stat().st_size

    def append(self, info: bytes) -> int:
        with self.segment_path.open("ab") as fh:
            offset = fh.tell()
            fh.write(HEADER_STRUCT.pack(len(info)))
            fh.write(info)
        return offset
    
    def read(self, offset: int) -> bytes:
        with self.segment_path.open("rb") as f:
            f.seek(offset)
            header = f.read(HEADER_STRUCT.size)
            if len(header) != HEADER_STRUCT.size:
                raise ValueError(f"Bad offset {offset}")
            info_len = HEADER_STRUCT.unpack(header)[0]
            info = f.read(info_len)
            if len(info) != info_len:
                raise ValueError(f"Corrupted at offset {offset}")
            return info