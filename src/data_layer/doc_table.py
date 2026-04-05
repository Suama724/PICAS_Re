from dataclasses import dataclass
from pathlib import Path
import struct

'''
这里利用了索引指引内容的思路找数据
先规定一种统一的对id的存储格式，主要是保证其对每个entry定长
从而对于给定id，就可以直接序号乘每个entry的固定长度时空O(1)找到目标
得到目标的offset与setment id就可以用类似的方法直接去提取内容
从而避开了扫盘

这里利用py自带的struct存二进制
第一行相当于规范制定
>: 大端序（高位字节存在低地址）
Q: paper_id unsigned ll 8B
I: segment_id unsigned int 4B
Q: offset unsigned ll 8B
B: tombstone unsigned char 1B （1为已删除）

则单条长度为 8+4+8+1=21
删除标记为记一下方便直接提取省的每个还要内部遍历
'''
ENTRY_STRUCT = struct.Struct(">QIQB")
ENTRY_SIZE = ENTRY_STRUCT.size
DELETED_FLAG_OFFSET = ENTRY_SIZE - 1

@dataclass(slots=True)
class DocTableEntry:
    paper_id: int
    segment_id: int
    offset: int
    deleted: bool = False

class DocTable:
    '''
    核心函数就三个， put， get与load,load加载表，get从内存或本地找paper
    额外的方法：mark_deleted， all_active_paper_ids（找没被删的）

    一旦有单子发生改变，就顺便存到缓存里加速
    '''
    def __init__(self, path: Path) -> None:
        self.path = path
        self.entries: dict[int, DocTableEntry] | None = None
        # 缓存记一下总records数量与当前总位数方便尾部append
        self._record_count_cache: int | None = None
        self._size_bytes_cache: int | None = None
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.write_bytes(b"")
        self._refresh_runtime_caches()        

    def _refresh_runtime_caches(self) -> None:
        size_bytes = self.path.stat().st_size
        if size_bytes % ENTRY_SIZE != 0:
            raise ValueError(f"Corrupted doc table size: {self.path}")
        self._size_bytes_cache = size_bytes
        self._record_count_cache = size_bytes // ENTRY_SIZE

    def put(self, entry: DocTableEntry) -> None:
        if entry.paper_id < 1: 
            raise ValueError(f"Invalid paper_id: {entry.paper_id}")

        next_paper_id = self.record_count() + 1

        if entry.paper_id != next_paper_id:
            raise ValueError(
                f"DocTable append out of order: expected paper_id {next_paper_id}, got {entry.paper_id}"
            )

        with self.path.open("ab") as f:
            f.write(
                ENTRY_STRUCT.pack(
                    entry.paper_id,
                    entry.segment_id,
                    entry.offset,
                    1 if entry.deleted else 0,
                )
            )
        if self._record_count_cache is not None:
            self._record_count_cache += 1
        if self._size_bytes_cache is not None:
            self._size_bytes_cache += ENTRY_SIZE
        if self.entries is not None:
            self.entries[entry.paper_id] = entry

    def record_count(self) -> int:
        if self.entries is not None:
            return len(self.entries)
        if self._record_count_cache is not None:
            return self._record_count_cache

        size_bytes = self.path.stat().st_size
        if size_bytes % ENTRY_SIZE != 0:
            raise ValueError(f"Corrupted doc table size: {self.path}")
        return size_bytes // ENTRY_SIZE

    def get(self, paper_id: int) -> DocTableEntry | None:
        if paper_id < 1:
            raise ValueError(f"Invalid paper_id: {paper_id}")
        # 先查内存
        if self.entries is not None:
            return self.entries.get(paper_id)

        entry_offset = self._entry_offset(paper_id)
        if self._known_size_bytes() < entry_offset + ENTRY_SIZE:
            return None

        with self.path.open("rb") as f:
            # seek可以直接跳到目标位置
            f.seek(entry_offset)
            chunk = f.read(ENTRY_SIZE)

        if len(chunk) != ENTRY_SIZE:
            raise ValueError(f"Corrupted doc table: {self.path}")

        return self._unpack_entry(chunk, expected_paper_id=paper_id)

    def _entry_offset(self, paper_id: int) -> int:
        return (paper_id - 1) * ENTRY_SIZE

    def _known_size_bytes(self) -> int:
        if self._size_bytes_cache is not None:
            return self._size_bytes_cache
        size_bytes = self.path.stat().st_size
        if size_bytes % ENTRY_SIZE != 0:
            raise ValueError(f"Corrupted doc table size: {self.path}")
        self._size_bytes_cache = size_bytes
        self._record_count_cache = size_bytes // ENTRY_SIZE
        return size_bytes

    @staticmethod
    def _unpack_entry(chunk: bytes, expected_paper_id: int | None = None) -> DocTableEntry:
        paper_id, segment_id, offset, deleted = ENTRY_STRUCT.unpack(chunk)
        if expected_paper_id is not None and paper_id != expected_paper_id:
            raise ValueError(
                f"Doc table paper_id mismatch: expected {expected_paper_id}, got {paper_id}"
            )
        return DocTableEntry(
            paper_id=paper_id,
            segment_id=segment_id,
            offset=offset,
            deleted=bool(deleted),
        )

    def mark_deleted(self, paper_id: int) -> None:
        entry = self.get(paper_id)
        if entry is None or entry.deleted:
            return

        with self.path.open("r+b") as f:
            f.seek(self._entry_offset(paper_id) + DELETED_FLAG_OFFSET)
            f.write(b"\x01")
        if self.entries is not None:
            self.entries[paper_id] = DocTableEntry(
                paper_id=entry.paper_id,
                segment_id=entry.segment_id,
                offset=entry.offset,
                deleted=True,
            )

    def all_active_paper_ids(self) -> list[int]:
        return list(self.iter_active_paper_ids())

    def iter_active_paper_ids(self):
        if self.entries is not None:
            for paper_id in sorted(self.entries):
                entry = self.entries[paper_id]
                if not entry.deleted:
                    yield paper_id
            return
        
        with self.path.open("rb") as f:
            while chunk := f.read(ENTRY_SIZE):
                if len(chunk) != ENTRY_SIZE:
                    raise ValueError(f"Corrupted doc table: {self.path}")
                entry = self._unpack_entry(chunk)
                if not entry.deleted:
                    yield entry.paper_id

    @classmethod
    def load(cls, path: Path):
        table = cls(path)
        table.entries = {}

        with path.open("rb") as f:
            while chunk := f.read(ENTRY_SIZE):
                if len(chunk) != ENTRY_SIZE:
                    raise ValueError(f"Corrupted doc table: {path}")
                entry = table._unpack_entry(chunk)
                table.entries[entry.paper_id] = entry
        table._record_count_cache = len(table.entries)
        table._size_bytes_cache = len(table.entries) * ENTRY_SIZE
        return table