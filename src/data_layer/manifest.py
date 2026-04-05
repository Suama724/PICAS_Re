"""
用来记录索引信息之类
相当于更多地用来存一些元信息
因而也会非常简短
"""

from dataclasses import asdict, dataclass
import json 
from pathlib import Path

from .config import DEFAULT_SEGMENT_ID

@dataclass(slots=True)
class Manifest:
    next_paper_id: int = 1
    active_segment_id: int = DEFAULT_SEGMENT_ID
    total_records: int = 0
    active_records: int = 0
    storage_revision: int = 0

    def allocate_paper_id(self) -> int:
        paper_id = self.next_paper_id
        self.next_paper_id += 1
        return paper_id
    
    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(asdict(self), ensure_ascii=False, indent=2),
        )

    @classmethod
    def load(cls, path: Path):
        if not path.exists():
            return cls()
        info = json.loads(path.read_text(encoding="utf-8"))
        return cls(
            next_paper_id=int(info.get("next_paper_id", 1)),
            active_segment_id=int(info.get("active_segment_id", DEFAULT_SEGMENT_ID)),
            total_records=int(info.get("total_records", 0)),
            active_records=int(info.get("active_records", 0)),
            storage_revision=int(info.get("storage_revision", 0)),
        )
