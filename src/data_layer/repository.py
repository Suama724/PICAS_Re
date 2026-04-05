from pathlib import Path

from .config import (
    DOC_TABLE_FILENAME,
    MANIFEST_FILENAME,
    SEGMENT_FILENAME_TEMPLATE,
    SEGMENT_MAX_BYTES,
    ensure_lib_dirs,
)
from .doc_table import DocTable, DocTableEntry
from .manifest import Manifest
from .segment_store import SegmentStore
from .serializer import deserialize_paper, serialize_paper
from .structures import Paper


class Repository:
    def __init__(
        self,
        library_path: str | Path,
        auto_flush: bool = True,
        segment_max_bytes: int = SEGMENT_MAX_BYTES,
    ) -> None:
        dirs = ensure_lib_dirs(Path(library_path))
        self.root = dirs["root"]
        self.id_concrete_info_dir = dirs["id_concrete_info_dir"]
        self.id_meta_info_dir = dirs["id_meta_info_dir"]
        self.search_index_dir = dirs["search_index_dir"]

        self.auto_flush = auto_flush
        self.segment_max_bytes = segment_max_bytes
        self.manifest_path = self.id_meta_info_dir / MANIFEST_FILENAME
        self.doc_table_path = self.id_meta_info_dir / DOC_TABLE_FILENAME

        self.manifest = Manifest.load(self.manifest_path)
        self.doc_table = DocTable(self.doc_table_path)
        self._segment_stores: dict[int, SegmentStore] = {}

    @property
    def storage_revision(self) -> int:
        return self.manifest.storage_revision

    def add(self, paper: Paper) -> int:
        paper_id = self.manifest.allocate_paper_id()
        paper_to_store = paper.with_paper_id(paper_id)
        concrete_info = serialize_paper(paper_to_store)
        self._maybe_rollover(concrete_info)

        active_segment_id = self.manifest.active_segment_id
        segment_store = self._get_segment_store(active_segment_id)
        offset = segment_store.append(concrete_info)

        self.doc_table.put(
            DocTableEntry(
                paper_id=paper_id,
                segment_id=active_segment_id,
                offset=offset,
            )
        )
        self.manifest.total_records += 1
        self.manifest.active_records += 1
        self.manifest.storage_revision += 1
        if self.auto_flush:
            self._flush_meta()
        return paper_id

    def get(self, paper_id: int) -> Paper | None:
        entry = self.doc_table.get(paper_id)
        if entry is None or entry.deleted:
            return None
        segment_store = self._get_segment_store(entry.segment_id)
        concrete_info = segment_store.read(entry.offset)
        return deserialize_paper(concrete_info)

    def delete(self, paper_id: int) -> bool:
        entry = self.doc_table.get(paper_id)
        if entry is None or entry.deleted:
            return False

        self.doc_table.mark_deleted(paper_id)
        self.manifest.active_records -= 1
        self.manifest.storage_revision += 1
        if self.auto_flush:
            self._flush_meta()
        return True

    def all_active_paper_ids(self) -> list[int]:
        return list(self.iter_active_paper_ids())

    def iter_active_paper_ids(self):
        return self.doc_table.iter_active_paper_ids()

    def count(self) -> int:
        return self.manifest.active_records

    def flush(self) -> None:
        self._flush_meta()

    def segment_paths(self) -> list[Path]:
        return sorted(self.id_concrete_info_dir.glob("data.seg*.bin"))

    def _maybe_rollover(self, concrete_info: bytes) -> None:
        segment_store = self._get_segment_store(self.manifest.active_segment_id)
        next_record_size = SegmentStore.record_size(len(concrete_info))
        current_size = segment_store.size_bytes()
        if current_size > 0 and current_size + next_record_size > self.segment_max_bytes:
            self.manifest.active_segment_id += 1

    def _segment_path(self, segment_id: int) -> Path:
        return self.id_concrete_info_dir / SEGMENT_FILENAME_TEMPLATE.format(segment_id=segment_id)

    def _get_segment_store(self, segment_id: int) -> SegmentStore:
        if segment_id not in self._segment_stores:
            self._segment_stores[segment_id] = SegmentStore(self._segment_path(segment_id))
        return self._segment_stores[segment_id]

    def _flush_meta(self) -> None:
        self.manifest.save(self.manifest_path)
