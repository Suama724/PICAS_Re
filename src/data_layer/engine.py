from collections.abc import Iterator
from pathlib import Path
import shutil

from .disk_index import DiskIndex, IndexBuilder
from .repository import Repository
from .structures import Paper
from .xml_parser import parse_dblp_xml


class DatabaseEngine:
    """
    准备用他作为唯一接口对接service
    """
    def __init__(self, lib_path: str | Path) -> None:
        self.lib_path = Path(lib_path)
        # 记录下更新状态，发生更新配合flush
        self._logical_revision = 0
        self._open_layers()
        self._repair_search_index_if_needed()

    @property
    def search_index_dir(self) -> Path:
        return self._repository.search_index_dir

    def import_xml(self, xml_path: str | Path) -> dict[str, int]:
        self._reset_lib_path()

        builder = IndexBuilder()
        total_records = 0
        for paper in parse_dblp_xml(xml_path):
            paper_id = self._repository.add(paper)
            builder.add_paper(paper, paper_id)
            total_records += 1

        self._repository.flush()
        stats = self._disk_index.replace_with_builder(
            builder,
            total_records,
            source_revision=self._repository.storage_revision,
        )
        self._update_logical_revision()
        return stats

    def add_paper(self, paper: Paper) -> int:
        paper_id = self._repository.add(paper)
        self._repository.flush()
        self._disk_index.add_paper(paper.with_paper_id(paper_id))
        self._disk_index.flush(source_revision=self._repository.storage_revision)
        self._update_logical_revision()
        return paper_id

    def get_paper(self, paper_id: int) -> Paper | None:
        return self._repository.get(paper_id)

    def delete_paper(self, paper_id: int) -> bool:
        deleted = self._repository.delete(paper_id)
        if deleted:
            self._repository.flush()
            self._disk_index.delete_paper(paper_id)
            self._disk_index.flush(source_revision=self._repository.storage_revision)
            self._update_logical_revision()
        return deleted

    def count_papers(self) -> int:
        return self._repository.count()

    def iter_active_paper_ids(self) -> Iterator[int]:
        return self._repository.iter_active_paper_ids()

    def iter_active_papers(self) -> Iterator[Paper]:
        for paper_id in self._repository.iter_active_paper_ids():
            paper = self._repository.get(paper_id)
            if paper is not None:
                yield paper

    def flush(self) -> None:
        self._repository.flush()
        self._disk_index.flush(source_revision=self._repository.storage_revision)

    def logical_revision(self) -> int:
        return self._logical_revision

    def find_paper_ids_by_author(self, author_name: str) -> list[int]:
        return self._disk_index.find_paper_ids_by_author(author_name)

    def find_paper_ids_by_title(self, title: str) -> list[int]:
        return self._disk_index.find_paper_ids_by_title(title)

    def find_paper_ids_by_year(self, year: int) -> list[int]:
        return self._disk_index.find_paper_ids_by_year(year)

    def find_paper_ids_by_venue(self, venue: str) -> list[int]:
        return self._disk_index.find_paper_ids_by_venue(venue)

    def find_paper_ids_by_title_keywords(self, query: str) -> list[int]:
        return self._disk_index.find_paper_ids_by_title_keywords(query)

    def find_paper_ids_by_keywords(self, query: str) -> list[int]:
        return self.find_paper_ids_by_title_keywords(query)

    def find_paper_ids_by_author_keywords(self, query: str) -> list[int]:
        return self._disk_index.find_paper_ids_by_author_keywords(query)

    def find_papers_by_author(self, author_name: str) -> list[Paper]:
        return self._papers_from_ids(self.find_paper_ids_by_author(author_name))

    def find_papers_by_title(self, title: str) -> list[Paper]:
        return self._papers_from_ids(self.find_paper_ids_by_title(title))

    def find_papers_by_year(self, year: int) -> list[Paper]:
        return self._papers_from_ids(self.find_paper_ids_by_year(year))

    def find_papers_by_venue(self, venue: str) -> list[Paper]:
        return self._papers_from_ids(self.find_paper_ids_by_venue(venue))

    def find_papers_by_title_keywords(self, query: str) -> list[Paper]:
        return self._papers_from_ids(self.find_paper_ids_by_title_keywords(query))

    def find_papers_by_keywords(self, query: str) -> list[Paper]:
        return self.find_papers_by_title_keywords(query)

    def find_papers_by_author_keywords(self, query: str) -> list[Paper]:
        return self._papers_from_ids(self.find_paper_ids_by_author_keywords(query))

    def rebuild_search_indexes(self) -> dict[str, int]:
        return self._disk_index.rebuild_from_repository(self._repository)

    def compact_search_indexes(self) -> dict[str, int]:
        return self._disk_index.compact(self._repository)

    def _open_layers(self) -> None:
        self._repository = Repository(self.lib_path, auto_flush=False)
        self._disk_index = DiskIndex(self._repository.search_index_dir, auto_flush=False)

    def _repair_search_index_if_needed(self) -> None:
        has_records = self._repository.count() > 0
        if not has_records:
            return
        if self._disk_index.created_new_manifest or not self._disk_index.has_persisted_index():
            self.rebuild_search_indexes()
            return
        if self._repository.storage_revision != self._disk_index.source_revision:
            self.rebuild_search_indexes()

    def _reset_lib_path(self) -> None:
        for dirname in ("concrete_info", "meta_info", "search_index"):
            target = self.lib_path / dirname
            if target.exists():
                shutil.rmtree(target)
        self._open_layers()

    def _papers_from_ids(self, paper_ids: list[int]) -> list[Paper]:
        return [
            paper
            for paper_id in paper_ids
            if (paper := self._repository.get(paper_id)) is not None
        ]

    def _update_logical_revision(self) -> None:
        self._logical_revision += 1
