
"""
isk_index 负责本地上的索引查
核心结构就是倒排


目前的四类方式：
1. 精准作者检索
2. 精准标题检索
3. 标题关键词检索
4. 作者关键词检索
以及year venue的索引
模糊搜索摸了，什么时候有空再补


框架上分了两层：
IndexBuilder: 内存中的倒排构建器，负责把 Paper 展开成多个倒排表
DiskIndex: 磁盘索引管理器，负责 base / delta / pending 三层的落盘与查询
附这里的思路是原始加附加，考虑预设情境下附加应该不会太大，选择维护一个效率低一些的delta系列
到时候直接合并


盘内布局采用 category-bucket-postings 三段：
category 目录按索引类型划分
bucket 文件保存 key -> (posting_offset, posting_count)
postings.bin 保存真正的 posting 列表


ps：标题关键词与作者关键词在流程上是对称的，但posting 的东西不同
title关键词 posting 保存 paper_id
author关键词 posting 保存 (paper_id, author_position)

是因为后者的目的是固定语义为：
"多 token 必须由同一个作者名共同满足"
因此像一篇论文作者列表是 ["Grace Hopper", "Alan Turing"] 时，
查询 "grace turing" 不会查到
（虽然感觉这个功能有点没必要）
"""

from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, field
import hashlib
import json
from pathlib import Path
import shutil
import struct
from typing import BinaryIO, TypeVar
from __future__ import annotations

from .config import (
    BACKUP_DIR_SUFFIX,
    BASE_DIRNAME,
    BUILDING_DIR_SUFFIX,
    BUCKET_COUNT,
    CATEGORY_NAMES,
    DEFAULT_DELTA_FLUSH_EVERY,
    DELTA_DIRNAME,
    EXACT_CATEGORY_NAMES,
    FORMAT_VERSION,
    SEARCH_INDEX_MANIFEST_FILENAME,
)
from .structures import Paper
from src.tools.text_procession import (
    normalize_text,
    tokenize_author_keywords,
    tokenize_title_keywords,
)
T = TypeVar("T")

KEY_LEN_STRUCT = struct.Struct(">I")
ENTRY_HEADER_STRUCT = struct.Struct(">QI")
PAPER_POSTING_ITEM_STRUCT = struct.Struct(">Q")
AUTHOR_POSTING_ITEM_STRUCT = struct.Struct(">QI")

PAPER_ID_CATEGORIES = ("author", "title", "year", "venue", "title_keyword")
AUTHOR_REF_CATEGORIES = ("author_keyword",)


def normalize_category_value(category_name: str, value: str | int | None) -> str:
    _validate_exact_category_name(category_name)
    if value is None:
        return ""
    if category_name == "year":
        return str(value).strip()
    return normalize_text(str(value))


def _validate_exact_category_name(category_name: str) -> None:
    if category_name not in EXACT_CATEGORY_NAMES:
        raise ValueError(f"Unsupported exact category_name: {category_name}")


def _validate_index_category_name(category_name: str) -> None:
    if category_name not in CATEGORY_NAMES:
        raise ValueError(f"Unsupported index category_name: {category_name}")


def _stable_bucket_id(key: str) -> int:
    """
    这里是hash截取的思路，主要是保证落在同一个稳定的bucket里就行
    同一个 key 总是落到同一个稳定 bucket
    """
    digest = hashlib.blake2b(key.encode("utf-8"), digest_size=2).digest()
    return int.from_bytes(digest, "big") % BUCKET_COUNT


def _bucket_filename(bucket_id: int) -> str:
    return f"bucket_{bucket_id:03d}.entries.bin"


def _shadow_dir(target_dir: Path, suffix: str) -> Path:
    return target_dir.parent / f"{target_dir.name}{suffix}"


def _category_dir(index_dir: Path, category_name: str) -> Path:
    return index_dir / category_name


def _postings_path(index_dir: Path, category_name: str) -> Path:
    return _category_dir(index_dir, category_name) / "postings.bin"


def _dedup_sorted(values: Iterable[T]) -> list[T]:
    return sorted(set(values))


def _intersect_groups(groups: Iterable[Iterable[T]]) -> list[T]:
    matched_groups = [set(group) for group in groups]
    if not matched_groups:
        return []

    matched = matched_groups[0]
    for group in matched_groups[1:]:
        matched &= group
        if not matched:
            return []
    return sorted(matched)


def _write_paper_ids(postings_file: BinaryIO, paper_ids: list[int]) -> None:
    for paper_id in paper_ids:
        postings_file.write(PAPER_POSTING_ITEM_STRUCT.pack(paper_id))


def _write_author_refs(postings_file: BinaryIO, author_refs: list[tuple[int, int]]) -> None:
    for paper_id, author_position in author_refs:
        postings_file.write(AUTHOR_POSTING_ITEM_STRUCT.pack(paper_id, author_position))


def _read_binary_chunk(
    postings_path: Path,
    posting_offset: int,
    posting_count: int,
    item_size: int,
) -> bytes:
    if posting_count == 0:
        return b""

    with postings_path.open("rb") as postings_file:
        postings_file.seek(posting_offset)
        chunk_size = item_size * posting_count
        chunk = postings_file.read(chunk_size)
        if len(chunk) != chunk_size:
            raise ValueError(f"Corrupted postings file: {postings_path}")
        return chunk


@dataclass(slots=True)
class IndexedPaperTerms:
    """Paper 被展开后参与索引的标准化中间形态"""

    normalized_authors: list[str]
    normalized_title: str
    normalized_year: str
    normalized_venue: str
    title_keywords: list[str]
    author_keywords_by_author: list[list[str]]


def _build_terms_for_paper(paper: Paper) -> IndexedPaperTerms:
    """把一篇 Paper 一次性规范化，供 add/remove/rebuild 共用。"""
    normalized_authors = list(
        dict.fromkeys(
            normalized_author
            for normalized_author in (normalize_text(author) for author in paper.authors)
            if normalized_author
        )
    )
    normalized_title = normalize_text(paper.title)
    normalized_year = "" if paper.year is None else str(paper.year).strip()
    normalized_venue = "" if not paper.venue else normalize_text(paper.venue)
    title_keywords = tokenize_title_keywords(paper.title)
    author_keywords_by_author = [
        tokenize_author_keywords(author)
        for author in paper.authors
        if normalize_text(author)
    ]
    return IndexedPaperTerms(
        normalized_authors=normalized_authors,
        normalized_title=normalized_title,
        normalized_year=normalized_year,
        normalized_venue=normalized_venue,
        title_keywords=title_keywords,
        author_keywords_by_author=author_keywords_by_author,
    )


def _remove_from_mapping(mapping: dict[str, list[T]], key: str, value: T) -> None:
    current_values = mapping.get(key)
    if not current_values:
        return
    filtered = [current_value for current_value in current_values if current_value != value]
    if filtered:
        mapping[key] = filtered
    else:
        del mapping[key]


class IndexBuilder:
    """
    内存态倒排构建器。

    它只负责维护“key -> postings”的映射，不关心 base/delta的区分
    DiskIndex 会用它做 pending 段，也会在 rebuild 时用它重建 base
    """

    def __init__(self) -> None:
        self.author_index: dict[str, list[int]] = defaultdict(list)
        self.title_index: dict[str, list[int]] = defaultdict(list)
        self.year_index: dict[str, list[int]] = defaultdict(list)
        self.venue_index: dict[str, list[int]] = defaultdict(list)
        self.title_keyword_index: dict[str, list[int]] = defaultdict(list)
        self.author_keyword_index: dict[str, list[tuple[int, int]]] = defaultdict(list)
        self.paper_terms: dict[int, IndexedPaperTerms] = {}

    def add_paper(self, paper: Paper, paper_id: int | None = None) -> None:
        """
        把一篇 paper 展开到所有倒排表

        author_keyword_index 的 posting 单元是 (paper_id, author_position)，
        所以作者关键词查询会先在作者维度做交集，再投影回 paper_id
        """
        resolved_paper_id = paper.paper_id if paper_id is None else paper_id
        if resolved_paper_id is None or resolved_paper_id < 1:
            raise ValueError(f"Invalid paper_id for indexing: {resolved_paper_id}")

        if resolved_paper_id in self.paper_terms:
            self.remove_paper(resolved_paper_id)

        terms = _build_terms_for_paper(paper)
        self.paper_terms[resolved_paper_id] = terms

        for normalized_author in terms.normalized_authors:
            self.author_index[normalized_author].append(resolved_paper_id)
        if terms.normalized_title:
            self.title_index[terms.normalized_title].append(resolved_paper_id)
        if terms.normalized_year:
            self.year_index[terms.normalized_year].append(resolved_paper_id)
        if terms.normalized_venue:
            self.venue_index[terms.normalized_venue].append(resolved_paper_id)
        for keyword in terms.title_keywords:
            self.title_keyword_index[keyword].append(resolved_paper_id)
        for author_position, author_keywords in enumerate(terms.author_keywords_by_author):
            author_ref = (resolved_paper_id, author_position)
            for keyword in author_keywords:
                self.author_keyword_index[keyword].append(author_ref)

    def remove_paper(self, paper_id: int) -> None:
        terms = self.paper_terms.pop(paper_id, None)
        if terms is None:
            return

        for normalized_author in terms.normalized_authors:
            _remove_from_mapping(self.author_index, normalized_author, paper_id)
        if terms.normalized_title:
            _remove_from_mapping(self.title_index, terms.normalized_title, paper_id)
        if terms.normalized_year:
            _remove_from_mapping(self.year_index, terms.normalized_year, paper_id)
        if terms.normalized_venue:
            _remove_from_mapping(self.venue_index, terms.normalized_venue, paper_id)
        for keyword in terms.title_keywords:
            _remove_from_mapping(self.title_keyword_index, keyword, paper_id)
        for author_position, author_keywords in enumerate(terms.author_keywords_by_author):
            author_ref = (paper_id, author_position)
            for keyword in author_keywords:
                _remove_from_mapping(self.author_keyword_index, keyword, author_ref)

    def contains_paper(self, paper_id: int) -> bool:
        return paper_id in self.paper_terms

    def paper_count(self) -> int:
        return len(self.paper_terms)

    def find_paper_ids(self, category_name: str, value: str | int | None) -> list[int]:
        normalized_value = normalize_category_value(category_name, value)
        if not normalized_value:
            return []

        exact_indexes = {
            "author": self.author_index,
            "title": self.title_index,
            "year": self.year_index,
            "venue": self.venue_index,
        }
        if category_name not in exact_indexes:
            raise ValueError(f"Unsupported exact category_name: {category_name}")
        return list(exact_indexes[category_name].get(normalized_value, []))

    def find_paper_ids_by_title_keywords(self, query: str) -> list[int]:
        keyword_tokens = tokenize_title_keywords(query)
        if not keyword_tokens:
            return []
        return _intersect_groups(
            self.title_keyword_index.get(keyword, [])
            for keyword in keyword_tokens
        )

    def find_paper_ids_by_author_keywords(self, query: str) -> list[int]:
        keyword_tokens = tokenize_author_keywords(query)
        if not keyword_tokens:
            return []
        matched_refs = _intersect_groups(
            self.author_keyword_index.get(keyword, [])
            for keyword in keyword_tokens
        )
        return sorted({paper_id for paper_id, _author_position in matched_refs})

    def clear(self) -> None:
        self.author_index.clear()
        self.title_index.clear()
        self.year_index.clear()
        self.venue_index.clear()
        self.title_keyword_index.clear()
        self.author_keyword_index.clear()
        self.paper_terms.clear()

    def build_to_disk(self, index_dir: str | Path) -> Path:
        """将当前内存倒排表完整落盘成一个独立索引目录。"""
        index_dir = Path(index_dir)
        if index_dir.exists():
            shutil.rmtree(index_dir)
        index_dir.mkdir(parents=True, exist_ok=True)

        self._write_category(index_dir, "author", self.author_index, _write_paper_ids)
        self._write_category(index_dir, "title", self.title_index, _write_paper_ids)
        self._write_category(index_dir, "year", self.year_index, _write_paper_ids)
        self._write_category(index_dir, "venue", self.venue_index, _write_paper_ids)
        self._write_category(index_dir, "title_keyword", self.title_keyword_index, _write_paper_ids)
        self._write_category(index_dir, "author_keyword", self.author_keyword_index, _write_author_refs)
        return index_dir

    def _write_category(
        self,
        index_dir: Path,
        category_name: str,
        mapping: dict[str, list[T]],
        posting_writer: Callable[[BinaryIO, list[T]], None],
    ) -> None:
        category_dir = _category_dir(index_dir, category_name)
        category_dir.mkdir(parents=True, exist_ok=True)
        entries_by_bucket: dict[int, list[tuple[str, int, int]]] = defaultdict(list)
        postings_path = _postings_path(index_dir, category_name)

        with postings_path.open("wb") as postings_file:
            for key in sorted(mapping):
                postings = mapping[key]
                posting_offset = postings_file.tell()
                posting_writer(postings_file, postings)
                entries_by_bucket[_stable_bucket_id(key)].append((key, posting_offset, len(postings)))

        for bucket_id in range(BUCKET_COUNT):
            bucket_path = category_dir / _bucket_filename(bucket_id)
            with bucket_path.open("wb") as bucket_file:
                for key, posting_offset, posting_count in sorted(entries_by_bucket.get(bucket_id, [])):
                    key_bytes = key.encode("utf-8")
                    bucket_file.write(KEY_LEN_STRUCT.pack(len(key_bytes)))
                    bucket_file.write(key_bytes)
                    bucket_file.write(ENTRY_HEADER_STRUCT.pack(posting_offset, posting_count))


def build_index_stats(builder: IndexBuilder, total_records: int) -> dict[str, int]:
    return {
        "format_version": FORMAT_VERSION,
        "total_records_seen": total_records,
        "indexed_authors_keys": len(builder.author_index),
        "indexed_titles_keys": len(builder.title_index),
        "indexed_years_keys": len(builder.year_index),
        "indexed_venues_keys": len(builder.venue_index),
        "indexed_keywords_keys": len(builder.title_keyword_index),
        "indexed_title_keywords_keys": len(builder.title_keyword_index),
        "indexed_author_keywords_keys": len(builder.author_keyword_index),
    }


def _read_paper_id_posting(index_dir: Path, category_name: str, posting_offset: int, posting_count: int) -> list[int]:
    chunk = _read_binary_chunk(
        _postings_path(index_dir, category_name),
        posting_offset,
        posting_count,
        PAPER_POSTING_ITEM_STRUCT.size,
    )
    if not chunk:
        return []
    return list(struct.unpack(f">{posting_count}Q", chunk))


def _read_author_ref_posting(
    index_dir: Path,
    category_name: str,
    posting_offset: int,
    posting_count: int,
) -> list[tuple[int, int]]:
    chunk = _read_binary_chunk(
        _postings_path(index_dir, category_name),
        posting_offset,
        posting_count,
        AUTHOR_POSTING_ITEM_STRUCT.size,
    )
    if not chunk:
        return []

    author_refs: list[tuple[int, int]] = []
    for offset in range(0, len(chunk), AUTHOR_POSTING_ITEM_STRUCT.size):
        author_refs.append(AUTHOR_POSTING_ITEM_STRUCT.unpack_from(chunk, offset))
    return author_refs


def _scan_bucket_for_key(index_dir: Path, category_name: str, normalized_key: str) -> tuple[int, int] | None:
    _validate_index_category_name(category_name)
    category_dir = _category_dir(index_dir, category_name)
    if not category_dir.exists():
        return None

    bucket_path = category_dir / _bucket_filename(_stable_bucket_id(normalized_key))
    if not bucket_path.exists():
        return None

    with bucket_path.open("rb") as bucket_file:
        while key_len_chunk := bucket_file.read(KEY_LEN_STRUCT.size):
            if len(key_len_chunk) != KEY_LEN_STRUCT.size:
                raise ValueError(f"Corrupted bucket file: {bucket_path}")

            key_len = KEY_LEN_STRUCT.unpack(key_len_chunk)[0]
            key_bytes = bucket_file.read(key_len)
            entry_chunk = bucket_file.read(ENTRY_HEADER_STRUCT.size)
            if len(key_bytes) != key_len or len(entry_chunk) != ENTRY_HEADER_STRUCT.size:
                raise ValueError(f"Corrupted bucket file: {bucket_path}")

            if key_bytes.decode("utf-8") == normalized_key:
                return ENTRY_HEADER_STRUCT.unpack(entry_chunk)
    return None


def _find_paper_ids_in_single_index(index_dir: str | Path, category_name: str, normalized_key: str) -> list[int]:
    if not normalized_key:
        return []
    index_dir = Path(index_dir)
    posting_header = _scan_bucket_for_key(index_dir, category_name, normalized_key)
    if posting_header is None:
        return []
    posting_offset, posting_count = posting_header
    return _read_paper_id_posting(index_dir, category_name, posting_offset, posting_count)


def _find_author_refs_in_single_index(index_dir: str | Path, normalized_key: str) -> list[tuple[int, int]]:
    if not normalized_key:
        return []
    index_dir = Path(index_dir)
    posting_header = _scan_bucket_for_key(index_dir, "author_keyword", normalized_key)
    if posting_header is None:
        return []
    posting_offset, posting_count = posting_header
    return _read_author_ref_posting(index_dir, "author_keyword", posting_offset, posting_count)


@dataclass(slots=True)
class SearchIndexManifest:
    """描述正式磁盘索引的 base / delta / delete 视图。"""

    format_version: int = FORMAT_VERSION
    base_dir: str = BASE_DIRNAME
    active_delta_segments: list[str] = field(default_factory=list)
    next_delta_id: int = 1
    delta_flush_every: int = DEFAULT_DELTA_FLUSH_EVERY
    deleted_paper_ids: list[int] = field(default_factory=list)
    source_revision: int = 0

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_suffix(f"{path.suffix}.tmp")
        temp_path.write_text(json.dumps(asdict(self), ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(path)

    @classmethod
    def load(cls, path: Path) -> SearchIndexManifest | None:
        if not path.exists():
            return None

        payload = json.loads(path.read_text(encoding="utf-8"))
        active_delta_segments = payload.get("active_delta_segments", [])
        deleted_paper_ids = payload.get("deleted_paper_ids", [])
        if not isinstance(active_delta_segments, list) or any(not isinstance(name, str) for name in active_delta_segments):
            raise ValueError(f"Invalid delta segment list: {path}")
        if not isinstance(deleted_paper_ids, list) or any(not isinstance(paper_id, int) or paper_id < 1 for paper_id in deleted_paper_ids):
            raise ValueError(f"Invalid deleted paper id list: {path}")

        manifest = cls(
            format_version=int(payload.get("format_version", FORMAT_VERSION)),
            base_dir=str(payload.get("base_dir", BASE_DIRNAME)),
            active_delta_segments=list(active_delta_segments),
            next_delta_id=int(payload.get("next_delta_id", 1)),
            delta_flush_every=int(payload.get("delta_flush_every", DEFAULT_DELTA_FLUSH_EVERY)),
            deleted_paper_ids=sorted(set(deleted_paper_ids)),
            source_revision=int(payload.get("source_revision", 0)),
        )
        if manifest.format_version != FORMAT_VERSION:
            raise ValueError(f"Unsupported search index format_version: {manifest.format_version}")
        if not manifest.base_dir:
            raise ValueError("Search index manifest base_dir is empty")
        if manifest.next_delta_id < 1 or manifest.delta_flush_every < 1 or manifest.source_revision < 0:
            raise ValueError(f"Invalid search index manifest: {path}")
        return manifest


@dataclass(slots=True)
class ManagedIndexView:
    manifest: SearchIndexManifest
    base_dir: Path
    delta_dirs: list[Path]
    deleted_paper_ids: set[int]


def _load_managed_index(index_dir: str | Path) -> ManagedIndexView | None:
    """如果目录是 DiskIndex 管理的正式索引，返回 base/delta 视图。"""
    index_dir = Path(index_dir)
    manifest = SearchIndexManifest.load(index_dir / SEARCH_INDEX_MANIFEST_FILENAME)
    if manifest is None:
        return None

    delta_root = index_dir / DELTA_DIRNAME
    delta_dirs = [delta_root / segment_name for segment_name in manifest.active_delta_segments]
    return ManagedIndexView(
        manifest=manifest,
        base_dir=index_dir / manifest.base_dir,
        delta_dirs=delta_dirs,
        deleted_paper_ids=set(manifest.deleted_paper_ids),
    )


def _apply_deleted_ids(paper_ids: Iterable[int], deleted_paper_ids: set[int]) -> list[int]:
    if not deleted_paper_ids:
        return _dedup_sorted(paper_ids)
    return sorted({paper_id for paper_id in paper_ids if paper_id not in deleted_paper_ids})


def _collect_paper_ids_from_index(
    index_dir: str | Path,
    single_index_lookup: Callable[[Path], Iterable[int]],
) -> list[int]:
    managed = _load_managed_index(index_dir)
    if managed is None:
        return _dedup_sorted(single_index_lookup(Path(index_dir)))

    paper_ids = list(single_index_lookup(managed.base_dir))
    for delta_dir in managed.delta_dirs:
        paper_ids.extend(single_index_lookup(delta_dir))
    return _apply_deleted_ids(paper_ids, managed.deleted_paper_ids)


def _collect_paper_ids_across_indexes(
    index_dirs: Iterable[str | Path],
    single_index_lookup: Callable[[Path], Iterable[int]],
) -> list[int]:
    paper_ids: list[int] = []
    for index_dir in index_dirs:
        paper_ids.extend(single_index_lookup(Path(index_dir)))
    return _dedup_sorted(paper_ids)


def find_paper_ids_by_category(index_dir: str | Path, category_name: str, value: str | int | None) -> list[int]:
    normalized_value = normalize_category_value(category_name, value)
    if not normalized_value:
        return []
    return _collect_paper_ids_from_index(
        index_dir,
        lambda single_index_dir: _find_paper_ids_in_single_index(single_index_dir, category_name, normalized_value),
    )


def find_paper_ids_by_category_across_indexes(
    index_dirs: Iterable[str | Path],
    category_name: str,
    value: str | int | None,
) -> list[int]:
    normalized_value = normalize_category_value(category_name, value)
    if not normalized_value:
        return []
    return _collect_paper_ids_across_indexes(
        index_dirs,
        lambda single_index_dir: _find_paper_ids_in_single_index(single_index_dir, category_name, normalized_value),
    )


def find_paper_ids_by_category_in_builder(
    builder: IndexBuilder,
    category_name: str,
    value: str | int | None,
) -> list[int]:
    return builder.find_paper_ids(category_name, value)


def find_paper_ids_by_title_keywords(index_dir: str | Path, query: str) -> list[int]:
    """标题关键词查询：多 token 做和，posting 直接按 paper_id 求交集"""
    keyword_tokens = tokenize_title_keywords(query)
    if not keyword_tokens:
        return []
    return _collect_paper_ids_from_index(
        index_dir,
        lambda single_index_dir: _intersect_groups(
            _find_paper_ids_in_single_index(single_index_dir, "title_keyword", keyword)
            for keyword in keyword_tokens
        ),
    )


def find_paper_ids_by_title_keywords_across_indexes(
    index_dirs: Iterable[str | Path],
    query: str,
) -> list[int]:
    keyword_tokens = tokenize_title_keywords(query)
    if not keyword_tokens:
        return []
    return _collect_paper_ids_across_indexes(
        index_dirs,
        lambda single_index_dir: _intersect_groups(
            _find_paper_ids_in_single_index(single_index_dir, "title_keyword", keyword)
            for keyword in keyword_tokens
        ),
    )


def find_paper_ids_by_title_keywords_in_builder(builder: IndexBuilder, query: str) -> list[int]:
    return builder.find_paper_ids_by_title_keywords(query)


def find_paper_ids_by_keywords(index_dir: str | Path, query: str) -> list[int]:
    return find_paper_ids_by_title_keywords(index_dir, query)


def find_paper_ids_by_keywords_across_indexes(index_dirs: Iterable[str | Path], query: str) -> list[int]:
    return find_paper_ids_by_title_keywords_across_indexes(index_dirs, query)


def find_paper_ids_by_keywords_in_builder(builder: IndexBuilder, query: str) -> list[int]:
    return find_paper_ids_by_title_keywords_in_builder(builder, query)


def find_paper_ids_by_author_keywords(index_dir: str | Path, query: str) -> list[int]:
    """
    作者关键词查询：多 token AND，且必须由同一个作者名共同满足。
    实现上先对 (paper_id, author_position) 求交集，再投影回 paper_id。
    """
    keyword_tokens = tokenize_author_keywords(query)
    if not keyword_tokens:
        return []
    return _collect_paper_ids_from_index(
        index_dir,
        lambda single_index_dir: sorted(
            {
                paper_id
                for paper_id, _author_position in _intersect_groups(
                    _find_author_refs_in_single_index(single_index_dir, keyword)
                    for keyword in keyword_tokens
                )
            }
        ),
    )


def find_paper_ids_by_author_keywords_across_indexes(
    index_dirs: Iterable[str | Path],
    query: str,
) -> list[int]:
    keyword_tokens = tokenize_author_keywords(query)
    if not keyword_tokens:
        return []
    return _collect_paper_ids_across_indexes(
        index_dirs,
        lambda single_index_dir: sorted(
            {
                paper_id
                for paper_id, _author_position in _intersect_groups(
                    _find_author_refs_in_single_index(single_index_dir, keyword)
                    for keyword in keyword_tokens
                )
            }
        ),
    )


def find_paper_ids_by_author_keywords_in_builder(builder: IndexBuilder, query: str) -> list[int]:
    return builder.find_paper_ids_by_author_keywords(query)


def find_paper_ids_by_author(index_dir: str | Path, author_name: str) -> list[int]:
    return find_paper_ids_by_category(index_dir, "author", author_name)


def find_paper_ids_by_title(index_dir: str | Path, title: str) -> list[int]:
    return find_paper_ids_by_category(index_dir, "title", title)


def find_paper_ids_by_year(index_dir: str | Path, year: int) -> list[int]:
    return find_paper_ids_by_category(index_dir, "year", year)


def find_paper_ids_by_venue(index_dir: str | Path, venue: str) -> list[int]:
    return find_paper_ids_by_category(index_dir, "venue", venue)


def find_paper_ids_by_keyword(index_dir: str | Path, keyword: str) -> list[int]:
    keyword_tokens = tokenize_title_keywords(keyword)
    if len(keyword_tokens) != 1:
        return []
    return _collect_paper_ids_from_index(
        index_dir,
        lambda single_index_dir: _find_paper_ids_in_single_index(single_index_dir, "title_keyword", keyword_tokens[0]),
    )


class DiskIndex:
    """
    Repository 使用的磁盘索引接入点
    """

    def __init__(
        self,
        index_root: str | Path,
        auto_flush: bool = True,
        delta_flush_every: int = DEFAULT_DELTA_FLUSH_EVERY,
    ) -> None:
        self.index_root = Path(index_root)
        self.index_root.mkdir(parents=True, exist_ok=True)
        self.auto_flush = auto_flush
        self.delta_root = self.index_root / DELTA_DIRNAME
        self.manifest_path = self.index_root / SEARCH_INDEX_MANIFEST_FILENAME
        self._pending_builder = IndexBuilder()
        self._pending_deleted_ids: set[int] = set()
        self.created_new_manifest = False
        self._manifest = self._load_or_create_manifest(delta_flush_every)

    def has_persisted_index(self) -> bool:
        return self.manifest_path.exists() and self._base_index_dir().is_dir()

    @property
    def source_revision(self) -> int:
        return self._manifest.source_revision

    def add_paper(self, paper: Paper) -> None:
        if paper.paper_id is None or paper.paper_id < 1:
            raise ValueError(f"Invalid paper_id for indexing: {paper.paper_id}")
        self._pending_deleted_ids.discard(paper.paper_id)
        self._pending_builder.add_paper(paper, paper.paper_id)
        if self.auto_flush or self._pending_builder.paper_count() >= self._manifest.delta_flush_every:
            self.flush()

    def delete_paper(self, paper_id: int) -> None:
        if paper_id < 1:
            raise ValueError(f"Invalid paper_id: {paper_id}")
        if self._pending_builder.contains_paper(paper_id):
            self._pending_builder.remove_paper(paper_id)
        else:
            self._pending_deleted_ids.add(paper_id)
        if self.auto_flush:
            self.flush()

    def flush(self, source_revision: int | None = None) -> None:
        """
        将 pending builder 落成新的 delta 段，并把逻辑删除写入 manifest
        当前实现不会原地修改旧段，delete 通过 deleted_paper_ids 过滤，
        等 rebuild/compact 时再重新生成干净 base
        """
        self.delta_root.mkdir(parents=True, exist_ok=True)

        if self._pending_builder.paper_count() > 0:
            segment_name = f"segment_{self._manifest.next_delta_id:06d}"
            segment_dir = self.delta_root / segment_name
            self._write_index_directory(self._pending_builder, segment_dir)
            self._manifest.active_delta_segments.append(segment_name)
            self._manifest.next_delta_id += 1
            self._pending_builder.clear()

        if self._pending_deleted_ids:
            deleted_paper_ids = set(self._manifest.deleted_paper_ids)
            deleted_paper_ids.update(self._pending_deleted_ids)
            self._manifest.deleted_paper_ids = sorted(deleted_paper_ids)
            self._pending_deleted_ids.clear()

        if source_revision is not None:
            self._manifest.source_revision = source_revision

        self._manifest.save(self.manifest_path)

    def find_paper_ids_by_author(self, author_name: str) -> list[int]:
        return self._collect_exact_match_ids("author", author_name)

    def find_paper_ids_by_title(self, title: str) -> list[int]:
        return self._collect_exact_match_ids("title", title)

    def find_paper_ids_by_year(self, year: int) -> list[int]:
        return self._collect_exact_match_ids("year", year)

    def find_paper_ids_by_venue(self, venue: str) -> list[int]:
        return self._collect_exact_match_ids("venue", venue)

    def find_paper_ids_by_title_keywords(self, query: str) -> list[int]:
        paper_ids = find_paper_ids_by_title_keywords(self.index_root, query)
        paper_ids.extend(self._pending_builder.find_paper_ids_by_title_keywords(query))
        return self._apply_pending_deletes(paper_ids)

    def find_paper_ids_by_keywords(self, query: str) -> list[int]:
        return self.find_paper_ids_by_title_keywords(query)

    def find_paper_ids_by_keyword(self, keyword: str) -> list[int]:
        paper_ids = find_paper_ids_by_keyword(self.index_root, keyword)
        return self._apply_pending_deletes(paper_ids)

    def find_paper_ids_by_author_keywords(self, query: str) -> list[int]:
        paper_ids = find_paper_ids_by_author_keywords(self.index_root, query)
        paper_ids.extend(self._pending_builder.find_paper_ids_by_author_keywords(query))
        return self._apply_pending_deletes(paper_ids)

    def replace_with_builder(
        self,
        builder: IndexBuilder,
        total_records: int,
        source_revision: int = 0,
    ) -> dict[str, int]:
        delta_flush_every = self._manifest.delta_flush_every
        target_manifest = SearchIndexManifest(
            delta_flush_every=delta_flush_every,
            source_revision=source_revision,
        )
        self._write_index_directory(builder, self.index_root / target_manifest.base_dir)
        self._reset_delta_root()
        self._manifest = target_manifest
        self._manifest.save(self.manifest_path)
        self._pending_builder.clear()
        self._pending_deleted_ids.clear()
        self.created_new_manifest = False
        return build_index_stats(builder, total_records)

    def rebuild_from_repository(self, repo) -> dict[str, int]:
        builder = IndexBuilder()
        total_records = 0
        for paper_id in repo.iter_active_paper_ids():
            paper = repo.get(paper_id)
            if paper is None:
                continue
            builder.add_paper(paper, paper_id)
            total_records += 1
        return self.replace_with_builder(builder, total_records, source_revision=repo.storage_revision)

    def compact(self, repo) -> dict[str, int]:
        compacted_delta_segments = len(self._manifest.active_delta_segments)
        stats = self.rebuild_from_repository(repo)
        stats["compacted_delta_segments"] = compacted_delta_segments
        return stats

    def _load_or_create_manifest(self, delta_flush_every: int) -> SearchIndexManifest:
        manifest = SearchIndexManifest.load(self.manifest_path)
        if manifest is not None:
            self.delta_root.mkdir(parents=True, exist_ok=True)
            return manifest

        manifest = SearchIndexManifest(delta_flush_every=delta_flush_every)
        self.created_new_manifest = True
        self.delta_root.mkdir(parents=True, exist_ok=True)
        self._write_index_directory(IndexBuilder(), self.index_root / manifest.base_dir)
        manifest.save(self.manifest_path)
        return manifest

    def _base_index_dir(self) -> Path:
        return self.index_root / self._manifest.base_dir

    def _collect_exact_match_ids(self, category_name: str, value: str | int | None) -> list[int]:
        paper_ids = find_paper_ids_by_category(self.index_root, category_name, value)
        paper_ids.extend(self._pending_builder.find_paper_ids(category_name, value))
        return self._apply_pending_deletes(paper_ids)

    def _apply_pending_deletes(self, paper_ids: Iterable[int]) -> list[int]:
        if not self._pending_deleted_ids:
            return _dedup_sorted(paper_ids)
        return sorted({paper_id for paper_id in paper_ids if paper_id not in self._pending_deleted_ids})

    def _write_index_directory(self, builder: IndexBuilder, target_dir: Path) -> None:
        """用 building / backup shadow dir 做近似原子替换"""
        build_dir = _shadow_dir(target_dir, BUILDING_DIR_SUFFIX)
        backup_dir = _shadow_dir(target_dir, BACKUP_DIR_SUFFIX)

        if build_dir.exists():
            shutil.rmtree(build_dir)
        if backup_dir.exists():
            shutil.rmtree(backup_dir)

        builder.build_to_disk(build_dir)

        if target_dir.exists():
            try:
                target_dir.rename(backup_dir)
            except OSError:
                shutil.rmtree(target_dir)

        try:
            build_dir.rename(target_dir)
        except OSError:
            if target_dir.exists():
                shutil.rmtree(target_dir)
            shutil.move(str(build_dir), str(target_dir))

        if backup_dir.exists():
            shutil.rmtree(backup_dir)

    def _reset_delta_root(self) -> None:
        if self.delta_root.exists():
            shutil.rmtree(self.delta_root)
        self.delta_root.mkdir(parents=True, exist_ok=True)


def rebuild_indexes_from_storage(workspace: str | Path) -> dict[str, int]:
    from .engine import DatabaseEngine

    engine = DatabaseEngine(workspace)
    return engine.rebuild_search_indexes()
