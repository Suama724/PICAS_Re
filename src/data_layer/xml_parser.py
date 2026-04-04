"""
这里用来进行xml文件的解析
用了sax来流式读取防止一次读整个文件爆了
基于 lxml.etree.iterparse
"""

from typing import Iterable, Generator
from io import BytesIO
from pathlib import Path

from lxml import etree
from .structures import Paper
from .config import VALID_RECORD_TAGS

def _get_text(elem: etree._Element | None) -> str | None:
    if elem is None:
        return None
    # itertext() 能拼接 <title>eafasef <sub>asfe</sub> Bewaf</title> 这类混合内容
    return "".join(elem.itertext()).strip() or None

def _extract_paper(elem: etree._Element) -> Paper:
    """
    从一个记录元素中提取内容
    构造 Paper 
    """
    key = elem.get("key")

    authors: list[str] = []
    title: str | None = None
    year: int | None = None
    venue: str | None = None
    journal: str | None = None
    booktitle: str | None = None
    volume: str | None = None
    doi: str | None = None
    external_ids: dict[str, str] = {}

    # 逐个读内容，貌似缩写也没太大意义
    for child in elem:
        tag = child.tag
        if tag == "author":
            text = _get_text(child)
            if text:
                authors.append(text)
        elif tag == "title":
            title = _get_text(child)
        elif tag == "year":
            raw = _get_text(child)
            if raw is not None:
                try:
                    year = int(raw)
                except ValueError:
                    pass
        elif tag == "journal":
            journal = _get_text(child)
        elif tag == "booktitle":
            booktitle = _get_text(child)
        elif tag == "volume":
            volume = _get_text(child)
        elif tag == "pages":
            external_ids["pages"] = _get_text(child) or ""
        elif tag in {"month", "mdate", "cdrom", "publtype", "crossref", "publisher", "isbn"}:
            text = _get_text(child)
            if text:
                external_ids[tag] = text
        elif tag == "doi":
            doi = _get_text(child)

    venue = journal or booktitle

    return Paper(
        paper_id=None,
        source="dblp",
        title=title or "",
        authors=authors,
        year=year,
        venue=venue,
        source_id=key,
        volume=volume,
        pages=external_ids.pop("pages", None),
        doi=doi,
        source_extra=external_ids,
    )   

def _iter_papers(source: str | bytes | Path) -> Generator[Paper, None, None]:
    """
    流式解析dblp.xml，逐个产出Paper
    通过监听 end 事件
    遇到有效记录标签（article、inproceedings 等）时提取字段
    构造 Paper 对象并 yield
    立即清理已处理的 XML 节点（防爆内存用）

    注意这里返回的是个generator

    这里context要加tag，不然如author这些每个都会单独溢出来
    """
    context = etree.iterparse(
        source,
        events=("end",),
        tag=tuple(VALID_RECORD_TAGS),
        dtd_validation=False,
        load_dtd=False,
        recover=True,
        huge_tree=True,
    )

    for _event, elem in context:
        yield _extract_paper(elem)
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]


def parse_dblp_xml(xml_path: str | Path) -> Iterable[Paper]:
    return _iter_papers(str(xml_path))


def parse_dblp_xml_bytes(xml_bytes: bytes) -> Iterable[Paper]:
    return _iter_papers(BytesIO(xml_bytes))