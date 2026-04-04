"""
这个用来切分原来的xml
原始xml解析一次一小时，总数量8m+
依据条目数量切
"""

from pathlib import Path
import time

from lxml import etree

from src.data_layer.xml_parser import parse_dblp_xml
from src.data_layer.config import DEFAULT_SLICE_SIZES, RAW_DBLP_XML_PATH, SPLIT_DATA_DIR, ensure_datas_dir_layout

def _paper_to_xml_bytes(paper) -> bytes:
    record = etree.Element("article")
    if paper.source_id:
        record.set("key", paper.source_id)

    for author in paper.authors:
        author_elem = etree.SubElement(record, "author")
        author_elem.text = author

    title_elem = etree.SubElement(record, "title")
    title_elem.text = paper.title

    if paper.venue:
        venue_tag = "journal" if paper.source == "dblp" else "booktitle"
        venue_elem = etree.SubElement(record, venue_tag)
        venue_elem.text = paper.venue

    if paper.year is not None:
        year_elem = etree.SubElement(record, "year")
        year_elem.text = str(paper.year)

    if paper.volume:
        volume_elem = etree.SubElement(record, "volume")
        volume_elem.text = paper.volume

    if paper.pages:
        pages_elem = etree.SubElement(record, "pages")
        pages_elem.text = paper.pages

    if paper.doi:
        doi_elem = etree.SubElement(record, "doi")
        doi_elem.text = paper.doi

    return etree.tostring(record, encoding="utf-8")


def create_dblp_slice(limit: int, output_path: str | Path) -> Path:
    ensure_datas_dir_layout()
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    start = time.perf_counter()
    count = 0

    with output.open("w", encoding="utf-8") as fh:
        fh.write("<dblp>\n")
        for paper in parse_dblp_xml(RAW_DBLP_XML_PATH):
            fh.write(_paper_to_xml_bytes(paper).decode("utf-8"))
            fh.write("\n")
            count += 1

            if count >= limit:
                break
        fh.write("</dblp>\n")

    elapsed = time.perf_counter() - start
    print(f"[Slice] output={output} records={count} elapsed={elapsed:.2f}s")
    return output


def default_slice_path(limit: int) -> Path:
    if limit == 10_000:
        suffix = "10k"
    elif limit == 100_000:
        suffix = "100k"
    elif limit == 1_000_000:
        suffix = "1m"
    else:
        suffix = str(limit)
    return SPLIT_DATA_DIR / f"dblp_{suffix}.xml"


def generate_default_slices() -> list[Path]:
    outputs: list[Path] = []
    for limit in DEFAULT_SLICE_SIZES:
        outputs.append(create_dblp_slice(limit, default_slice_path(limit)))
    return outputs

if __name__ == "__main__":
    generate_default_slices()