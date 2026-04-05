from __future__ import annotations

from contextlib import contextmanager
import random
import shutil
import time
from pathlib import Path

from src.data_layer.config import RAW_DBLP_XML_PATH, SPLIT_DATA_DIR, TEST_DIR
from src.data_layer.structures import Paper
from src.data_layer.xml_parser import parse_dblp_xml
from src.tools.timer import timer

DEFAULT_SLICED_XML_PATH = SPLIT_DATA_DIR / "dblp_10k.xml"

SAMPLE_XML_SINGLE = b"""
<dblp>
  <article key="journals/test/parser-sample">
    <author>Alice Zhang</author>
    <author>Bob Li</author>
    <title>Graph <i>Search</i> Pipelines</title>
    <journal>Journal of Parser Tests</journal>
    <year>2026</year>
    <volume>7</volume>
    <pages>11-19</pages>
    <doi>10.1234/parser-sample</doi>
    <publisher>PICAS Press</publisher>
  </article>
</dblp>
"""

SAMPLE_XML_BATCH = b"""
<dblp>
  <article key="journals/test/paper-1">
    <author>Alice Zhang</author>
    <title>Storage First</title>
    <journal>Journal A</journal>
    <year>2024</year>
  </article>
  <article key="journals/test/paper-2">
    <author>Bob Li</author>
    <author>Carol Wu</author>
    <title>Storage Reopen</title>
    <journal>Journal B</journal>
    <year>2025</year>
  </article>
  <article key="journals/test/paper-3">
    <author>Dora Chen</author>
    <title>Segment Rollover</title>
    <journal>Journal C</journal>
    <year>2026</year>
  </article>
</dblp>
"""


def reset_test_workspace(name: str) -> Path:
    workspace = TEST_DIR / name
    if workspace.exists():
        shutil.rmtree(workspace)
    return workspace


def ensure_sliced_xml_path() -> Path:
    if not DEFAULT_SLICED_XML_PATH.is_file():
        raise FileNotFoundError(f"slice xml not found: {DEFAULT_SLICED_XML_PATH}")
    return DEFAULT_SLICED_XML_PATH


def ensure_raw_dblp_xml_path() -> Path:
    return RAW_DBLP_XML_PATH


def make_paper(
    title: str,
    authors: list[str],
    year: int | None,
    *,
    venue: str = "Test Venue",
    source: str = "dblp",
    source_id: str | None = None,
    doi: str | None = None,
) -> Paper:
    return Paper(
        paper_id=None,
        source=source,
        title=title,
        authors=authors,
        year=year,
        venue=venue,
        doi=doi,
        source_id=source_id or f"{source}:{year}:{title}",
    )


def collect_query_samples(xml_path: Path) -> dict[str, object]:
    author = None
    title = None
    venue = None
    year = None

    for paper in parse_dblp_xml(xml_path):
        if author is None and paper.authors:
            author = paper.authors[0]
        if title is None and paper.title:
            title = paper.title
        if venue is None and paper.venue:
            venue = paper.venue
        if year is None and paper.year is not None:
            year = paper.year
        if author and title and venue and year is not None:
            break

    if author is None or title is None or venue is None or year is None:
        raise AssertionError(f"failed to collect query samples from {xml_path}")

    return {
        "author": author,
        "title": title,
        "venue": venue,
        "year": year,
    }


def paper_to_dict(paper: Paper) -> dict[str, object]:
    return {
        "paper_id": paper.paper_id,
        "source_id": paper.source_id,
        "title": paper.title,
        "authors": list(paper.authors),
        "year": paper.year,
        "venue": paper.venue,
    }


def reservoir_sample_source_papers(
    xml_path: Path,
    sample_size: int,
    seed: int,
) -> tuple[int, list[dict[str, object]]]:
    rng = random.Random(seed)
    samples: list[dict[str, object]] = []
    total_records = 0

    for paper in parse_dblp_xml(xml_path):
        total_records += 1
        paper_dict = paper_to_dict(paper)
        paper_dict["paper_id"] = total_records
        if len(samples) < sample_size:
            samples.append(paper_dict)
            continue

        sample_index = rng.randint(1, total_records)
        if sample_index <= sample_size:
            samples[sample_index - 1] = paper_dict

    return total_records, samples


def print_test_title(test_code: str, title: str) -> None:
    print(f"============[{test_code} {title}]============")


def print_step(label: str) -> None:
    print(f"[STEP] {label}")


@contextmanager
def timed_step(label: str):
    print_step(label)
    with timer(label):
        yield


def assert_equal(label: str, expected, actual) -> None:
    print(f"{label}: expected={_format_value(expected)}, actual={_format_value(actual)}")
    assert actual == expected


def assert_true(label: str, actual: bool) -> None:
    print(f"{label}: expected=True, actual={_format_value(actual)}")
    assert actual is True


def assert_false(label: str, actual: bool) -> None:
    print(f"{label}: expected=False, actual={_format_value(actual)}")
    assert actual is False


def assert_not_none(label: str, actual) -> None:
    print(f"{label}: expected=not None, actual={_format_value(actual)}")
    assert actual is not None


def assert_in(label: str, member, container) -> None:
    print(
        f"{label}: expected member={_format_value(member)} "
        f"in actual={_format_value(container)}"
    )
    assert member in container


def assert_not_in(label: str, member, container) -> None:
    print(
        f"{label}: expected member={_format_value(member)} "
        f"not in actual={_format_value(container)}"
    )
    assert member not in container


@contextmanager
def perf_measurement(label: str):
    print_step(label)
    started_at = time.perf_counter()
    with timer(label):
        yield lambda: time.perf_counter() - started_at


def _format_value(value) -> str:
    if isinstance(value, str):
        return repr(value if len(value) <= 160 else f"{value[:157]}...")

    if isinstance(value, list):
        if len(value) <= 12:
            return repr(value)
        head = value[:5]
        tail = value[-3:]
        return f"list(len={len(value)}, head={head!r}, tail={tail!r})"

    if isinstance(value, tuple):
        if len(value) <= 12:
            return repr(value)
        head = value[:5]
        tail = value[-3:]
        return f"tuple(len={len(value)}, head={head!r}, tail={tail!r})"

    if isinstance(value, set):
        sorted_values = sorted(value)
        if len(sorted_values) <= 12:
            return repr(sorted_values)
        head = sorted_values[:5]
        tail = sorted_values[-3:]
        return f"set(len={len(sorted_values)}, head={head!r}, tail={tail!r})"

    if isinstance(value, dict):
        items = list(value.items())
        if len(items) <= 8:
            return repr(value)
        head = items[:4]
        tail = items[-2:]
        return f"dict(len={len(items)}, head={head!r}, tail={tail!r})"

    return repr(value)
