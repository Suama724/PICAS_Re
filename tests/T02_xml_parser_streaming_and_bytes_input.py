from itertools import islice

from src.data_layer.xml_parser import parse_dblp_xml, parse_dblp_xml_bytes

from ._data_layer_test_support import (
    SAMPLE_XML_SINGLE,
    assert_equal,
    print_test_title,
    timed_step,
    ensure_sliced_xml_path,
)


def test_02_xml_parser_streaming_and_bytes_input() -> None:
    print_test_title("T02", "XML Parser Streaming And Bytes Input")

    with timed_step("Parse a single XML record from in-memory bytes and verify extracted fields"):
        parsed_from_bytes = list(parse_dblp_xml_bytes(SAMPLE_XML_SINGLE))
        assert_equal("[T02] parsed record count from bytes", 1, len(parsed_from_bytes))

        paper = parsed_from_bytes[0]
        assert_equal("[T02] bytes paper_id", None, paper.paper_id)
        assert_equal("[T02] bytes source", "dblp", paper.source)
        assert_equal("[T02] bytes source_id", "journals/test/parser-sample", paper.source_id)
        assert_equal("[T02] bytes title", "Graph Search Pipelines", paper.title)
        assert_equal("[T02] bytes authors", ["Alice Zhang", "Bob Li"], paper.authors)
        assert_equal("[T02] bytes year", 2026, paper.year)
        assert_equal("[T02] bytes venue", "Journal of Parser Tests", paper.venue)
        assert_equal("[T02] bytes volume", "7", paper.volume)
        assert_equal("[T02] bytes pages", "11-19", paper.pages)
        assert_equal("[T02] bytes doi", "10.1234/parser-sample", paper.doi)
        assert_equal("[T02] bytes source_extra", {"publisher": "PICAS Press"}, paper.source_extra)

    with timed_step("Stream the first 20 records from dblp_10k.xml and verify parser output shape"):
        xml_path = ensure_sliced_xml_path()
        streamed_papers = list(islice(parse_dblp_xml(xml_path), 20))
        assert_equal("[T02] streamed record count", 20, len(streamed_papers))
        assert_equal(
            "[T02] all streamed paper_id are None",
            True,
            all(streamed_paper.paper_id is None for streamed_paper in streamed_papers),
        )
        assert_equal(
            "[T02] all streamed source are dblp",
            True,
            all(streamed_paper.source == "dblp" for streamed_paper in streamed_papers),
        )
        assert_equal(
            "[T02] all streamed source_id present",
            True,
            all(streamed_paper.source_id for streamed_paper in streamed_papers),
        )
        assert_equal(
            "[T02] all streamed titles are strings",
            True,
            all(isinstance(streamed_paper.title, str) for streamed_paper in streamed_papers),
        )
        assert_equal(
            "[T02] at least one title is non-empty",
            True,
            any(streamed_paper.title for streamed_paper in streamed_papers),
        )
        assert_equal(
            "[T02] at least one record has authors",
            True,
            any(streamed_paper.authors for streamed_paper in streamed_papers),
        )
        assert_equal(
            "[T02] at least one record has venue",
            True,
            any(streamed_paper.venue for streamed_paper in streamed_papers),
        )
        assert_equal(
            "[T02] at least one record has year",
            True,
            any(streamed_paper.year is not None for streamed_paper in streamed_papers),
        )

    print(f"xml_path={xml_path}")
    print(f"streamed_records_checked={len(streamed_papers)}")
    print(f"first_streamed_record={streamed_papers[0]!r}")
