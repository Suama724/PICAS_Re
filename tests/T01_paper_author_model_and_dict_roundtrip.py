from src.data_layer.structures import Author, Paper

from ._data_layer_test_support import (
    assert_equal,
    print_test_title,
    timed_step,
)


def test_01_paper_author_model_and_dict_roundtrip() -> None:
    print_test_title("T01", "Paper Author Model And Dict Roundtrip")

    with timed_step("Create minimal Paper and verify default fields"):
        paper_minimal = Paper(
            paper_id=None,
            source="DBLP",
            title="Minimal Paper",
        )
        assert_equal("[T01] minimal paper_id", None, paper_minimal.paper_id)
        assert_equal("[T01] minimal title", "Minimal Paper", paper_minimal.title)
        assert_equal("[T01] minimal authors default", [], paper_minimal.authors)
        assert_equal("[T01] minimal source_extra default", {}, paper_minimal.source_extra)

    with timed_step("Create full Paper, assign paper_id, and verify dict roundtrip"):
        paper_full = Paper(
            paper_id=None,
            source="Arxiv",
            title="A Very Long Title That Exceeds Sixty Characters To Test The Truncation Logic In Repr",
            authors=["Alice", "Bob", "Charlie", "Dave"],
            year=2024,
            venue="ICLR",
            doi="10.1234/test",
            source_id="arxiv/test/2024",
            volume="1",
            pages="10-20",
            source_extra={"url": "https://test.com", "note": "test note"},
        )

        paper_with_id = paper_full.with_paper_id(42)
        assert_equal("[T01] reassigned paper_id", 42, paper_with_id.paper_id)
        assert_equal("[T01] original paper_id unchanged", None, paper_full.paper_id)

        paper_dict = paper_with_id.to_dict()
        assert_equal("[T01] dict paper_id", 42, paper_dict["paper_id"])
        assert_equal("[T01] dict authors", ["Alice", "Bob", "Charlie", "Dave"], paper_dict["authors"])
        assert_equal("[T01] dict source_extra.url", "https://test.com", paper_dict["source_extra"]["url"])

        paper_rebuilt = Paper.build_from_dict(paper_dict)
        assert_equal("[T01] rebuilt paper_id", 42, paper_rebuilt.paper_id)
        assert_equal("[T01] rebuilt title", paper_full.title, paper_rebuilt.title)
        assert_equal("[T01] rebuilt authors", paper_full.authors, paper_rebuilt.authors)
        assert_equal("[T01] rebuilt source_extra", paper_full.source_extra, paper_rebuilt.source_extra)

    with timed_step("Verify repr output for Paper and Author"):
        paper_repr = repr(paper_full)
        assert_equal("[T01] paper repr contains source", True, "Arxiv" in paper_repr)
        assert_equal("[T01] paper repr contains et al.", True, "et al." in paper_repr)
        assert_equal("[T01] paper repr contains truncation", True, "..." in paper_repr)

        author = Author(
            author_id=1001,
            name="Alice Zhang",
            paper_cnt=5,
        )
        assert_equal("[T01] author_id", 1001, author.author_id)
        assert_equal("[T01] author name", "Alice Zhang", author.name)
        assert_equal("[T01] author paper count", 5, author.paper_cnt)

        author_repr = repr(author)
        assert_equal("[T01] author repr contains id", True, "1001" in author_repr)
        assert_equal("[T01] author repr contains name", True, "Alice Zhang" in author_repr)
        assert_equal("[T01] author repr contains count", True, "5" in author_repr)

    print(paper_repr)
    print(author_repr)
