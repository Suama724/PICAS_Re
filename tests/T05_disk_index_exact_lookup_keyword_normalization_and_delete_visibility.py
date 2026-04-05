from src.data_layer.disk_index import (
    find_paper_ids_by_author,
    find_paper_ids_by_author_keywords,
    find_paper_ids_by_keywords,
    find_paper_ids_by_title,
    find_paper_ids_by_venue,
    find_paper_ids_by_year,
)
from src.data_layer.engine import DatabaseEngine

from ._data_layer_test_support import (
    assert_equal,
    assert_true,
    make_paper,
    print_test_title,
    timed_step,
    reset_test_workspace,
)


def test_05_disk_index_exact_lookup_keyword_normalization_and_delete_visibility() -> None:
    print_test_title("T05", "Disk Index Exact Lookup Keyword Normalization And Delete Visibility")
    workspace = reset_test_workspace("T05_disk_index_exact_lookup_keyword_normalization_and_delete_visibility")
    engine = DatabaseEngine(workspace)

    with timed_step("Create indexed papers that cover exact match and keyword match semantics"):
        paper_ids = [
            engine.add_paper(
                make_paper(
                    "Compiler Patterns for COBOL",
                    ["Grace Hopper", "Alan Turing"],
                    1959,
                    venue="Queue Conf",
                    source_id="t05-1",
                )
            ),
            engine.add_paper(
                make_paper(
                    "COBOL Language Notes",
                    ["Grace Murray Hopper"],
                    1960,
                    venue="Queue Conf",
                    source_id="t05-2",
                )
            ),
            engine.add_paper(
                make_paper(
                    "Distributed Queue Systems",
                    ["Andrew Hopper"],
                    1990,
                    venue="Queue Journal",
                    source_id="t05-3",
                )
            ),
            engine.add_paper(
                make_paper(
                    "Relational Foundations",
                    ["E. F. Codd"],
                    1971,
                    venue="Data Conf",
                    source_id="t05-4",
                )
            ),
        ]
        assert_equal("[T05] allocated paper_ids", [1, 2, 3, 4], paper_ids)

    with timed_step("Verify exact lookup normalization and author-keyword same-author semantics in engine"):
        assert_equal("[T05] author exact normalized", [paper_ids[0]], engine.find_paper_ids_by_author("  GRACE HOPPER  "))
        assert_equal("[T05] title exact normalized", [paper_ids[0]], engine.find_paper_ids_by_title("  Compiler   Patterns for   COBOL  "))
        assert_equal("[T05] venue exact normalized", paper_ids[:2], engine.find_paper_ids_by_venue("  queue  conf "))
        assert_equal("[T05] year exact", [paper_ids[1]], engine.find_paper_ids_by_year(1960))
        assert_equal("[T05] title keywords", [paper_ids[0]], engine.find_paper_ids_by_keywords(" compiler   cobol "))
        assert_equal("[T05] author keyword grace", paper_ids[:2], engine.find_paper_ids_by_author_keywords("grace"))
        assert_equal("[T05] author keyword hopper", paper_ids[:3], engine.find_paper_ids_by_author_keywords("hopper"))
        assert_equal("[T05] author keyword grace hopper", paper_ids[:2], engine.find_paper_ids_by_author_keywords("grace hopper"))
        assert_equal("[T05] author keyword grace murray hopper", [paper_ids[1]], engine.find_paper_ids_by_author_keywords("grace murray hopper"))
        assert_equal("[T05] author keyword grace turing", [], engine.find_paper_ids_by_author_keywords("grace turing"))
        assert_equal("[T05] author keyword e f codd", [paper_ids[3]], engine.find_paper_ids_by_author_keywords(" e  f codd "))

    with timed_step("Flush and reopen, then compare engine query results with direct disk-index lookup results"):
        engine.flush()
        reopened = DatabaseEngine(workspace)

        assert_equal("[T05] reopened author exact", [paper_ids[0]], reopened.find_paper_ids_by_author("Grace Hopper"))
        assert_equal("[T05] reopened title exact", [paper_ids[0]], reopened.find_paper_ids_by_title("Compiler Patterns for COBOL"))
        assert_equal("[T05] reopened venue exact", paper_ids[:2], reopened.find_paper_ids_by_venue("Queue Conf"))
        assert_equal("[T05] reopened year exact", [paper_ids[1]], reopened.find_paper_ids_by_year(1960))
        assert_equal("[T05] reopened title keywords", [paper_ids[0]], reopened.find_paper_ids_by_keywords("compiler cobol"))
        assert_equal("[T05] reopened author keywords", paper_ids[:2], reopened.find_paper_ids_by_author_keywords("grace hopper"))

        assert_equal("[T05] disk author exact", [paper_ids[0]], find_paper_ids_by_author(reopened.search_index_dir, "Grace Hopper"))
        assert_equal("[T05] disk title exact", [paper_ids[0]], find_paper_ids_by_title(reopened.search_index_dir, "Compiler Patterns for COBOL"))
        assert_equal("[T05] disk venue exact", paper_ids[:2], find_paper_ids_by_venue(reopened.search_index_dir, "Queue Conf"))
        assert_equal("[T05] disk year exact", [paper_ids[1]], find_paper_ids_by_year(reopened.search_index_dir, 1960))
        assert_equal("[T05] disk title keywords", [paper_ids[0]], find_paper_ids_by_keywords(reopened.search_index_dir, "compiler cobol"))
        assert_equal("[T05] disk author keywords", paper_ids[:2], find_paper_ids_by_author_keywords(reopened.search_index_dir, "grace hopper"))

    with timed_step("Delete one paper and verify both engine and disk-index views hide the deleted record"):
        assert_true("[T05] delete paper 1", reopened.delete_paper(paper_ids[0]))
        reopened.flush()
        after_delete = DatabaseEngine(workspace)

        assert_equal("[T05] author exact after delete", [], after_delete.find_paper_ids_by_author("Grace Hopper"))
        assert_equal("[T05] title exact after delete", [], after_delete.find_paper_ids_by_title("Compiler Patterns for COBOL"))
        assert_equal("[T05] title keywords after delete", [], after_delete.find_paper_ids_by_keywords("compiler cobol"))
        assert_equal("[T05] author keywords after delete", [paper_ids[1]], after_delete.find_paper_ids_by_author_keywords("grace hopper"))

        assert_equal("[T05] disk author exact after delete", [], find_paper_ids_by_author(after_delete.search_index_dir, "Grace Hopper"))
        assert_equal("[T05] disk title exact after delete", [], find_paper_ids_by_title(after_delete.search_index_dir, "Compiler Patterns for COBOL"))
        assert_equal("[T05] disk title keywords after delete", [], find_paper_ids_by_keywords(after_delete.search_index_dir, "compiler cobol"))
        assert_equal("[T05] disk author keywords after delete", [paper_ids[1]], find_paper_ids_by_author_keywords(after_delete.search_index_dir, "grace hopper"))

    print(f"workspace={workspace}")
    print(f"paper_ids={paper_ids}")
    print(f"remaining_grace_hopper_ids={after_delete.find_paper_ids_by_author_keywords('grace hopper')}")
