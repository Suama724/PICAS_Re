from src.data_layer.config import DOC_TABLE_FILENAME, MANIFEST_FILENAME
from src.data_layer.repository import Repository
from src.data_layer.xml_parser import parse_dblp_xml_bytes

from ._data_layer_test_support import (
    SAMPLE_XML_BATCH,
    SAMPLE_XML_SINGLE,
    assert_equal,
    assert_not_none,
    assert_true,
    print_test_title,
    timed_step,
    reset_test_workspace,
)


def test_03_repository_add_get_delete_and_segment_rollover() -> None:
    print_test_title("T03", "Repository Add Get Delete And Segment Rollover")

    with timed_step("Write one parsed XML record into Repository and verify persisted files"):
        single_workspace = reset_test_workspace("T03_repository_single_record")
        single_repo = Repository(single_workspace)
        single_paper = next(iter(parse_dblp_xml_bytes(SAMPLE_XML_SINGLE)))
        single_paper_id = single_repo.add(single_paper)
        stored_single = single_repo.get(single_paper_id)

        assert_equal("[T03] original paper_id before add", None, single_paper.paper_id)
        assert_equal("[T03] allocated paper_id", 1, single_paper_id)
        assert_not_none("[T03] stored single paper", stored_single)
        assert_equal("[T03] stored single paper_id", 1, stored_single.paper_id)
        assert_equal("[T03] stored single title", "Graph Search Pipelines", stored_single.title)
        assert_equal("[T03] stored single year", 2026, stored_single.year)
        assert_equal(
            "[T03] single segment file exists",
            True,
            (single_workspace / "concrete_info" / "data.seg0.bin").exists(),
        )
        assert_equal(
            "[T03] single doc table exists",
            True,
            (single_workspace / "meta_info" / DOC_TABLE_FILENAME).exists(),
        )
        assert_equal(
            "[T03] single manifest exists",
            True,
            (single_workspace / "meta_info" / MANIFEST_FILENAME).exists(),
        )

    with timed_step("Write multiple records with a small segment limit and verify rollover"):
        rollover_workspace = reset_test_workspace("T03_repository_segment_rollover")
        papers = list(parse_dblp_xml_bytes(SAMPLE_XML_BATCH))
        rollover_repo = Repository(rollover_workspace, segment_max_bytes=220)
        paper_ids = [rollover_repo.add(paper) for paper in papers]

        assert_equal("[T03] allocated paper_ids", [1, 2, 3], paper_ids)
        assert_equal("[T03] active record count before reopen", 3, rollover_repo.count())
        assert_equal("[T03] active ids before reopen", [1, 2, 3], list(rollover_repo.iter_active_paper_ids()))
        assert_equal("[T03] segment rollover happened", True, len(rollover_repo.segment_paths()) >= 2)

    with timed_step("Reopen repository, verify reads, delete one record, and verify active view"):
        reopened = Repository(rollover_workspace, segment_max_bytes=220)
        restored_1 = reopened.get(1)
        restored_2 = reopened.get(2)
        restored_3 = reopened.get(3)

        assert_not_none("[T03] restored paper 1", restored_1)
        assert_equal("[T03] restored paper 1 title", "Storage First", restored_1.title)
        assert_not_none("[T03] restored paper 2", restored_2)
        assert_equal("[T03] restored paper 2 authors", ["Bob Li", "Carol Wu"], restored_2.authors)
        assert_not_none("[T03] restored paper 3", restored_3)
        assert_equal("[T03] restored paper 3 title", "Segment Rollover", restored_3.title)

        assert_true("[T03] delete paper 1", reopened.delete(1))
        reopened.flush()
        after_delete = Repository(rollover_workspace, segment_max_bytes=220)

        assert_equal("[T03] active record count after delete", 2, after_delete.count())
        assert_equal("[T03] active ids after delete", [2, 3], list(after_delete.iter_active_paper_ids()))
        assert_equal("[T03] deleted paper returns None", None, after_delete.get(1))
        assert_not_none("[T03] kept paper 2 still readable", after_delete.get(2))

    print(f"single_workspace={single_workspace}")
    print(f"rollover_workspace={rollover_workspace}")
    print(f"segment_paths={rollover_repo.segment_paths()}")
