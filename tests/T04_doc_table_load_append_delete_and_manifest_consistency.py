from src.data_layer.config import DOC_TABLE_FILENAME, MANIFEST_FILENAME
from src.data_layer.doc_table import ENTRY_SIZE, DocTable, DocTableEntry
from src.data_layer.manifest import Manifest
from src.data_layer.repository import Repository
from src.data_layer.xml_parser import parse_dblp_xml_bytes

from ._data_layer_test_support import (
    SAMPLE_XML_BATCH,
    assert_equal,
    assert_true,
    print_test_title,
    timed_step,
    reset_test_workspace,
)


def test_04_doc_table_load_append_delete_and_manifest_consistency() -> None:
    print_test_title("T04", "Doc Table Load Append Delete And Manifest Consistency")
    workspace = reset_test_workspace("T04_doc_table_load_append_delete_and_manifest_consistency")

    with timed_step("Directly create, load, append, and tombstone DocTable entries"):
        direct_doc_table_path = workspace / "doc_table_direct.bin"
        seed_doc_table = DocTable(direct_doc_table_path)
        seed_doc_table.put(
            DocTableEntry(
                paper_id=1,
                segment_id=98,
                offset=654321,
            )
        )
        loaded_doc_table = DocTable.load(direct_doc_table_path)
        loaded_doc_table.put(
            DocTableEntry(
                paper_id=2,
                segment_id=99,
                offset=123456,
            )
        )

        assert_equal(
            "[T04] doc table entry 1 after load",
            DocTableEntry(paper_id=1, segment_id=98, offset=654321, deleted=False),
            loaded_doc_table.get(1),
        )
        assert_equal(
            "[T04] doc table entry 2 after append",
            DocTableEntry(paper_id=2, segment_id=99, offset=123456, deleted=False),
            loaded_doc_table.get(2),
        )
        assert_equal("[T04] doc table record count before delete", 2, loaded_doc_table.record_count())

        loaded_doc_table.mark_deleted(2)
        assert_equal(
            "[T04] doc table entry 2 after mark_deleted",
            DocTableEntry(paper_id=2, segment_id=99, offset=123456, deleted=True),
            loaded_doc_table.get(2),
        )
        assert_equal("[T04] active ids after mark_deleted", [1], list(loaded_doc_table.iter_active_paper_ids()))

    with timed_step("Cross-check repository doc_table bytes and manifest counts after delete"):
        repo_workspace = workspace / "repo_case"
        repo = Repository(repo_workspace, segment_max_bytes=220)
        papers = list(parse_dblp_xml_bytes(SAMPLE_XML_BATCH))
        paper_ids = [repo.add(paper) for paper in papers]
        assert_equal("[T04] repository paper_ids", [1, 2, 3], paper_ids)

        assert_true("[T04] repository delete paper 1", repo.delete(1))
        repo.flush()
        reopened = Repository(repo_workspace, segment_max_bytes=220)

        doc_table_path = repo_workspace / "meta_info" / DOC_TABLE_FILENAME
        manifest_path = repo_workspace / "meta_info" / MANIFEST_FILENAME
        manifest = Manifest.load(manifest_path)

        assert_equal("[T04] doc table file size", 3 * ENTRY_SIZE, doc_table_path.stat().st_size)
        assert_equal("[T04] reopened doc table record count", 3, reopened.doc_table.record_count())
        assert_equal("[T04] reopened active ids", [2, 3], list(reopened.iter_active_paper_ids()))
        assert_equal("[T04] manifest total_records", 3, manifest.total_records)
        assert_equal("[T04] manifest active_records", 2, manifest.active_records)

    print(f"workspace={workspace}")
    print(f"direct_doc_table_path={direct_doc_table_path}")
    print(f"repo_workspace={repo_workspace}")
    print(f"doc_table_size_bytes={doc_table_path.stat().st_size}")
    print(f"entry_size={ENTRY_SIZE}")
