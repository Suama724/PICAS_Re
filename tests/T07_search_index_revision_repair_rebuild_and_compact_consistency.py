import json
import shutil

from src.data_layer.config import DELTA_DIRNAME, MANIFEST_FILENAME, SEARCH_INDEX_MANIFEST_FILENAME
from src.data_layer.disk_index import SearchIndexManifest
from src.data_layer.engine import DatabaseEngine
from src.data_layer.manifest import Manifest

from ._data_layer_test_support import (
    assert_equal,
    assert_in,
    assert_not_in,
    assert_true,
    collect_query_samples,
    ensure_sliced_xml_path,
    make_paper,
    print_test_title,
    timed_step,
    reset_test_workspace,
)

KEEP_TITLE = "Stage Delta Keep Title"
DELETE_TITLE = "Stage Delta Delete Title"
DELTA_AUTHOR = "Stage Delta Author"
DELTA_VENUE = "Stage Delta Venue"


def test_07_search_index_revision_repair_rebuild_and_compact_consistency() -> None:
    print_test_title("T07", "Search Index Revision Repair Rebuild And Compact Consistency")
    xml_path = ensure_sliced_xml_path()
    samples = collect_query_samples(xml_path)
    workspace = reset_test_workspace("T07_search_index_revision_repair_rebuild_and_compact_consistency")

    with timed_step("Import base data, append two papers, and delete one base plus one appended paper"):
        engine = DatabaseEngine(workspace)
        import_stats = engine.import_xml(xml_path)

        appended_keep_id = engine.add_paper(
            make_paper(
                KEEP_TITLE,
                [DELTA_AUTHOR],
                2099,
                venue=DELTA_VENUE,
                source_id="t07-keep",
            )
        )
        appended_delete_id = engine.add_paper(
            make_paper(
                DELETE_TITLE,
                [DELTA_AUTHOR],
                2098,
                venue=DELTA_VENUE,
                source_id="t07-delete",
            )
        )
        base_deleted_id = engine.find_paper_ids_by_title(str(samples["title"]))[0]

        assert_true("[T07] delete sampled base paper", engine.delete_paper(base_deleted_id))
        assert_true("[T07] delete appended delete-title paper", engine.delete_paper(appended_delete_id))

        expected_active_ids = list(engine.iter_active_paper_ids())
        delta_root = workspace / "search_index" / DELTA_DIRNAME
        pre_compact_segments = sorted(path.name for path in delta_root.iterdir() if path.is_dir())

        assert_equal("[T07] import total_records_seen", 10000, import_stats["total_records_seen"])
        assert_in("[T07] keep id remains active", appended_keep_id, expected_active_ids)
        assert_not_in("[T07] deleted base id inactive", base_deleted_id, expected_active_ids)
        assert_not_in("[T07] deleted appended id inactive", appended_delete_id, expected_active_ids)
        assert_equal("[T07] keep title query before compact", [appended_keep_id], engine.find_paper_ids_by_title(KEEP_TITLE))
        assert_equal("[T07] delete title query before compact", [], engine.find_paper_ids_by_title(DELETE_TITLE))
        assert_equal("[T07] original title query excludes deleted base id", False, base_deleted_id in engine.find_paper_ids_by_title(str(samples["title"])))
        assert_equal("[T07] delta segment names before compact", ["segment_000001", "segment_000002"], pre_compact_segments)

    with timed_step("Compact search indexes and verify active view stays consistent while delta segments are cleared"):
        compact_stats = engine.compact_search_indexes()
        compacted = DatabaseEngine(workspace)
        post_compact_segments = sorted(path.name for path in delta_root.iterdir() if path.is_dir())

        assert_equal("[T07] compacted delta segment count", 2, compact_stats["compacted_delta_segments"])
        assert_equal("[T07] delta segment names after compact", [], post_compact_segments)
        assert_equal("[T07] active ids after compact", expected_active_ids, list(compacted.iter_active_paper_ids()))
        assert_equal("[T07] keep title query after compact", [appended_keep_id], compacted.find_paper_ids_by_title(KEEP_TITLE))
        assert_equal("[T07] delete title query after compact", [], compacted.find_paper_ids_by_title(DELETE_TITLE))
        assert_equal("[T07] deleted base id absent after compact", False, base_deleted_id in compacted.find_paper_ids_by_title(str(samples["title"])))

    with timed_step("Inject source_revision mismatch and verify reopen performs automatic repair"):
        repo_manifest_path = workspace / "meta_info" / MANIFEST_FILENAME
        search_manifest_path = workspace / "search_index" / SEARCH_INDEX_MANIFEST_FILENAME
        repo_manifest = Manifest.load(repo_manifest_path)
        injected_manifest = json.loads(search_manifest_path.read_text(encoding="utf-8"))
        injected_manifest["source_revision"] = max(0, repo_manifest.storage_revision - 1)
        search_manifest_path.write_text(
            json.dumps(injected_manifest, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        repaired = DatabaseEngine(workspace)
        repaired_manifest = SearchIndexManifest.load(search_manifest_path)
        assert_equal("[T07] repaired manifest exists", True, repaired_manifest is not None)
        assert_equal("[T07] repaired source_revision", repo_manifest.storage_revision, repaired_manifest.source_revision)
        assert_equal("[T07] active ids after repair", expected_active_ids, list(repaired.iter_active_paper_ids()))
        assert_equal("[T07] keep title query after repair", [appended_keep_id], repaired.find_paper_ids_by_title(KEEP_TITLE))
        assert_equal("[T07] delete title query after repair", [], repaired.find_paper_ids_by_title(DELETE_TITLE))

    with timed_step("Delete search_index directory, reopen from storage, rebuild explicitly, and compare final active view"):
        shutil.rmtree(workspace / "search_index")
        rebuilt_from_missing_index = DatabaseEngine(workspace)
        assert_equal("[T07] active ids after missing-index reopen", expected_active_ids, list(rebuilt_from_missing_index.iter_active_paper_ids()))
        assert_equal("[T07] keep title query after missing-index reopen", [appended_keep_id], rebuilt_from_missing_index.find_paper_ids_by_title(KEEP_TITLE))
        assert_equal("[T07] delete title query after missing-index reopen", [], rebuilt_from_missing_index.find_paper_ids_by_title(DELETE_TITLE))

        rebuild_stats = rebuilt_from_missing_index.rebuild_search_indexes()
        rebuilt = DatabaseEngine(workspace)

        assert_equal("[T07] rebuild total_records_seen", len(expected_active_ids), rebuild_stats["total_records_seen"])
        assert_equal("[T07] final active ids after rebuild", expected_active_ids, list(rebuilt.iter_active_paper_ids()))
        assert_equal("[T07] keep title query after rebuild", [appended_keep_id], rebuilt.find_paper_ids_by_title(KEEP_TITLE))
        assert_equal("[T07] delete title query after rebuild", [], rebuilt.find_paper_ids_by_title(DELETE_TITLE))
        assert_equal("[T07] deleted base id absent after rebuild", False, base_deleted_id in rebuilt.find_paper_ids_by_title(str(samples["title"])))

    print(f"xml_path={xml_path}")
    print(f"workspace={workspace}")
    print(f"base_deleted_id={base_deleted_id}")
    print(f"appended_keep_id={appended_keep_id}")
    print(f"appended_delete_id={appended_delete_id}")
    print(f"expected_active_records={len(expected_active_ids)}")
    print(f"compact_stats={compact_stats}")
    print(f"rebuild_stats={rebuild_stats}")
