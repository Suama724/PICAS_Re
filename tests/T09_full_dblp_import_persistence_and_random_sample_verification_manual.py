import time
from pathlib import Path

from src.data_layer.config import MANIFEST_FILENAME, SEARCH_INDEX_MANIFEST_FILENAME
from src.data_layer.disk_index import SearchIndexManifest
from src.data_layer.engine import DatabaseEngine
from src.data_layer.manifest import Manifest

from ._data_layer_test_support import (
    assert_equal,
    print_test_title,
    timed_step,
    ensure_raw_dblp_xml_path,
    reservoir_sample_source_papers,
    reset_test_workspace,
)


def _segment_paths(workspace: Path) -> list[Path]:
    return sorted((workspace / "concrete_info").glob("data.seg*.bin"))


def test_09_full_dblp_import_persistence_and_random_sample_verification_manual(
    sample_size: int = 100,
    seed: int = 20260405,
) -> None:
    print_test_title("T09", "Full Dblp Import Persistence And Random Sample Verification Manual")
    xml_path = ensure_raw_dblp_xml_path()
    if not xml_path.is_file():
        print(f"[T09] skipped: raw dblp.xml not found at {xml_path}")
        return

    workspace = reset_test_workspace("T09_full_dblp_import_persistence_and_random_sample_verification_manual")

    with timed_step("Reservoir-sample source papers from raw dblp.xml for later readback verification"):
        sampling_start = time.perf_counter()
        total_source_records, samples = reservoir_sample_source_papers(xml_path, sample_size, seed)
        sampling_elapsed = time.perf_counter() - sampling_start
        assert_equal("[T09] sampled record count", min(sample_size, total_source_records), len(samples))

    with timed_step("Import raw dblp.xml through DatabaseEngine and persist repository plus search index"):
        engine = DatabaseEngine(workspace)
        import_start = time.perf_counter()
        import_stats = engine.import_xml(xml_path)
        import_elapsed = time.perf_counter() - import_start

    with timed_step("Reopen workspace, compare sampled papers, and inspect manifest plus segment metadata"):
        reopened = DatabaseEngine(workspace)
        mismatches: list[dict[str, object]] = []
        for expected in samples:
            restored = reopened.get_paper(int(expected["paper_id"]))
            if restored is None:
                mismatches.append(
                    {
                        "paper_id": expected["paper_id"],
                        "field": "missing",
                    }
                )
                continue

            actual = {
                "paper_id": restored.paper_id,
                "source_id": restored.source_id,
                "title": restored.title,
                "authors": list(restored.authors),
                "year": restored.year,
                "venue": restored.venue,
            }
            for field in ("paper_id", "source_id", "title", "authors", "year", "venue"):
                if actual[field] != expected[field]:
                    mismatches.append(
                        {
                            "paper_id": expected["paper_id"],
                            "field": field,
                            "expected": expected[field],
                            "actual": actual[field],
                        }
                    )

        repo_manifest = Manifest.load(workspace / "meta_info" / MANIFEST_FILENAME)
        index_manifest = SearchIndexManifest.load(workspace / "search_index" / SEARCH_INDEX_MANIFEST_FILENAME)
        segment_paths = _segment_paths(workspace)
        segment_sizes = [segment_path.stat().st_size for segment_path in segment_paths]
        import_rate = import_stats["total_records_seen"] / import_elapsed if import_elapsed > 0 else 0.0

    print(f"xml_path={xml_path}")
    print(f"workspace={workspace}")
    print(f"sampling_elapsed_seconds={sampling_elapsed:.2f}")
    print(f"import_elapsed_seconds={import_elapsed:.2f}")
    print(f"records_per_second={import_rate:.2f}")
    print(f"total_source_records={total_source_records}")
    print(f"import_total_records_seen={import_stats['total_records_seen']}")
    print(f"active_records={reopened.count_papers()}")
    print(f"segment_count={len(segment_paths)}")
    print(f"segment_total_size_bytes={sum(segment_sizes)}")
    print(f"sample_size={len(samples)}")
    print(f"sample_mismatches={len(mismatches)}")
    print(f"repo_storage_revision={repo_manifest.storage_revision}")
    print(f"index_source_revision={index_manifest.source_revision if index_manifest else None}")

    assert_equal("[T09] total source records > 0", True, total_source_records > 0)
    assert_equal("[T09] imported record count", total_source_records, import_stats["total_records_seen"])
    assert_equal("[T09] reopened active record count", total_source_records, reopened.count_papers())
    assert_equal("[T09] repo manifest total_records", total_source_records, repo_manifest.total_records)
    assert_equal("[T09] repo manifest active_records", total_source_records, repo_manifest.active_records)
    assert_equal("[T09] index manifest exists", True, index_manifest is not None)
    assert_equal("[T09] index source_revision matches repo", repo_manifest.storage_revision, index_manifest.source_revision)
    assert_equal("[T09] segment files exist", True, bool(segment_paths))
    assert_equal("[T09] sample size", min(sample_size, total_source_records), len(samples))
    assert_equal("[T09] sample mismatches", [], mismatches)
