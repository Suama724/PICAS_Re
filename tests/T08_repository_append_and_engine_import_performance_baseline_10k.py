import time

from src.data_layer.config import DOC_TABLE_FILENAME, MANIFEST_FILENAME, SEARCH_INDEX_MANIFEST_FILENAME
from src.data_layer.engine import DatabaseEngine
from src.data_layer.repository import Repository
from src.data_layer.xml_parser import parse_dblp_xml

from ._data_layer_test_support import (
    assert_equal,
    print_test_title,
    timed_step,
    ensure_sliced_xml_path,
    reset_test_workspace,
)


def test_08_repository_append_and_engine_import_performance_baseline_10k() -> None:
    print_test_title("T08", "Repository Append And Engine Import Performance Baseline 10k")
    xml_path = ensure_sliced_xml_path()

    with timed_step("Benchmark Repository append and flush on dblp_10k.xml"):
        repo_workspace = reset_test_workspace("T08_repository_append_performance")
        repo = Repository(repo_workspace, auto_flush=False)
        repo_total_records = 0
        repo_start = time.perf_counter()
        for paper in parse_dblp_xml(xml_path):
            repo.add(paper)
            repo_total_records += 1
        repo.flush()
        repo_elapsed = time.perf_counter() - repo_start
        repo_rate = repo_total_records / repo_elapsed if repo_elapsed > 0 else 0.0

        assert_equal("[T08] repository total_records", 10000, repo_total_records)
        assert_equal("[T08] repository count after flush", repo_total_records, repo.count())
        assert_equal("[T08] repository doc table exists", True, (repo_workspace / "meta_info" / DOC_TABLE_FILENAME).exists())
        assert_equal("[T08] repository manifest exists", True, (repo_workspace / "meta_info" / MANIFEST_FILENAME).exists())

    with timed_step("Benchmark DatabaseEngine import_xml on dblp_10k.xml"):
        engine_workspace = reset_test_workspace("T08_engine_import_performance")
        engine = DatabaseEngine(engine_workspace)
        engine_start = time.perf_counter()
        import_stats = engine.import_xml(xml_path)
        engine_elapsed = time.perf_counter() - engine_start
        engine_rate = import_stats["total_records_seen"] / engine_elapsed if engine_elapsed > 0 else 0.0

        assert_equal("[T08] engine total_records_seen", 10000, import_stats["total_records_seen"])
        assert_equal("[T08] engine count after import", 10000, engine.count_papers())
        assert_equal(
            "[T08] search index manifest exists",
            True,
            (engine_workspace / "search_index" / SEARCH_INDEX_MANIFEST_FILENAME).exists(),
        )

    print(f"xml_path={xml_path}")
    print(f"repository_workspace={repo_workspace}")
    print(f"repository_total_records={repo_total_records}")
    print(f"repository_elapsed_seconds={repo_elapsed:.2f}")
    print(f"repository_records_per_second={repo_rate:.2f}")
    print(f"engine_workspace={engine_workspace}")
    print(f"engine_total_records={import_stats['total_records_seen']}")
    print(f"engine_elapsed_seconds={engine_elapsed:.2f}")
    print(f"engine_records_per_second={engine_rate:.2f}")
