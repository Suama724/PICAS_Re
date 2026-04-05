from src.data_layer.engine import DatabaseEngine

from ._data_layer_test_support import (
    assert_equal,
    assert_not_none,
    assert_true,
    collect_query_samples,
    ensure_sliced_xml_path,
    print_test_title,
    timed_step,
    reset_test_workspace,
)


def test_06_database_engine_import_query_delete_and_reopen_consistency() -> None:
    print_test_title("T06", "Database Engine Import Query Delete And Reopen Consistency")
    xml_path = ensure_sliced_xml_path()
    samples = collect_query_samples(xml_path)
    workspace = reset_test_workspace("T06_database_engine_import_query_delete_and_reopen_consistency")

    with timed_step("Import dblp_10k.xml and capture count/query baseline"):
        engine = DatabaseEngine(workspace)
        import_stats = engine.import_xml(xml_path)
        count_after_import = engine.count_papers()
        active_ids = list(engine.iter_active_paper_ids())
        first_paper = engine.get_paper(1)

        author_hits = engine.find_paper_ids_by_author(str(samples["author"]))
        title_hits = engine.find_paper_ids_by_title(str(samples["title"]))
        venue_hits = engine.find_paper_ids_by_venue(str(samples["venue"]))
        year_hits = engine.find_paper_ids_by_year(int(samples["year"]))

        assert_equal("[T06] import total_records_seen", 10000, import_stats["total_records_seen"])
        assert_equal("[T06] count after import", 10000, count_after_import)
        assert_equal("[T06] first active id", 1, active_ids[0])
        assert_equal("[T06] last active id before delete", count_after_import, active_ids[-1])
        assert_not_none("[T06] get_paper(1)", first_paper)
        assert_equal("[T06] author hits non-empty", True, bool(author_hits))
        assert_equal("[T06] title hits non-empty", True, bool(title_hits))
        assert_equal("[T06] venue hits non-empty", True, bool(venue_hits))
        assert_equal("[T06] year hits non-empty", True, bool(year_hits))

    with timed_step("Delete one sampled title hit and verify live engine state is corrected"):
        deleted_paper_id = title_hits[0]
        assert_true("[T06] delete sampled paper", engine.delete_paper(deleted_paper_id))
        assert_equal("[T06] count after delete", count_after_import - 1, engine.count_papers())
        assert_equal(
            "[T06] deleted id absent from title query",
            False,
            deleted_paper_id in engine.find_paper_ids_by_title(str(samples["title"])),
        )

    with timed_step("Reopen DatabaseEngine and verify delete/query consistency persists"):
        reopened = DatabaseEngine(workspace)
        reopened_active_ids = list(reopened.iter_active_paper_ids())

        assert_equal("[T06] count after reopen", count_after_import - 1, reopened.count_papers())
        assert_equal("[T06] active id count after reopen", count_after_import - 1, len(reopened_active_ids))
        assert_equal("[T06] deleted id absent from active ids after reopen", False, deleted_paper_id in reopened_active_ids)
        assert_equal(
            "[T06] deleted id absent from title query after reopen",
            False,
            deleted_paper_id in reopened.find_paper_ids_by_title(str(samples["title"])),
        )
        assert_equal("[T06] author hits still available after reopen", True, bool(reopened.find_paper_ids_by_author(str(samples["author"]))))
        assert_equal("[T06] venue hits still available after reopen", True, bool(reopened.find_paper_ids_by_venue(str(samples["venue"]))))
        assert_equal("[T06] year hits still available after reopen", True, bool(reopened.find_paper_ids_by_year(int(samples["year"]))))

    print(f"xml_path={xml_path}")
    print(f"workspace={workspace}")
    print(f"import_total_records_seen={import_stats['total_records_seen']}")
    print(f"deleted_paper_id={deleted_paper_id}")
    print(f"count_after_reopen={reopened.count_papers()}")
