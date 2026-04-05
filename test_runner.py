import sys

from tests import (
    test_01_paper_author_model_and_dict_roundtrip,
    test_02_xml_parser_streaming_and_bytes_input,
    test_03_repository_add_get_delete_and_segment_rollover,
    test_04_doc_table_load_append_delete_and_manifest_consistency,
    test_05_disk_index_exact_lookup_keyword_normalization_and_delete_visibility,
    test_06_database_engine_import_query_delete_and_reopen_consistency,
    test_07_search_index_revision_repair_rebuild_and_compact_consistency,
    test_08_repository_append_and_engine_import_performance_baseline_10k,
    test_09_full_dblp_import_persistence_and_random_sample_verification_manual,
)

TESTS = {
    "T01": test_01_paper_author_model_and_dict_roundtrip,
    "T02": test_02_xml_parser_streaming_and_bytes_input,
    "T03": test_03_repository_add_get_delete_and_segment_rollover,
    "T04": test_04_doc_table_load_append_delete_and_manifest_consistency,
    "T05": test_05_disk_index_exact_lookup_keyword_normalization_and_delete_visibility,
    "T06": test_06_database_engine_import_query_delete_and_reopen_consistency,
    "T07": test_07_search_index_revision_repair_rebuild_and_compact_consistency,
    "T08": test_08_repository_append_and_engine_import_performance_baseline_10k,
    "T09": test_09_full_dblp_import_persistence_and_random_sample_verification_manual,
}


if __name__ == "__main__":
    selected = sys.argv[1:]

    for name in selected:
        test_fn = TESTS[name.upper()]
        test_fn()
        print("\n")
