from pathlib import Path

'''
关于xml_parser的配置 ======================
'''

VALID_RECORD_TAGS = frozenset(
    {
        "article",
        "inproceedings",
        "incollection",
        "phdthesis",
        "mastersthesis",        
    }
)

'''
关于各种路径的配置 ======================
'''
PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATAS_DIR = PROJECT_ROOT / "datas"
RAW_DATA_DIR = DATAS_DIR / "raw_dblp_xml"
SPLIT_DATA_DIR = DATAS_DIR / "split_dblp_datas"
RAW_DBLP_XML_PATH = RAW_DATA_DIR / "dblp.xml"

TEST_DIR = DATAS_DIR / "picas_test_outputs"
DEFAULT_LIB_NAME = "lib_default"
LIBRARY_DIR = DATAS_DIR / "libraries" / f"{DEFAULT_LIB_NAME}"
TMP_DIR = DATAS_DIR / "tmp"


def ensure_datas_dir_layout() -> None:
    for path in (
        DATAS_DIR,
        RAW_DATA_DIR,
        SPLIT_DATA_DIR,
        TEST_DIR,
        LIBRARY_DIR,
        TMP_DIR
    ):
        path.mkdir(parents=True, exist_ok=True)


'''
关于data_slicer的配置 ======================
'''

DEFAULT_SLICE_SIZES = (int(1e4), int(1e5), int(1e6)) 

'''
关于本地存储的配置(id篇) ======================
'''

DEFAULT_SEGMENT_ID = 0
SEGMENT_MAX_BYTES = 512 * 1024 * 1024
SEGMENT_FILENAME_TEMPLATE = "data.seg{segment_id}.bin"
DOC_TABLE_FILENAME = "doc_table.bin"
MANIFEST_FILENAME = "manifest.json"

def ensure_lib_dirs(root: Path) -> dict[str, Path]:
    id_concrete_info_dir = root / "concrete_info"
    id_meta_info_dir = root / "meta_info"
    search_index_dir = root / "search_index"
    root.mkdir(parents=True, exist_ok=True)
    id_concrete_info_dir.mkdir(parents=True, exist_ok=True)
    id_meta_info_dir.mkdir(parents=True, exist_ok=True)
    search_index_dir.mkdir(parents=True, exist_ok=True)

    return {
        "root": root,
        "id_concrete_info_dir": id_concrete_info_dir,
        "id_meta_info_dir": id_meta_info_dir,
        "search_index_dir": search_index_dir
    }

'''
关于本地存储的配置(索引篇) ======================
'''

BUCKET_COUNT = 256
FORMAT_VERSION = 1
BASE_DIRNAME = "base"
DELTA_DIRNAME = "delta"
SEARCH_INDEX_MANIFEST_FILENAME = "manifest.json"
DEFAULT_DELTA_FLUSH_EVERY = 256

BUILDING_DIR_SUFFIX = ".__building__"
BACKUP_DIR_SUFFIX = ".__backup__"

EXACT_FIELD_NAMES = ("author", "title", "year", "venue") # 不能动
FIELD_NAMES = ("author", "title", "year", "venue", "title_keyword", "author_keyword")

# 保留旧命名别名，不知道哪里有没改回来的天天爆bug
EXACT_CATEGORY_NAMES = EXACT_FIELD_NAMES
CATEGORY_NAMES = FIELD_NAMES
