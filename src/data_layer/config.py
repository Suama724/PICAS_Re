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
