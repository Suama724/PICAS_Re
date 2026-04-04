'''
测试config里预设的路径是否正确以及parser
'''

from itertools import islice

from src.data_layer.xml_parser import parse_dblp_xml
from src.data_layer.config import *

from src.data_layer.config import (
    DATAS_DIR,
    RAW_DATA_DIR,
    SPLIT_DATA_DIR,
    TEST_DIR,
    LIBRARY_DIR,
    TMP_DIR,
    RAW_DBLP_XML_PATH
)

from src.tools.timer import timer

def test_02_path_and_parser(cnt=10) -> None:
    # config paths
    paths = [DATAS_DIR, RAW_DATA_DIR, SPLIT_DATA_DIR, RAW_DBLP_XML_PATH, TEST_DIR, LIBRARY_DIR, TMP_DIR]
    print(*(repr(path) for path in paths), sep="\n")

    path = SPLIT_DATA_DIR / "dblp_1m.xml"
    with timer("Time used for parse 1m indices"):
        action = parse_dblp_xml(path)
    papers = list(islice(action, cnt))
    for idx, paper in enumerate(papers, start=1):
        print(f"#{idx}: {paper}")
        if idx == cnt:
            break

    # 结果貌似无论10k还是1m都是 0.00002s，也就是说速度是非常快的