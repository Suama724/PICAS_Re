"""
测试datalayer的基本类功能与timer
"""
from src.data_layer.structures import Paper, Author
from src.tools.timer import timer

def test_01_datalayer_structure_and_timer() -> None:
    # Paper 创建与默认
    paper_minimal = Paper(
        paper_id=None,
        source="DBLP",
        title="Minimal Paper"
    )
    assert paper_minimal.paper_id is None
    assert paper_minimal.title == "Minimal Paper"
    assert paper_minimal.authors == []  # 检查 default_factory=list 是否生效
    assert paper_minimal.source_extra == {} # 检查 default_factory=dict 是否生效

    # Paper 实例测试
    paper_full = Paper(
        paper_id=None,
        source="Arxiv",
        title="A Very Long Title That Exceeds Sixty Characters To Test The Truncation Logic In Repr",
        authors=["Alice", "Bob", "Charlie", "Dave"],
        year=2024,
        venue="ICLR",
        doi="10.1234/test",
        source_id="arxiv/test/2024",
        volume="1",
        pages="10-20",
        source_extra={"url": "https://test.com", "note": "test note"}
    )
    
    # 测试填入 paper_id
    paper_with_id = paper_full.get_paper_with_id(42)
    assert paper_with_id.paper_id == 42
    assert paper_full.paper_id is None  # 原实例应该不变

    # to_dict 与 build_from_dict
    paper_dict = paper_with_id.to_dict()
    assert isinstance(paper_dict, dict)
    assert paper_dict["paper_id"] == 42
    assert paper_dict["authors"] == ["Alice", "Bob", "Charlie", "Dave"]
    assert paper_dict["source_extra"]["url"] == "https://test.com"

    # 从字典重新构建
    paper_rebuilt = Paper.build_from_dict(paper_dict)
    assert paper_rebuilt.paper_id == 42
    assert paper_rebuilt.title == paper_full.title
    assert paper_rebuilt.authors == paper_full.authors
    assert paper_rebuilt.source_extra == paper_full.source_extra

    # __repr__
    paper_repr = repr(paper_full)
    assert "et al." in paper_repr  
    assert "..." in paper_repr     
    assert "Arxiv" in paper_repr
    
    # author
    author = Author(
        author_id=1001,
        name="Alice Zhang",
        paper_cnt=5
    )
    assert author.author_id == 1001
    assert author.name == "Alice Zhang"
    assert author.paper_cnt == 5

    author_repr = repr(author)
    assert "1001" in author_repr
    assert "Alice Zhang" in author_repr
    assert "5" in author_repr

    print(paper_repr)
    print(author_repr)

    # timer
    with timer("Test Timer: sum from 0 to 9999"):
        print(sum(range(10000)))

    print("Test Passed.")
    