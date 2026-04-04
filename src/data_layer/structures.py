"""
这里放data layer用到的结构体
具体来讲就是作者，用户等东西
"""

from dataclasses import dataclass, field, replace, asdict

@dataclass
class Paper:
    paper_id: int | None
    source: str
    title: str
    authors: list[str] = field(default_factory=list) # 防止传引用传乱了
    year: int | None = None
    venue: str | None = None  # 期刊或会议名

    doi: str | None = None
    source_id: str | None = None
    volume: str | None = None
    pages: str | None = None    
    # 这里放一下不同来源的paper的不共通的乱七八糟的信息
    source_extra: dict[str, str] = field(default_factory=dict)

    def get_paper_with_id(self, paper_id: int):
        return replace(self, paper_id=paper_id)
    
    def to_dict(self) -> dict[str, object]:
        return asdict(self)
    
    @classmethod
    def build_from_dict(cls, info: dict[str, object]):
        return Paper(
            paper_id=info.get("paper_id"),
            source=str(info.get("source", "")),
            title=str(info.get("title", "")),
            authors=list(info.get("authors", [])),
            year=info.get("year"),
            venue=info.get("venue"),
            doi=info.get("doi"),
            source_id=info.get("source_id"),
            volume=info.get("volume"),
            pages=info.get("pages"),
            source_extra=dict(info.get("source_extra", {}))
        )
    
    def __repr__(self) -> str:
        author_str = ",".join(self.authors[:3])
        if len(self.authors) > 3:
            author_str += " et al."
        
        extra_str = ( " | ".join(f"{k}:{v[:20]}" for k, v in self.source_extra.items())
                    if self.source_extra 
                    else "-" )
        return (
            f"Paper(id={self.paper_id})\n"
            f"  title: {self.title[:60]}{'...' if len(self.title) > 60 else ''}\n"
            f"  authors: [{author_str}]\n"
            f"  source: {self.source} | source_id: {self.source_id}\n"
            f"  year: {self.year} | venue: {self.venue}\n"
            f"  doi: {self.doi} \n"
            f"  extra: {extra_str}\n"
        )
    
"""
关于author
原来写了个类，
后来发现本地存就够了，直接读出来就是一个map与单独封装类完全等效
不过这里还是附上，虽然基本用的不多了
"""

@dataclass
class Author:
    author_id: int
    name: str
    paper_cnt: int = 0

    def __repr__(self):
        return (
            f"Author(id={self.author_id}),\n"
            f"-name='{self.name}'\n"
            f"-Paper Released Num={self.paper_cnt}\n"
        )