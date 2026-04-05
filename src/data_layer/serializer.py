import json

from .structures import Paper

def serialize_paper(paper: Paper) -> bytes:
    # details 是对具体信息的存放，整个结构前面还有加上信息头
    # 相当于obj->dict
    details = json.dumps(paper.to_dict(), ensure_ascii=False, separators=(",", ":"))
    return details.encode("utf-8")

def deserialize_paper(data: bytes) -> Paper:
    details = json.loads(data.decode("utf-8"))
    return Paper.from_dict(details)