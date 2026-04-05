import re

_TOKEN_PATTERN = re.compile(r"[0-9a-z]+")

DEFAULT_STOPWORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "by",
        "for",
        "from",
        "in",
        "into",
        "is",
        "of",
        "on",
        "or",
        "the",
        "to",
        "with",
    }
)


def normalize_text(input_text: str) -> str:
    return " ".join(input_text.strip().lower().split())


def tokenize_title_keywords(input_text: str) -> list[str]:
    normalized = normalize_text(input_text)
    if not normalized:
        return []

    keywords: list[str] = []
    for token in _TOKEN_PATTERN.findall(normalized):
        if len(token) <= 1:
            continue
        if token in DEFAULT_STOPWORDS:
            continue
        keywords.append(token)

    return list(dict.fromkeys(keywords))


def tokenize_author_keywords(input_text: str) -> list[str]:
    normalized = normalize_text(input_text)
    if not normalized:
        return []

    keywords = [token for token in _TOKEN_PATTERN.findall(normalized) if token]
    return list(dict.fromkeys(keywords))


def tokenize_keywords(input_text: str) -> list[str]:
    return tokenize_title_keywords(input_text)
