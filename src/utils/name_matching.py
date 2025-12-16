import re


def normalize_company_name(name: str) -> str:
    """Normalize company name for matching.

    - Uppercase
    - Remove "THE" prefix
    - Standardize LTD/LIMITED
    - Standardize &/AND
    - Remove extra whitespace
    """
    normalized = name.upper().strip()

    # Remove "THE" prefix
    normalized = re.sub(r"^THE\s+", "", normalized)

    # Standardize LIMITED/LTD
    normalized = re.sub(r"\bLIMITED\b", "LTD", normalized)

    # Standardize AND/&
    normalized = re.sub(r"\s*&\s*", " AND ", normalized)

    # Remove punctuation except alphanumeric and spaces
    normalized = re.sub(r"[^\w\s]", "", normalized)

    # Collapse multiple spaces
    normalized = re.sub(r"\s+", " ", normalized).strip()

    return normalized


def names_match(name1: str, name2: str) -> bool:
    """Check if two company names match after normalization."""
    return normalize_company_name(name1) == normalize_company_name(name2)
