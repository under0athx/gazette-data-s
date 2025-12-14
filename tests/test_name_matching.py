
from src.utils.name_matching import names_match, normalize_company_name


class TestNormalizeCompanyName:
    def test_uppercase(self):
        assert normalize_company_name("smith properties ltd") == "SMITH PROPERTIES LTD"

    def test_removes_the_prefix(self):
        assert normalize_company_name("The ABC Company") == "ABC COMPANY"

    def test_standardizes_limited(self):
        assert normalize_company_name("Smith Limited") == "SMITH LTD"

    def test_standardizes_ampersand(self):
        assert normalize_company_name("J & K Holdings") == "J AND K HOLDINGS"

    def test_removes_punctuation(self):
        assert normalize_company_name("Smith's Properties, Ltd.") == "SMITHS PROPERTIES LTD"

    def test_collapses_whitespace(self):
        assert normalize_company_name("Smith   Properties   Ltd") == "SMITH PROPERTIES LTD"


class TestNamesMatch:
    def test_exact_match(self):
        assert names_match("Smith Ltd", "SMITH LTD")

    def test_limited_vs_ltd(self):
        assert names_match("Smith Limited", "Smith Ltd")

    def test_the_prefix(self):
        assert names_match("The ABC Company", "ABC Company")

    def test_ampersand_vs_and(self):
        assert names_match("J & K Holdings", "J AND K HOLDINGS LTD")

    def test_no_match(self):
        assert not names_match("Smith Ltd", "Jones Ltd")
