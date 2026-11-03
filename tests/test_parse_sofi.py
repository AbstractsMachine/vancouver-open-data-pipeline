"""The payee rule decides which names may leave the warehouse: a name is published only with an organisation marker."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipeline" / "finance"))
from parse_sofi import amount, is_individual  # noqa: E402


@pytest.mark.parametrize(
    "name",
    ["Smith, John", "Park, Jiwon", "John Smith", "Lara Pratt", "Dr. Ann Lee", "Mr. Tom Wu"],
)
def test_names_shaped_like_a_person_are_withheld(name: str) -> None:
    assert is_individual(name)


@pytest.mark.parametrize(
    "name",
    ["Jiwon Park", "Sarah Bell", "Kevin Law", "John Shaw", "Mary Rogers", "David Ford", "Tom House", "Lee Power", "Ann J. Glass"],
)
def test_a_surname_that_is_also_a_marker_word_does_not_publish_a_person(name: str) -> None:
    assert is_individual(name)


@pytest.mark.parametrize(
    "name",
    [
        "Bell Canada",
        "Rogers",
        "Shaw Cablesystems GP",
        "Telus",
        "ICBC",
        "Park Board",
        "Ed Bell Plumbing Ltd",
        "J. Smith Consulting",
        "Aramark Canada Ltd",
        "Local 118",
        "Fire Chiefs Assn",
    ],
)
def test_organisations_stay_named(name: str) -> None:
    assert not is_individual(name)


def test_a_person_with_a_legal_form_is_a_firm() -> None:
    assert not is_individual("Smith, John Law Corporation")


@pytest.mark.parametrize(
    ("text", "value"),
    [("1,234", 1234), ("$ 25,000", 25000), ("(1,500)", -1500), ("-40", -40), ("12", 12)],
)
def test_amounts_read_as_printed(text: str, value: int) -> None:
    assert amount(text) == value


SUPPLIER_PAGE = "\n".join(
    [
        "SCHEDULE OF PAYMENTS TO SUPPLIERS OF GOODS AND SERVICES",
        "Name                                 Amount     Name                                  Amount",
        "Acme Paving Ltd                     125,000     Bell Canada                           30,500",
        "Northern Very Long Construction                 Smith, John                           26,000",
        "  Holdings Inc                       45,000     Sarah Bell                            27,100",
        "Zed Supply Co                        40,000     Vendors > $25,000              $    293,600",
    ]
)


def test_a_two_column_supplier_page_reads_every_row_and_its_control_total() -> None:
    from parse_sofi import parse_supplier_pages

    rows, controls, unread = parse_supplier_pages([SUPPLIER_PAGE], 2020)
    assert [(r["name"], r["amount_cad"], r["is_individual"]) for r in rows] == [
        ("Acme Paving Ltd", 125000, False),
        ("Bell Canada", 30500, False),
        ("Smith, John", 26000, True),
        ("Northern Very Long Construction Holdings Inc", 45000, False),
        ("Sarah Bell", 27100, True),
        ("Zed Supply Co", 40000, False),
    ]
    assert [(c["label"], c["amount_cad"]) for c in controls] == [("Vendors > $25,000", 293600)]
    assert sum(r["amount_cad"] for r in rows) == controls[0]["amount_cad"]
    assert unread == []


def test_an_amount_the_pdf_cut_short_is_kept_unread_not_guessed() -> None:
    from parse_sofi import read_pairs

    assert read_pairs(["Aramark Canada Ltd        ,541,678"], grouped=True) == [
        ("Aramark Canada Ltd", None, False, ",541,678")
    ]


def test_a_page_that_is_not_the_citys_supplier_schedule_is_skipped() -> None:
    from parse_sofi import parse_supplier_pages

    library = SUPPLIER_PAGE.replace("SCHEDULE OF PAYMENTS", "PUBLIC LIBRARY BOARD SCHEDULE OF PAYMENTS")
    assert parse_supplier_pages([library], 2020) == ([], [], [])
