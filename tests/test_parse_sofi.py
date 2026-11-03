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
