"""
test_commbank_parse.py — Tests for CommBank statement text parsing (no PDF needed).
"""

from datetime import date

import pytest

from database.transaction.ingest.commbank import (
    CommbankParseError, _assign_ids, parse_commbank_text,
)

HEADER = """Account Number 000000 12345678
Page 1 of 2
JANE CITIZEN
1 EXAMPLE ST
Account name JANE CITIZEN
Date Transaction details Amount Balance
"""

FOOTER = """Any pending transactions haven’t been included in this list. Proceeds of cheques aren’t available until
cleared.
Kind regards,
The CommBank Team.
"""


def make_text(body: str) -> str:
    return HEADER + body + FOOTER


SAMPLE = make_text("""11 Sep 2026 Fast Transfer From Jane Citizen $100.00 $100.00
CREDIT TO ACCOUNT
13 Sep 2026 SOME CAFE MELBOURNE AU -$7.12 $92.88
15 Sep 2026 KFC St Kilda AUS -$12.95 $79.93
Card xx0488
Value Date: 13/09/2026
Created 01/10/26 02:37pm (Sydney/Melbourne time)
While this letter is accurate at the time it’s produced,
Account Number 000000 12345678
Page 2 of 2
Date Transaction details Amount Balance
16 Sep 2026 Transfer To Jane Citizen -$1.00 $78.93
PayID Email from CommBank App
Test
""")


def test_parses_all_rows():
    txs = parse_commbank_text(SAMPLE)
    assert len(txs) == 4
    assert [t["amount_cents"] for t in txs] == [10000, -712, -1295, -100]
    assert txs[0]["posted_date"] == date(2026, 9, 11)


def test_card_and_value_date_attached():
    tx = parse_commbank_text(SAMPLE)[2]
    assert tx["card_suffix"] == "0488"
    assert tx["value_date"] == date(2026, 9, 13)
    assert tx["notes"] == []


def test_notes_captured_noise_dropped():
    txs = parse_commbank_text(SAMPLE)
    assert txs[0]["notes"] == []  # CREDIT TO ACCOUNT is noise
    assert txs[3]["notes"] == ["PayID Email from CommBank App", "Test"]


def test_header_not_parsed_into_rows():
    txs = parse_commbank_text(SAMPLE)
    assert all("JANE" not in t["description"] and "EXAMPLE" not in " ".join(t["notes"]) for t in txs)


def test_page_furniture_not_in_notes():
    for t in parse_commbank_text(SAMPLE):
        assert not any(n.startswith(("Created", "While", "Page", "Account Number")) for n in t["notes"])


def test_footer_not_in_notes():
    assert all("pending" not in " ".join(t["notes"]) for t in parse_commbank_text(SAMPLE))


def test_thousands_separator_and_negative_balance():
    text = make_text(
        "01 Sep 2026 Pay $1,234.56 $1,234.56\n"
        "02 Sep 2026 Rent -$1,300.00 -$65.44\n"
    )
    txs = parse_commbank_text(text)
    assert txs[0]["amount_cents"] == 123456
    assert txs[1]["balance_cents"] == -6544


def test_balance_mismatch_raises():
    text = make_text(
        "01 Sep 2026 A -$1.00 $9.00\n"
        "02 Sep 2026 B -$1.00 $5.00\n"
    )
    with pytest.raises(CommbankParseError, match="reconcile"):
        parse_commbank_text(text)


def test_no_transactions_raises():
    with pytest.raises(CommbankParseError):
        parse_commbank_text("not a statement")


def test_identical_same_day_rows_get_distinct_stable_ids():
    text = make_text(
        "16 Sep 2026 Base St Kilda VI AUS -$9.25 $90.75\n"
        "16 Sep 2026 Base St Kilda VI AUS -$9.25 $81.50\n"
    )
    a = parse_commbank_text(text)
    _assign_ids(a)
    b = parse_commbank_text(text)
    _assign_ids(b)
    assert a[0]["id"] != a[1]["id"]
    assert [t["id"] for t in a] == [t["id"] for t in b]
    assert a[0]["id"].startswith("CBA-")
