from datetime import timedelta
from decimal import Decimal, InvalidOperation

import pytest

from database_writer import validate_trade


def valid_trade(**overrides):
    trade = {
        "tradeId": "P1-test-001",
        "producerId": "P1",
        "symbol": "EURUSD",
        "side": "BUY",
        "notional": 5_000_000,
        "status": "NEW",
        "createdAt": "2026-10-14T12:00:00+00:00",
    }
    trade.update(overrides)
    return trade


def test_valid_trade_is_parsed():
    result = validate_trade(valid_trade())

    assert result[0] == "P1-test-001"
    assert result[1] == "P1"
    assert result[2] == "EURUSD"
    assert result[3] == "BUY"
    assert result[4] == Decimal("5000000")
    assert result[5] == "NEW"
    assert result[6].utcoffset() == timedelta(0)


def test_lowercase_side_is_normalized():
    result = validate_trade(valid_trade(side="sell"))

    assert result[3] == "SELL"


@pytest.mark.parametrize(
    ("event", "expected_exception"),
    [
        (None, ValueError),
        (valid_trade(tradeId=""), ValueError),
        (valid_trade(side="HOLD"), ValueError),
        (valid_trade(notional=-100), ValueError),
        (valid_trade(notional="NaN"), ValueError),
        (valid_trade(notional="not-a-number"), InvalidOperation),
        (valid_trade(createdAt="not-a-timestamp"), ValueError),
        (
            {
                "tradeId": "P1-test-002",
                "producerId": "P1",
                "symbol": "EURUSD",
                "side": "BUY",
                "notional": 1000,
                "status": "NEW",
            },
            ValueError,
        ),
    ],
)
def test_invalid_trade_is_rejected(event, expected_exception):
    with pytest.raises(expected_exception):
        validate_trade(event)
