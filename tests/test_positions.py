from decimal import Decimal

from dashboard.app import build_positions_response


def test_positions_are_aggregated_from_all_database_rows():
    rows = [
        ("EURUSD", 2, 1, Decimal("100.50"), Decimal("40.25"), 3),
        ("USDJPY", 1, 3, Decimal("90.00"), Decimal("150.00"), 4),
    ]

    result = build_positions_response(rows)

    assert result["scope"] == "all persisted BUY/SELL trades in PostgreSQL"
    assert result["tradeCount"] == 7

    eurusd = next(p for p in result["positions"] if p["symbol"] == "EURUSD")
    assert eurusd["buyTrades"] == 2
    assert eurusd["sellTrades"] == 1
    assert eurusd["buyNotional"] == 100.50
    assert eurusd["sellNotional"] == 40.25
    assert eurusd["netNotional"] == 60.25
    assert eurusd["tradeCount"] == 3

    usdjpy = next(p for p in result["positions"] if p["symbol"] == "USDJPY")
    assert usdjpy["netNotional"] == -60.00


def test_empty_database_returns_empty_positions():
    result = build_positions_response([])

    assert result["tradeCount"] == 0
    assert result["positions"] == []
