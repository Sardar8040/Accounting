import io
import pandas as pd
from utils.excel_utils import parse_sales_excel


def make_excel_bytes(df: pd.DataFrame) -> bytes:
    bio = io.BytesIO()
    df.to_excel(bio, index=False)
    bio.seek(0)
    return bio.read()


def test_parse_sales_excel_happy_path():
    # Number column now contains GSM strings (9 digits). Quantity is inferred from rows.
    df = pd.DataFrame({"Number": ["750000001", "750000002"], "Recharge": [100.0, 50.0], "item_code": ["SIM", "SWAP"], "Notes": ["a","b"]})
    b = make_excel_bytes(df)
    entries, errors, daily_regs = parse_sales_excel(b, "2025-10-12", "Tester")
    assert not errors
    assert len(entries) == 2
    assert entries[0]["number"] == "750000001"
    assert entries[1]["recharge_amount"] == 50.0

