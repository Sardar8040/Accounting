import io
import pandas as pd
from utils.excel_utils import parse_sales_excel

def make_excel_bytes(df: pd.DataFrame) -> bytes:
    bio = io.BytesIO()
    df.to_excel(bio, index=False)
    bio.seek(0)
    return bio.read()

def test_valid_gsm_number():
    """Test that valid 9-digit GSM numbers are accepted"""
    df = pd.DataFrame({
        "Number": ["123456789"],
        "Recharge": [100.0], 
        "item_code": ["SIM"]
    })
    b = make_excel_bytes(df)
    entries, errors, _ = parse_sales_excel(b, "2025-10-12", "Tester")
    assert not errors
    assert len(entries) == 1
    assert entries[0]["gsm_number"] == "123456789"

def test_invalid_gsm_length():
    """Test that GSM numbers with wrong length are rejected"""
    df = pd.DataFrame({
        "Number": ["12345", "1234567890"],
        "Recharge": [100.0, 200.0], 
        "item_code": ["SIM", "SIM"]
    })
    b = make_excel_bytes(df)
    entries, errors, _ = parse_sales_excel(b, "2025-10-12", "Tester")
    assert len(errors) == 2
    assert "must be exactly 9 digits" in errors[0]
    assert "must be exactly 9 digits" in errors[1]
    assert len(entries) == 0

def test_non_numeric_gsm():
    """Test that non-numeric GSM numbers are rejected"""
    df = pd.DataFrame({
        "Number": ["ABC123456", "123-456-789"],
        "Recharge": [100.0, 200.0], 
        "item_code": ["SIM", "SIM"]
    })
    b = make_excel_bytes(df)
    entries, errors, _ = parse_sales_excel(b, "2025-10-12", "Tester")
    # After cleaning non-digit characters, 'ABC123456' -> '123456' (invalid length),
    # '123-456-789' -> '123456789' (valid). So expect one skipped row and one valid entry.
    assert len(errors) == 1
    assert "must be exactly 9 digits" in errors[0]
    assert len(entries) == 1

def test_mixed_valid_invalid():
    """Test processing of mixed valid and invalid GSM numbers"""
    df = pd.DataFrame({
        "Number": ["123456789", "1234", "987654321"],
        "Recharge": [100.0, 200.0, 300.0], 
        "item_code": ["SIM", "SIM", "SIM"]
    })
    b = make_excel_bytes(df)
    entries, errors, _ = parse_sales_excel(b, "2025-10-12", "Tester")
    assert len(errors) == 1  # Only the invalid row
    assert "must be exactly 9 digits" in errors[0]
    assert len(entries) == 2  # Two valid rows
    assert sorted([e["gsm_number"] for e in entries]) == ["123456789", "987654321"]

def test_gsm_column():
    """Test that GSM NUMBER column is also validated"""
    df = pd.DataFrame({
        "Number": [1, 2],
        "GSM NUMBER": ["123456789", "1234"],
        "Recharge": [100.0, 200.0], 
        "item_code": ["SIM", "SIM"]
    })
    b = make_excel_bytes(df)
    entries, errors, _ = parse_sales_excel(b, "2025-10-12", "Tester")
    assert len(errors) == 1
    assert "must be exactly 9 digits" in errors[0]
    assert len(entries) == 1
    assert entries[0]["gsm_number"] == "123456789"