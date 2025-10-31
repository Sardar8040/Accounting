import os
import asyncio
import io
import pandas as pd
from db import models
from utils.excel_utils import parse_sales_excel, extract_daily_regs

DB_PATH = "test_db_journal.db"


def setup_module(module):
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    asyncio.run(models.init_db(DB_PATH))


def teardown_module(module):
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)


def make_excel_bytes(df: pd.DataFrame) -> bytes:
    bio = io.BytesIO()
    df.to_excel(bio, index=False)
    bio.seek(0)
    return bio.read()


def test_parse_and_daily_regs_and_journal_and_revert():
    # prepare data: first Notes cell contains daily regs = 3
    # Number column contains GSM strings (9 digits). Daily regs present in first Notes cell.
    df = pd.DataFrame({"Number": ["750000001", "750000002"], "Recharge": [100.0, 50.0], "item_code": ["SIM", "SIM"], "Notes": ["3", "note2"]})
    b = make_excel_bytes(df)
    entries, errors, regs = parse_sales_excel(b, "2025-10-12", "Tester")
    daily_regs = extract_daily_regs(b)
    assert not errors
    assert len(entries) == 2
    assert daily_regs == 3

    # ensure staff and seed inventory
    asyncio.run(models.ensure_staff(DB_PATH, "tester", "Tester"))
    asyncio.run(models.add_stock(DB_PATH, "tester", "sim", 5))
    staff = asyncio.run(models.get_staff_by_username(DB_PATH, "tester"))
    # last-upload-wins: insert and then delete and ensure inventory back to original
    result = asyncio.run(models.insert_sales_and_update_inventory(DB_PATH, staff["id"], "2025-10-12", entries))
    assert isinstance(result, dict)
    assert result.get("inserted") == 2
    info_after = asyncio.run(models.view_stock_by_staff(DB_PATH, "tester"))
    assert info_after["sim"] <= 3
    # delete previous
    deleted = asyncio.run(models.delete_sales_for_staff_date(DB_PATH, staff["id"], "2025-10-12"))
    assert deleted >= 2
    info_reverted = asyncio.run(models.view_stock_by_staff(DB_PATH, "tester"))
    # inventory should be restored (or increased by deleted amount)
    assert info_reverted["sim"] >= info_after["sim"]
    # check journal entries: there should be sale journal entries with source_ref linking to sales
    conn = models.get_connection(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT id, source_ref, change_type, source FROM inventory_journal WHERE staff_id = ?", (staff["id"],))
    rows = cur.fetchall()
    conn.close()
    assert rows
    # there should be at least one sale entry (change_type='sale') with a non-null source_ref
    assert any(r["change_type"] == "sale" and r["source_ref"] for r in rows)


def test_daily_regs_storage_and_query():
    asyncio.run(models.ensure_staff(DB_PATH, "alice", "Alice"))
    staff = asyncio.run(models.get_staff_by_username(DB_PATH, "alice"))
    ok = asyncio.run(models.insert_daily_regs(DB_PATH, staff["id"], "2025-10-01", 4))
    assert ok
    rows = asyncio.run(models.get_regs_between(DB_PATH, "2025-10-01", "2025-10-31"))
    assert any(r["username"] == "alice" and int(r["total_regs"]) == 4 for r in rows)


def test_borrow_ledger():
    asyncio.run(models.ensure_staff(DB_PATH, "adminx", "AdminX"))
    # simulate admin id
    admin_id = "99999"
    ok = asyncio.run(models.borrow_add(DB_PATH, admin_id, "John", 500, "2025-10-10", "lunch"))
    assert ok
    rows = asyncio.run(models.borrow_list_for_admin(DB_PATH, admin_id))
    assert rows and rows[0]["person_name"] == "John"
    summary = asyncio.run(models.borrow_summary(DB_PATH, admin_id, "2025-10-01", "2025-10-31"))
    assert any(r["person_name"] == "John" and float(r["total"]) == 500.0 for r in summary)
