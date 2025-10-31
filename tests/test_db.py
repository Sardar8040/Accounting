import os
import asyncio
import pathlib
import pandas as pd

from db import models


DB_PATH = "test_db_unit.db"


def setup_module(module):
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    asyncio.run(models.init_db(DB_PATH))


def teardown_module(module):
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)


def test_add_and_remove_stock_and_inventory_summary():
    asyncio.run(_test_stock_flow())


async def _test_stock_flow():
    await models.ensure_staff(DB_PATH, "alice", "Alice")
    await models.add_stock(DB_PATH, "alice", "sim", 10)
    await models.add_stock(DB_PATH, "alice", "swap", 5)
    s = await models.inventory_summary(DB_PATH)
    assert s["sim"] >= 10
    assert s["swap"] >= 5
    ok = await models.remove_stock(DB_PATH, "alice", "sim", 3)
    assert ok
    s2 = await models.inventory_summary(DB_PATH)
    assert s2["sim"] == s["sim"] - 3


def test_insert_sales_and_delete():
    asyncio.run(_test_sales_flow())


async def _test_sales_flow():
    await models.ensure_staff(DB_PATH, "bob", "Bob")
    # seed inventory
    await models.add_stock(DB_PATH, "bob", "sim", 5)
    # Number column contains GSM string; quantity is implied by rows
    df = pd.DataFrame({"Number": ["750000002"], "Recharge": [100.0], "item_code": ["SIM"], "Notes": ["s"]})
    bio = io = None
    import io as _io
    bio = _io.BytesIO()
    df.to_excel(bio, index=False)
    bio.seek(0)
    from utils.excel_utils import parse_sales_excel
    entries, errs, regs = parse_sales_excel(bio.read(), "2025-10-12", "Bob")
    assert not errs
    staff = await models.get_staff_by_username(DB_PATH, "bob")
    await models.insert_sales_and_update_inventory(DB_PATH, staff["id"], "2025-10-12", entries)
    # check inventory decreased by 1 (quantity is inferred by rows)
    info = await models.view_stock_by_staff(DB_PATH, "bob")
    assert info["sim"] <= 4
    # get all sales and delete one
    rows = await models.get_all_sales_by_date(DB_PATH, "2025-10-12")
    assert rows
    sid = rows[0]["id"]
    ok = await models.delete_sale(DB_PATH, sid)
    assert ok


def test_set_admin():
    asyncio.run(models.ensure_staff(DB_PATH, "carol", "Carol"))
    ok = asyncio.run(models.set_admin(DB_PATH, "carol", True))
    assert ok
    assert asyncio.run(models.is_admin_by_username(DB_PATH, "carol"))
