import os
import asyncio
import pandas as pd
import io

from db import models

DB_PATH = "test_db_credit.db"


def setup_module(module):
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    asyncio.run(models.init_db(DB_PATH))


def teardown_module(module):
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)


async def _prepare_staff_and_inventory():
    await models.ensure_staff(DB_PATH, "tt", "Tester")
    # seed inventories
    await models.add_stock(DB_PATH, "tt", "sim", 20)
    await models.add_stock(DB_PATH, "tt", "swap", 20)
    await models.add_stock(DB_PATH, "tt", "credit_50", 10)
    await models.add_stock(DB_PATH, "tt", "credit_100", 10)


def test_credit_deduction_and_delete_by_staff_date():
    asyncio.run(_test_flow())


async def _test_flow():
    await _prepare_staff_and_inventory()
    # build an excel with one row that has sim + credit50 + credit100
    df = pd.DataFrame({
        "Number": [777123456],
        "Recharge": [50.0],
        "item_code": ["SIM"],
        "Credit_50": [2],
        "Credit_100": [3],
        "Notes": ["test"]
    })
    bio = io.BytesIO()
    df.to_excel(bio, index=False)
    bio.seek(0)
    from utils.excel_utils import parse_sales_excel
    entries, errs, regs = parse_sales_excel(bio.read(), "2025-10-19", "Tester")
    assert not errs
    staff = await models.get_staff_by_username(DB_PATH, "tt")
    res = await models.insert_sales_and_update_inventory(DB_PATH, staff["id"], "2025-10-19", entries)
    # check that sim decreased by 1 and credits decreased by 2 and 3 respectively
    inv = await models.view_stock_by_staff(DB_PATH, "tt")
    assert inv["sim"] == 19
    assert inv["credit_50"] == 8
    assert inv["credit_100"] == 7

    # delete all sales for that staff/date
    deleted = await models.delete_sales_for_staff_date(DB_PATH, staff["id"], "2025-10-19")
    assert deleted > 0
    # inventory should be reverted
    inv2 = await models.view_stock_by_staff(DB_PATH, "tt")
    assert inv2["sim"] == 20
    assert inv2["credit_50"] == 10
    assert inv2["credit_100"] == 10
