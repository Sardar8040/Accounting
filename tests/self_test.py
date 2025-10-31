"""Quick self-test for core functionality.

Creates a temporary DB in memory (or file), inserts a staff, simulates an Excel upload via pandas-created bytes,
and verifies sales insertion and inventory changes.
"""
import io
import os
import sys
import asyncio
import pathlib
import pandas as pd

# make sure project root is on sys.path so 'db' and 'utils' packages import correctly
HERE = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))

from db import models


async def run_test():
    db_path = "test_teleshop.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    await models.init_db(db_path)
    username = "tester"
    staff_id = await models.ensure_staff(db_path, username, "Test User")

    # seed initial inventory
    await models.add_stock(db_path, username, "sim", 10)
    await models.add_stock(db_path, username, "swap", 5)
    # create a fake excel
    df = pd.DataFrame({"Number": [2, 1], "Recharge": [100.0, 50.0], "item_code": ["SIM", "SWAP"], "Notes": ["x","y"]})
    bio = io.BytesIO()
    df.to_excel(bio, index=False)
    bio.seek(0)
    # Use excel util to parse
    from utils.excel_utils import parse_sales_excel
    entries, errs = parse_sales_excel(bio.read(), "2025-10-12", "Test User")
    if errs:
        print("Parse errors:", errs)
        return
    await models.insert_sales_and_update_inventory(db_path, staff_id, "2025-10-12", entries)

    inv = await models.get_inventory(db_path, staff_id)
    print("Inventory after upload:", inv)

    # cleanup
    try:
        os.remove(db_path)
    except Exception:
        pass


if __name__ == "__main__":
    asyncio.run(run_test())
