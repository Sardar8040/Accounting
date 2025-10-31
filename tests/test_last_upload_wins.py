import os
import asyncio
import pytest
from db import models

DB_PATH = "test_last_upload_wins.db"


async def setup_db():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    await models.init_db(DB_PATH)


@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.mark.asyncio
async def test_repeated_simple_sim_uploads():
    await setup_db()
    # create staff and seed inventory
    sid = await models.ensure_staff(DB_PATH, "u1", "User One")
    # seed 10 SIMs
    await models.add_stock(DB_PATH, "u1", "sim", 10)

    # sequence of uploads: 2, 3, 1, 4 sims
    seq = [2, 3, 1, 4]
    date = "2025-10-31"
    initial = 10
    for expected_count in seq:
        # build entries for this upload
        entries = []
        for i in range(expected_count):
            gsm = str(750000000 + i)
            entries.append({
                "item_code": "SIM",
                "number": gsm,
                "recharge_amount": 100.0,
                "notes": "",
                "gsm_number": gsm,
            })
        res = await models.insert_sales_and_update_inventory(DB_PATH, sid, date, entries)
        # verify insertion summary roughly matches
        assert isinstance(res, dict)
        # verify inventory final equals initial - expected_count
        info = await models.view_stock_by_staff(DB_PATH, "u1")
        assert info["sim"] == initial - expected_count

    # finally, ensure the DB has only the last upload's sales for that date
    rows = await models.get_sales_by_staff_date(DB_PATH, "u1", date)
    assert len(rows) == seq[-1]


@pytest.mark.asyncio
async def test_duplicate_gsm_and_multiple_uploads_do_not_double_count():
    await setup_db()
    sid = await models.ensure_staff(DB_PATH, "u2", "User Two")
    await models.add_stock(DB_PATH, "u2", "sim", 5)
    date = "2025-10-31"

    # First upload: 2 GSMs
    entries1 = [
        {"item_code": "SIM", "number": "750100001", "gsm_number": "750100001"},
        {"item_code": "SIM", "number": "750100002", "gsm_number": "750100002"},
    ]
    await models.insert_sales_and_update_inventory(DB_PATH, sid, date, entries1)
    info1 = await models.view_stock_by_staff(DB_PATH, "u2")
    assert info1["sim"] == 3

    # Second upload: same two GSMs + one new GSM
    entries2 = [
        {"item_code": "SIM", "number": "750100001", "gsm_number": "750100001"},
        {"item_code": "SIM", "number": "750100002", "gsm_number": "750100002"},
        {"item_code": "SIM", "number": "750100003", "gsm_number": "750100003"},
    ]
    await models.insert_sales_and_update_inventory(DB_PATH, sid, date, entries2)
    info2 = await models.view_stock_by_staff(DB_PATH, "u2")
    # should be initial 5 - 3 = 2
    assert info2["sim"] == 2

    # Third upload: empty upload (no entries) should result in no sales and inventory = initial
    # Simulate empty upload by passing empty entries list -> function should skip if entries empty
    await models.insert_sales_and_update_inventory(DB_PATH, sid, date, [])
    info3 = await models.view_stock_by_staff(DB_PATH, "u2")
    # Since we passed empty entries, last-upload-wins semantics mean previous sales are deleted
    # and inventory should be restored to initial (5)
    assert info3["sim"] == 5


@pytest.mark.asyncio
async def test_many_reuploads_stress():
    await setup_db()
    sid = await models.ensure_staff(DB_PATH, "u3", "User Three")
    await models.add_stock(DB_PATH, "u3", "sim", 20)
    date = "2025-10-31"

    import random
    random.seed(0)

    last_count = 0
    for _ in range(10):
        cnt = random.randint(0, 5)
        last_count = cnt
        entries = []
        for i in range(cnt):
            gsm = str(760000000 + i)
            entries.append({"item_code": "SIM", "number": gsm, "gsm_number": gsm})
        await models.insert_sales_and_update_inventory(DB_PATH, sid, date, entries)
        info = await models.view_stock_by_staff(DB_PATH, "u3")
        assert info["sim"] == 20 - cnt

    # final check: matches last_count
    info_final = await models.view_stock_by_staff(DB_PATH, "u3")
    assert info_final["sim"] == 20 - last_count
