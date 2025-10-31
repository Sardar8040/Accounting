import os
import asyncio
import io
import pandas as pd
from db import models
from utils.excel_utils import parse_pickup_excel, parse_sales_excel

DB_PATH = "test_db_sim_batches.db"


def setup_module(module):
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    asyncio.run(models.init_db(DB_PATH))


def teardown_module(module):
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)


def make_pickup_bytes(df: pd.DataFrame) -> bytes:
    bio = io.BytesIO()
    df.to_excel(bio, index=False)
    bio.seek(0)
    return bio.read()


def test_import_pickup_and_duplicates():
    df = pd.DataFrame({"Carton #": [1,1], "BOX #": [10,10], "GSM NUMBER": ["749600001","749600002"], "ICCID": ["iccid1","iccid2"], "Type": ["SIM","SIM"]})
    b = make_pickup_bytes(df)
    res = asyncio.run(models.insert_pickup_list(DB_PATH, b, "testfile.xlsx", "admin"))
    assert res["inserted"] == 2
    # import again to cause duplicates
    res2 = asyncio.run(models.insert_pickup_list(DB_PATH, b, "testfile.xlsx", "admin"))
    assert res2["inserted"] == 0
    assert res2["duplicates"] >= 2


def test_transfer_box_range_and_journal():
    # ensure admin user
    asyncio.run(models.ensure_staff(DB_PATH, "admin", "Admin"))
    # add a few sim batches
    df = pd.DataFrame({"Carton #": [2,2,2], "BOX #": [54,55,56], "GSM NUMBER": ["749653372","749653387","749654035"], "ICCID": ["a","b","c"], "Type": ["SIM","SIM","SIM"]})
    b = make_pickup_bytes(df)
    asyncio.run(models.insert_pickup_list(DB_PATH, b, "testfile2.xlsx", "admin"))
    # transfer box range 54-58 to Teleshop_A
    res = asyncio.run(models.transfer_sims_by_clause(DB_PATH, "box_no BETWEEN ? AND ?", ["54","58"], "Shop:Teleshop_A", "admin"))
    assert res["moved"] == 3
    # check status updated
    conn = models.get_connection(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM sim_batches WHERE current_location = ?", ("Shop:Teleshop_A",))
    cnt = cur.fetchone()[0]
    conn.close()
    assert cnt == 3
    # check journal entry exists
    conn = models.get_connection(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT * FROM inventory_journal WHERE source = 'backoffice' AND change_type = 'backoffice_transfer'")
    rows = cur.fetchall()
    conn.close()
    assert rows


def test_sim_sale_updates_status():
    # simulate sale marking: update sim_batches for a gsm to sold
    asyncio.run(models.ensure_staff(DB_PATH, "seller", "Seller"))
    # mark a GSM as sold (simulate existing sale flow hook)
    conn = models.get_connection(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT gsm_number FROM sim_batches LIMIT 1")
    row = cur.fetchone()
    if not row:
        # insert one SIM so the test can run standalone
        conn.execute("INSERT INTO sim_batches (carton_no, box_no, gsm_number, iccid, type, note) VALUES (?, ?, ?, ?, ?, ?)", ("99", "99", "900000001", "iccidx", "SIM", "test"))
        conn.commit()
        cur.execute("SELECT gsm_number FROM sim_batches LIMIT 1")
        row = cur.fetchone()
    assert row
    gsm = row[0]
    conn.close()
    # simulate update that should be performed by sales flow
    def _fn():
        conn2 = models.get_connection(DB_PATH)
        cur2 = conn2.cursor()
        cur2.execute("UPDATE sim_batches SET status = 'sold', current_location = ? WHERE gsm_number = ?", ("Employee:seller", gsm))
        conn2.commit()
        conn2.close()
    asyncio.run(asyncio.to_thread(_fn))
    res = asyncio.run(models.sim_status(DB_PATH, 'gsm', gsm))
    assert res.get('status') == 'sold'


def test_sales_upload_marks_sim_sold():
    # Prepare: ensure seller and insert a sim batch with known GSM
    asyncio.run(models.ensure_staff(DB_PATH, "seller2", "Seller2"))
    # insert sim
    conn = models.get_connection(DB_PATH)
    cur = conn.cursor()
    cur.execute("INSERT INTO sim_batches (carton_no, box_no, gsm_number, iccid, type) VALUES (?, ?, ?, ?, ?)", ("5", "5", "900000123", "iccidx", "SIM"))
    conn.commit()
    conn.close()
    # build a sales Excel that includes GSM NUMBER column matching the SIM
    df = pd.DataFrame({"Number": [1], "Recharge": [0], "item_code": ["SIM"], "GSM NUMBER": ["900000123"], "Notes": [""]})
    bio = io.BytesIO()
    df.to_excel(bio, index=False)
    bio.seek(0)
    entries, errs, regs = parse_sales_excel(bio.read(), "2025-10-15", "Seller2")
    assert not errs
    # ensure staff id and give them a SIM in inventory so sale can be processed
    staff = asyncio.run(models.get_staff_by_username(DB_PATH, "seller2"))
    asyncio.run(models.add_stock(DB_PATH, "seller2", "sim", 1))
    # call existing insertion function which now contains the safe sim marking hook
    asyncio.run(models.insert_sales_and_update_inventory(DB_PATH, staff["id"], "2025-10-15", entries))
    # assert sim marked sold
    res = asyncio.run(models.sim_status(DB_PATH, 'gsm', '900000123'))
    assert res.get('status') == 'sold'