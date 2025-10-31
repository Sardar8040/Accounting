import os
import asyncio
from db import models

DB_PATH = "test_db_backoffice.db"


def setup_module(module):
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    asyncio.run(models.init_db(DB_PATH))


def teardown_module(module):
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)


def test_backoffice_add_and_transfer():
    db = DB_PATH
    # ensure admin exists
    asyncio.run(models.ensure_staff(db, "admin", "Admin"))
    # add backoffice stock
    ok = asyncio.run(models.add_backoffice_stock(db, "sim", 20))
    assert ok
    qty = asyncio.run(models.get_backoffice_quantity(db, "sim"))
    assert qty >= 20
    # transfer to admin
    ok2 = asyncio.run(models.transfer_backoffice(db, "sim", 5, to_username="admin"))
    assert ok2
    # admin inventory should increase
    info = asyncio.run(models.view_stock_by_staff(db, "admin"))
    assert info.get("sim", 0) >= 5
    # backoffice reduced
    qty2 = asyncio.run(models.get_backoffice_quantity(db, "sim"))
    assert qty2 == qty - 5
    # journal entries exist
    conn = models.get_connection(db)
    cur = conn.cursor()
    cur.execute("SELECT * FROM inventory_journal WHERE item = ?", ("sim",))
    rows = cur.fetchall()
    conn.close()
    assert rows