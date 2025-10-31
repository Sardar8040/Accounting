import os
import asyncio
from db import models

DB_PATH = "test_db_total_regs.db"


def setup_module(module):
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    asyncio.run(models.init_db(DB_PATH))


def teardown_module(module):
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)


def test_insert_and_query_daily_regs():
    async def _run():
        # create staff
        sid = await models.ensure_staff(DB_PATH, "tester1", "Tester One")
        sid2 = await models.ensure_staff(DB_PATH, "tester2", "Tester Two")
        # insert daily regs
        await models.insert_daily_regs(DB_PATH, sid, "2025-10-01", 5)
        await models.insert_daily_regs(DB_PATH, sid2, "2025-10-01", 3)
        # query between
        rows = await models.get_regs_between(DB_PATH, "2025-10-01", "2025-10-01")
        assert any(r['username'] == 'tester1' and int(r['total_regs']) == 5 for r in rows)
        assert any(r['username'] == 'tester2' and int(r['total_regs']) == 3 for r in rows)

    asyncio.run(_run())
