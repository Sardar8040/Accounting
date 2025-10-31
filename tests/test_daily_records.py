"""Tests for daily_totals and daily_regs "last upload wins" logic.

These tests use synchronous wrappers (asyncio.run) to avoid anyio backend
differences during CI / local runs.
"""

import asyncio
from datetime import datetime, timedelta

from db import models


def test_daily_totals_last_upload_wins(tmp_path):
    async def _run():
        db_path = str(tmp_path / "test.db")
        await models.init_db(db_path)

        date = "2025-10-25"
        shop_id = 1

        # Insert initial total
        await models.insert_daily_total(db_path, date, shop_id, 1000.0)
        totals = await models.get_daily_totals(db_path, date, shop_id)
        assert len(totals) == 1
        assert totals[0]["total_amount"] == 1000.0

        # Re-insert for same date/shop -> should replace existing
        await models.insert_daily_total(db_path, date, shop_id, 2000.0)
        totals = await models.get_daily_totals(db_path, date, shop_id)
        assert len(totals) == 1
        assert totals[0]["total_amount"] == 2000.0

        # Insert for another shop -> both should exist
        await models.insert_daily_total(db_path, date, shop_id + 1, 3000.0)
        all_totals = await models.get_daily_totals(db_path, date)
        assert len(all_totals) == 2
        totals_by_shop = {t["shop_id"]: t["total_amount"] for t in all_totals}
        assert totals_by_shop[shop_id] == 2000.0
        assert totals_by_shop[shop_id + 1] == 3000.0

        # Grand total (NULL shop_id) should also replace on re-insert
        await models.insert_daily_total(db_path, date, None, 5000.0)
        await models.insert_daily_total(db_path, date, None, 6000.0)
        nulls = [t for t in await models.get_daily_totals(db_path, date) if t["shop_id"] is None]
        assert len(nulls) == 1
        assert nulls[0]["total_amount"] == 6000.0

    asyncio.run(_run())


def test_daily_regs_last_upload_wins(tmp_path):
    async def _run():
        db_path = str(tmp_path / "test.db")
        await models.init_db(db_path)

        staff_id = await models.ensure_staff(db_path, "testuser", "Test User")
        date = "2025-10-25"

        # Insert initial regs
        await models.insert_daily_regs(db_path, staff_id, date, 10)
        regs = await models.get_regs_between(db_path, date, date)
        assert len(regs) == 1
        assert regs[0]["total_regs"] == 10

        # Re-insert for same staff/date -> should replace
        await models.insert_daily_regs(db_path, staff_id, date, 20)
        regs = await models.get_regs_between(db_path, date, date)
        assert len(regs) == 1
        assert regs[0]["total_regs"] == 20

        # Insert for another staff -> both should exist
        staff_id2 = await models.ensure_staff(db_path, "testuser2", "Test User 2")
        await models.insert_daily_regs(db_path, staff_id2, date, 30)
        regs = await models.get_regs_between(db_path, date, date)
        assert len(regs) == 2
        regs_by_staff = {r["staff_id"]: r["total_regs"] for r in regs}
        assert regs_by_staff[staff_id] == 20
        assert regs_by_staff[staff_id2] == 30

        # Different date should be independent
        tomorrow = datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)
        tomorrow_str = tomorrow.strftime("%Y-%m-%d")
        await models.insert_daily_regs(db_path, staff_id, tomorrow_str, 40)
        regs = await models.get_regs_between(db_path, date, date)
        assert len([r for r in regs if r["staff_id"] == staff_id]) == 1
        assert [r for r in regs if r["staff_id"] == staff_id][0]["total_regs"] == 20

        # Updating tomorrow should replace
        await models.insert_daily_regs(db_path, staff_id, tomorrow_str, 50)
        regs = await models.get_regs_between(db_path, tomorrow_str, tomorrow_str)
        assert len([r for r in regs if r["staff_id"] == staff_id]) == 1
        assert [r for r in regs if r["staff_id"] == staff_id][0]["total_regs"] == 50

    asyncio.run(_run())

