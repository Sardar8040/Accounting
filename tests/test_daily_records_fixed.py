"""Tests for daily_totals and daily_regs "last upload wins" logic.

Synchronous wrappers call asyncio.run to avoid anyio backend parametrization issues
during the project's pytest runs.
"""
import asyncio
from datetime import datetime, timedelta

from db import models


def test_daily_totals_last_upload_wins(tmp_path):
    async def _run():
        db_file = tmp_path / "test.db"
        db_path = str(db_file)
        await models.init_db(db_path)

        # Insert initial total
        date = "2025-10-25"
        shop_id = 1
        await models.insert_daily_total(db_path, date, shop_id, 1000.0)

        # Get totals - should see just the first insert
        totals = await models.get_daily_totals(db_path, date, shop_id)
        assert len(totals) == 1
        assert totals[0]["total_amount"] == 1000.0

        # Insert new total for same date/shop - should replace
        await models.insert_daily_total(db_path, date, shop_id, 2000.0)

        # Verify only new value exists
        totals = await models.get_daily_totals(db_path, date, shop_id)
        assert len(totals) == 1
        assert totals[0]["total_amount"] == 2000.0

        # Insert for different shop - should not affect first shop
        await models.insert_daily_total(db_path, date, shop_id + 1, 3000.0)

        # Check both exist
        all_totals = await models.get_daily_totals(db_path, date)
        assert len(all_totals) == 2
        totals_by_shop = {t["shop_id"]: t["total_amount"] for t in all_totals}
        assert totals_by_shop[shop_id] == 2000.0
        assert totals_by_shop[shop_id + 1] == 3000.0

        # Test NULL shop_id (grand totals)
        await models.insert_daily_total(db_path, date, None, 5000.0)
        await models.insert_daily_total(db_path, date, None, 6000.0)  # Should replace

        nulls = [t for t in await models.get_daily_totals(db_path, date) if t["shop_id"] is None]
        assert len(nulls) == 1
        assert nulls[0]["total_amount"] == 6000.0

    asyncio.run(_run())


def test_daily_regs_last_upload_wins(tmp_path):
    async def _run():
        db_file = tmp_path / "test.db"
        db_path = str(db_file)
        await models.init_db(db_path)

        # Create test staff
        staff_id = await models.ensure_staff(db_path, "testuser", "Test User")
        date = "2025-10-25"

        # Insert initial registration count
        await models.insert_daily_regs(db_path, staff_id, date, 10)

        # Get regs between dates - should see initial count
        regs = await models.get_regs_between(db_path, date, date)
        assert len(regs) == 1
        assert regs[0]["total_regs"] == 10

        # Insert new count for same staff/date - should update
        await models.insert_daily_regs(db_path, staff_id, date, 20)

        # Verify only new value exists
        regs = await models.get_regs_between(db_path, date, date)
        assert len(regs) == 1
        assert regs[0]["total_regs"] == 20

        # Insert for different staff - should not affect first staff
        staff_id2 = await models.ensure_staff(db_path, "testuser2", "Test User 2")
        await models.insert_daily_regs(db_path, staff_id2, date, 30)

        # Check both exist
        regs = await models.get_regs_between(db_path, date, date)
        assert len(regs) == 2
        regs_by_staff = {r["staff_id"]: r["total_regs"] for r in regs}
        assert regs_by_staff[staff_id] == 20
        assert regs_by_staff[staff_id2] == 30

        # Test different date - should not affect original date
        tomorrow = datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)
        tomorrow_str = tomorrow.strftime("%Y-%m-%d")
        await models.insert_daily_regs(db_path, staff_id, tomorrow_str, 40)

        # Check original date unchanged
        regs = await models.get_regs_between(db_path, date, date)
        assert len([r for r in regs if r["staff_id"] == staff_id]) == 1
        assert [r for r in regs if r["staff_id"] == staff_id][0]["total_regs"] == 20

        # Update tomorrow's count - should replace
        await models.insert_daily_regs(db_path, staff_id, tomorrow_str, 50)
        regs = await models.get_regs_between(db_path, tomorrow_str, tomorrow_str)
        assert len([r for r in regs if r["staff_id"] == staff_id]) == 1
        assert [r for r in regs if r["staff_id"] == staff_id][0]["total_regs"] == 50

    asyncio.run(_run())
