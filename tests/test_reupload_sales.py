"""Test re-upload sales behavior: previous sales should be deleted/reverted before new insert so inventory doesn't accumulate."""
import asyncio
import pytest

from db import models


def test_reupload_sales_inventory_idempotent(tmp_path):
    async def _run():
        db_file = tmp_path / "test.db"
        db_path = str(db_file)
        await models.init_db(db_path)

        # create staff and set initial inventory
        username = "reupload_user"
        name = "Reupload User"
        staff_id = await models.ensure_staff(db_path, username, name)
        # set inventory to known values
        await models.update_inventory(db_path, username, {'sim': 10, 'swap': 5, 'credit_50': 20, 'credit_100': 10})

        report_date = "2025-10-26"
        # entries: one sim (gsm), one credit_50 of 2, one recharge amount
        entries = [
            {'item_code': 'sim', 'number': '123456789', 'gsm_number': '123456789', 'recharge_amount': 0.0},
            {'item_code': 'credit_50', 'number': 2, 'recharge_amount': 0.0},
            {'item_code': 'recharge', 'number': 0, 'recharge_amount': 150.0},
        ]

        # First upload: delete previous (should be none) then insert
        await models.delete_sales_for_staff_date(db_path, staff_id, report_date)
        res1 = await models.insert_sales_and_update_inventory(db_path, staff_id, report_date, entries)
        inv_after_first = await models.get_inventory(db_path, staff_id)

        # Second upload: simulate re-upload: delete previous then insert
        await models.delete_sales_for_staff_date(db_path, staff_id, report_date)
        res2 = await models.insert_sales_and_update_inventory(db_path, staff_id, report_date, entries)
        inv_after_second = await models.get_inventory(db_path, staff_id)

        # Inventory after second upload should equal inventory after first upload (idempotent)
        assert inv_after_first['sim'] == inv_after_second['sim']
        assert inv_after_first['credit_50'] == inv_after_second['credit_50']
        assert inv_after_first['credit_100'] == inv_after_second['credit_100']
        assert inv_after_first['swap'] == inv_after_second['swap']

        # Also ensure sales table only contains the expected rows for the date
        rows = await models.get_sales_by_staff_date(db_path, username, report_date)
        # Should contain at least sim and credit_50 rows
        assert len(rows) >= 2
        # There should be no duplicated sim rows
        sim_rows = [r for r in rows if (r['item_code'] or '').lower() in ('sim','simcard','sim_card')]
        assert len(sim_rows) == 1

    asyncio.run(_run())
