import os
import asyncio
from typing import Optional
from pathlib import Path

from telegram import InputFile

from . import models


async def _run_db(fn, *a, **kw):
    return await asyncio.to_thread(fn, *a, **kw)


async def set_pending_upload(db_path: str, admin_id: str, target_username: str, ttl_seconds: int = 300) -> bool:
    return await _run_db(models.set_admin_pending_upload, db_path, str(admin_id), target_username, ttl_seconds)


async def pop_pending_upload(db_path: str, admin_id: str) -> Optional[str]:
    return await _run_db(models.pop_admin_pending_upload, db_path, str(admin_id))


async def upload_for_cmd(update, context) -> None:
    """Admin marks next uploaded file to be processed for another username."""
    if len(context.args) < 1:
        await update.message.reply_text("Usage: /upload_for <username>")
        return
    target = context.args[0]
    db_path = os.getenv('DB_PATH', 'teleshop.db')
    staff = await models.get_staff_by_username(db_path, target)
    if not staff:
        await update.message.reply_text(f"⚠️ Employee not found: {target}")
        return
    if not staff.get('chat_id'):
        await update.message.reply_text(f"⚠️ Employee {target} is not linked to Telegram (no chat_id). Upload cancelled.")
        return
    ok = await set_pending_upload(db_path, update.effective_user.id, target)
    if not ok:
        await update.message.reply_text("⚠️ Failed to register pending upload. Try again.")
        return
    await update.message.reply_text(f"📤 Ready — please upload the sales file now. It will be processed for user '{target}'. This pending request expires in 5 minutes.")


async def backup_db_cmd(update, context) -> None:
    """Create a snapshot backup of the DB (stored in backups/) and send it to the invoking admin.

    Usage: /db or /backup_db
    """
    db_path = os.getenv('DB_PATH', 'teleshop.db')
    backup_dir = Path(os.getenv('BACKUP_DIR', 'backups'))
    try:
        backup_dir.mkdir(parents=True, exist_ok=True)
        snapshot = await _run_db(models.create_db_snapshot, db_path, str(backup_dir))
        if not snapshot:
            await update.message.reply_text(f"⚠️ Failed to create DB snapshot.")
            return
        try:
            await context.bot.send_document(chat_id=update.effective_user.id, document=InputFile(str(snapshot), filename=Path(snapshot).name))
            await update.message.reply_text(f"✅ Database snapshot created and sent: {Path(snapshot).name}")
        except Exception as ex:
            await update.message.reply_text(f"⚠️ Failed to send DB snapshot: {ex}")
    except Exception as ex:
        await update.message.reply_text(f"⚠️ Backup failed: {ex}")


async def restore_db_cmd(update, context) -> None:
    """Restore the live DB from a snapshot stored in backups/.

    Usage: /restore_db <backup_filename>
    NOTE: This operation overwrites the live DB and should only be used when necessary.
    """
    if len(context.args) < 1:
        await update.message.reply_text("Usage: /restore_db <backup_filename>")
        return
    filename = context.args[0]
    backup_dir = Path(os.getenv('BACKUP_DIR', 'backups'))
    candidate = backup_dir / filename
    if not candidate.exists():
        await update.message.reply_text(f"⚠️ Backup file not found: {candidate}")
        return
    db_path = os.getenv('DB_PATH', 'teleshop.db')
    try:
        await _run_db(models.restore_db_from_snapshot, db_path, str(candidate))
        await update.message.reply_text(f"✅ Database restored from {filename}.")
    except Exception as ex:
        await update.message.reply_text(f"⚠️ Restore failed: {ex}")


# Re-export small helper for handlers
async def pop_pending_for_admin(db_path: str, admin_id: str) -> Optional[str]:
    return await pop_pending_upload(db_path, admin_id)
        