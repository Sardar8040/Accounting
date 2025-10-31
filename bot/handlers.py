"""Telegram bot handlers for the Teleshop system."""
from __future__ import annotations

import os
import logging
from pathlib import Path
from typing import Any
import datetime
import asyncio
import math
import dateutil.parser

from telegram import Update, InputFile
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, filters

from utils.excel_utils import parse_sales_excel
from db import models

logger = logging.getLogger(__name__)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Capture chat_id for notifications
    db_path = os.getenv("DB_PATH", "teleshop.db")
    username = update.effective_user.username or str(update.effective_user.id)
    chat_id = update.effective_chat.id
    try:
        await models.ensure_staff(db_path, username, update.effective_user.full_name)
        await models.set_staff_chat_id(db_path, username, str(chat_id))
    except Exception:
        logger.exception("Failed to record chat_id for %s", username)

    await update.message.reply_text("Welcome to Teleshop Auto Reporting Bot. Use /upload to send today's sales Excel.")


async def register_me(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """User-initiated storing of chat_id for reliable notifications."""
    db_path = os.getenv("DB_PATH", "teleshop.db")
    username = update.effective_user.username or str(update.effective_user.id)
    chat_id = update.effective_chat.id
    ok = False
    try:
        await models.ensure_staff(db_path, username, update.effective_user.full_name)
        ok = await models.set_staff_chat_id(db_path, username, str(chat_id))
    except Exception:
        logger.exception("register_me failed for %s", username)
    if ok:
        await update.message.reply_text("You have been registered for notifications.")
    else:
        await update.message.reply_text("Failed to register you. Contact admin.")


async def send_message_safe(bot, chat_id: str, text: str) -> bool:
    """Send a message to chat_id safely; returns True if sent."""
    try:
        await bot.send_message(chat_id=chat_id, text=text)
        return True
    except Exception:
        logger.exception("Failed to send message to %s", chat_id)
        return False


async def missing_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle upload for a specific past date: /missing_upload YYYY-MM-DD"""
    if not context.args:
        await update.message.reply_text(
            "Please provide a date in YYYY-MM-DD format.\n"
            "Example: /missing_upload 2025-10-29"
        )
        return
        
    # Validate date format
    date_str = context.args[0]
    try:
        # Parse and validate date
        parsed_date = dateutil.parser.parse(date_str).date()
        if parsed_date > datetime.date.today():
            await update.message.reply_text("Cannot upload for future dates.")
            return
        # Store the target date in context for handle_document
        context.user_data["pending_upload_date"] = parsed_date.isoformat()
        await update.message.reply_text(
            f"Please upload your Excel file for {parsed_date.isoformat()}.\n"
            "The file will be processed for this specific date."
        )
    except ValueError:
        await update.message.reply_text(
            "Invalid date format. Please use YYYY-MM-DD.\n"
            "Example: /missing_upload 2025-10-29"
        )
        return

async def help_cmd(update, context):
    """Show available commands based on user's permissions."""
    db_path = os.getenv("DB_PATH", "teleshop.db")
    username = update.effective_user.username or str(update.effective_user.id)
    # Check admin status combining DB flag and env var
    is_admin_db = await models.is_admin_by_username(db_path, username)
    admin_ids = os.getenv("ADMIN_IDS", "")
    admin_list = [a.strip() for a in admin_ids.split(",") if a.strip()]
    is_admin = is_admin_db or str(update.effective_user.id) in admin_list

    # Get commands from registry grouped by category
    from .commands import get_commands_by_category
    categories = get_commands_by_category(admin=is_admin)
    
    # Format help text by category
    sections = []
    for category_name in sorted(categories.keys()):
        commands = categories[category_name]
        lines = [f"\n{category_name} Commands:"]
        for cmd in sorted(commands, key=lambda c: c.name):
            cmd_text = f"/{cmd.name}"
            if cmd.usage:
                cmd_text = f"{cmd_text} {cmd.usage}"
            lines.append(f"{cmd_text} — {cmd.description}")
        sections.append("\n".join(lines))
    
    help_text = "Available commands:" + "\n".join(sections)
    await update.message.reply_text(help_text)
    
    # Grouped help with one-line-per-command descriptions
    user_cmds = [
        ("/start", "Register and capture your chat for notifications"),
        ("/register_me", "(Re)register your chat_id for notifications"),
        ("/help", "Show this help message"),
        ("/summary", "Show your current stock summary"),
        ("/my_stock", "Show your stock counts"),
        ("/my_sales", "Show your sales for a date (optional)"),
        ("/missing_upload YYYY-MM-DD", "Upload sales Excel for a past date")
    ]
    admin_cmds = [
        ("/add_stock <user> <item> <qty>", "Add stock to a user"),
        ("/remove_stock <user> <item> <qty>", "Remove stock from a user"),
        ("/view_stock <user>", "View a user's stock"),
        ("/list_inventory", "List all inventories"),
        ("/delete_sale <id>", "Delete a sale by id and revert inventory (updates credits)"),
        ("/delete_sale <user> <date>", "Delete all sales for a user on a date and revert inventory with credits (last-upload-wins safe)"),
        ("/report [date]", "Download daily recharge report with credit deductions"),
        ("/all_sales [date]", "List all sales and credits for a date"),
        ("/inventory_summary", "Show total inventory values"),
        ("/promote <user>", "Promote a user to admin"),
        ("/transfer_stock <user> <item> <qty>", "Transfer stock to a user"),
        ("/weekly_regs [start] [end]", "Aggregate daily registrations by employee")
    ]
    borrow_cmds = [
        ("/borrow_add <name> <amount> [note]", "Record a money transaction (admin only)"),
        ("/borrow_list", "List your recorded transactions (admin only)"),
        ("/borrow_summary [start] [end]", "Summary totals per person (admin only)")
    ]

    def format_cmds(cmds):
        return "\n".join([f"{c} — {d}" for c, d in cmds])

    text = "User commands:\n" + format_cmds(user_cmds)
    if is_admin:
        text += "\n\nAdmin commands:\n" + format_cmds(admin_cmds)
        text += "\n\nBorrow / Money commands:\n" + format_cmds(borrow_cmds)
        # Backoffice
        backoffice_cmds = [
            ("/backoffice_add <item> <qty>", "Add central backoffice stock (admin only)"),
            ("/backoffice_list", "List backoffice stock (admin only)"),
            ("/transfer_backoffice <user> <item> <qty>", "Transfer from backoffice to a user (admin only)")
        ]
        # SIM batch tracking
        sim_cmds = [
            ("/import_pickup", "Import SIM pickup list Excel (admin only)"),
            ("/transfer_sims <mode> <params> <target>", "Transfer SIMs by carton/box/gsm_range/list (admin only)"),
            ("/sim_status <gsm|box|carton>", "Query SIM status/location (admin only)")
        ]
        text += "\n\nBackoffice:\n" + format_cmds(backoffice_cmds)
        text += "\n\nSIM Batches:\n" + format_cmds(sim_cmds)
    
    await update.message.reply_text(text)


async def summary(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_path = os.getenv("DB_PATH", "teleshop.db")
    username = update.effective_user.username or str(update.effective_user.id)
    staff_id = await models.ensure_staff(db_path, username, update.effective_user.full_name)
    inv = await models.get_inventory(db_path, staff_id)
    text = (
        f"📦 Stock Remaining: SIM {inv['sim']} | SWAP {inv['swap']} | Credit50 {inv['credit_50']} | Credit100 {inv['credit_100']}\n"
        f"(Last updated: {inv.get('updated_at')})"
    )
    await update.message.reply_text(text)


async def my_stock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_path = os.getenv("DB_PATH", "teleshop.db")
    username = update.effective_user.username or str(update.effective_user.id)
    info = await models.view_stock_by_staff(db_path, username)
    if not info:
        await update.message.reply_text("No inventory found for you. Please contact admin.")
        return
    await update.message.reply_text(f"Your stock — SIM {info['sim']} | SWAP {info['swap']} | Credit50 {info['credit_50']} | Credit100 {info['credit_100']}")


async def my_sales(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_path = os.getenv("DB_PATH", "teleshop.db")
    username = update.effective_user.username or str(update.effective_user.id)
    date = None
    if context.args:
        date = context.args[0]
    rows = await models.get_sales_by_staff_date(db_path, username, date)
    if not rows:
        await update.message.reply_text("No sales found for the given date.")
        return
    lines = [f"Sales for {rows[0]['report_date']}:"]
    total = 0.0
    for r in rows:
        num = int(r['number'] or 0)
        amt = float(r['recharge_amount'] or 0)
        lines.append(f"{r['item_code']} — {num} pcs — Recharge {amt} AF")
        # accumulate simple totals (we won't convert item to AF here)
        total += amt
    lines.append(f"Recharge Sum: {total} AF")
    await update.message.reply_text("\n".join(lines))


def _summarize_entries(entries: list[dict]) -> dict:
    # Helper: consider a value a valid GSM if it's exactly 9 digits
    def _is_valid_gsm(val: Any) -> bool:
        try:
            s = str(val).strip()
            return s.isdigit() and len(s) == 9
        except Exception:
            return False

    # counts and sums
    res = {"SIM": 0, "SWAP": 0, "Credit50": 0, "Credit100": 0, "Recharge": 0.0}
    for e in entries:
        code = (e.get("item_code") or "").lower()
        # For SIM/SWAP, count 1 per valid GSM row (do NOT sum the 'number' field)
        if code in ("sim", "simcard", "sim_card"):
            if _is_valid_gsm(e.get("gsm_number") or e.get("number")):
                res["SIM"] += 1
            else:
                # If no gsm present but row otherwise valid, still count as 1
                res["SIM"] += 1
            continue
        if code in ("swap",):
            if _is_valid_gsm(e.get("gsm_number") or e.get("number")):
                res["SWAP"] += 1
            else:
                res["SWAP"] += 1
            continue

        # Credits store counts in dedicated fields or in 'number'
        if code in ("credit50", "credit_50", "credit-50"):
            try:
                res["Credit50"] += int(e.get("credit_50") or e.get("number") or 0)
            except Exception:
                pass
            continue
        if code in ("credit100", "credit_100", "credit-100"):
            try:
                res["Credit100"] += int(e.get("credit_100") or e.get("number") or 0)
            except Exception:
                pass
            continue

        # For other items, sum recharge amounts into total (and allow number to be used elsewhere)
        try:
            res["Recharge"] += float(e.get("recharge_amount") or 0)
        except Exception:
            pass

    return res

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle uploaded Excel document, parse, store to DB, and reply with summaries."""
    import datetime
    from pathlib import Path
    db_path = os.getenv("DB_PATH", "teleshop.db")

    # ---------------- Step 0: get username, name and date ----------------
    username = update.effective_user.username or str(update.effective_user.id)
    name = update.effective_user.full_name or username
    
    # Check if this is a missing_upload with custom date
    report_date = context.user_data.pop("pending_upload_date", None)
    if not report_date:
        report_date = datetime.date.today().isoformat()
    
    # Log with clear indication if this is a past-date upload
    is_past_upload = report_date != datetime.date.today().isoformat()
    logger.info(f"[UPLOAD] User={username}, Name={name}, Date={report_date}" + 
                (" (past date upload)" if is_past_upload else ""))

    # ---------------- Step 1: get uploaded file ----------------
    doc = update.message.document
    if not doc:
        await update.message.reply_text("Please upload an Excel file as a document.")
        logger.warning("No document found in upload.")
        return

    file = await doc.get_file()
    try:
        b = await file.download_as_bytearray()
        logger.info(f"Downloaded file {doc.file_name} ({len(b)} bytes) for user {username}")
    except Exception as ex:
        logger.exception("Failed to download uploaded document: %s", ex)
        await update.message.reply_text("Failed to download file. Try again.")
        return

    # ---------------- Step 2.5: two-step send_file flows ----------------
    # If an admin previously ran /send_file or /sendfiletoall without attaching a file,
    # the command stored a flag in context.user_data['awaiting_send_file'] which we'll handle here.
    pending_send = context.user_data.pop("awaiting_send_file", None)
    if pending_send:
        try:
            mode = pending_send.get("mode")
            target = pending_send.get("target")  # may be None for broadcast
            # Basic safety: only admins should be able to set this flag, check again
            db_path = os.getenv("DB_PATH", "teleshop.db")
            is_admin_db = await models.is_admin_by_username(db_path, update.effective_user.username or str(update.effective_user.id))
            admin_ids = os.getenv("ADMIN_IDS", "")
            admin_list = [a.strip() for a in admin_ids.split(",") if a.strip()]
            is_admin_env = str(update.effective_user.id) in admin_list
            if not (is_admin_db or is_admin_env):
                await update.message.reply_text("You are not authorized to send files.")
                return

            # Size check (conservative): reject files larger than 48 MB to avoid Telegram limits
            max_bytes = 48 * 1024 * 1024
            if len(b) > max_bytes:
                await update.message.reply_text("File too large to send (limit ~48MB).")
                return

            from telegram import InputFile as _InputFile
            # Single target send
            if mode == 'single' and target:
                staff = await models.get_staff_by_username(db_path, target)
                if not staff or not staff.get('chat_id'):
                    await update.message.reply_text(f"Target {target} not found or has no chat_id registered.")
                    return
                try:
                    await context.bot.send_document(chat_id=int(staff.get('chat_id')), document=_InputFile(bytes(b), filename=doc.file_name or 'file'))
                    await update.message.reply_text(f"File sent to {target}.")
                except Exception:
                    logger.exception("Failed to send file to %s", target)
                    await update.message.reply_text(f"Failed to send file to {target}.")
                return

            # Broadcast to all staff with chat_id
            if mode == 'all':
                chat_ids = await models.get_all_staff_chat_ids(db_path)
                if not chat_ids:
                    await update.message.reply_text("No staff chat_ids registered to broadcast.")
                    return
                sent = 0
                failed = 0
                for cid in chat_ids:
                    try:
                        await context.bot.send_document(chat_id=int(cid), document=_InputFile(bytes(b), filename=doc.file_name or 'file'))
                        sent += 1
                        # small throttle to avoid hitting rate limits
                        await asyncio.sleep(0.05)
                    except Exception:
                        failed += 1
                        logger.exception("Broadcast send failed for chat_id=%s", cid)
                await update.message.reply_text(f"Broadcast complete: sent={sent}, failed={failed}")
                return
        except Exception:
            logger.exception("Error handling awaiting_send_file flow")
            await update.message.reply_text("Failed to process pending send file request.")
            return

    # ---------------- Step 2: save uploaded Excel ----------------
    # Save uploaded file in per-employee subfolder: uploads/<username>/<original_filename>
    base_upload_dir = Path("uploads")
    staff_upload_dir = base_upload_dir / username
    staff_upload_dir.mkdir(parents=True, exist_ok=True)

    # Preserve original filename and extension (e.g., .xlsm). If missing, fall back to username_date.xlsm
    try:
        orig_name = doc.file_name or f"{username}_{report_date}.xlsm"
    except Exception:
        orig_name = f"{username}_{report_date}.xlsm"

    # Ensure extension is present; prefer .xlsm when extension absent
    if not Path(orig_name).suffix:
        orig_name = f"{orig_name}.xlsm"

    file_path = staff_upload_dir / orig_name
    try:
        # write raw bytes to disk (preserve formulas / macro-enabled workbook if provided)
        with open(file_path, "wb") as f:
            f.write(b)
        logger.info(f"Saved uploaded file to {file_path}")
    except Exception as ex:
        logger.exception("Failed to save uploaded file to disk: %s", ex)
        await update.message.reply_text("Failed to save uploaded file. Try again.")
        return

    # ---------------- Step 3: parse Excel ----------------
    # Support two modes:
    # - If user had previously run /import_pickup without attaching a file, context.user_data['awaiting_pickup']
    #   will be True and this uploaded document should be treated as a pickup list.
    # - Otherwise, treat as a sales upload (existing behavior).
    try:
        import asyncio
        # pickup two-step flow: user ran /import_pickup and then uploaded file
        if context.user_data.get("awaiting_pickup"):
            logger.info(f"[UPLOAD] Treating document as PICKUP list for user {username}")
            from utils.excel_utils import parse_pickup_excel
            try:
                rows = await asyncio.to_thread(parse_pickup_excel, bytes(b))
            except Exception as ex:
                logger.exception("Failed parsing pickup Excel: %s", ex)
                await update.message.reply_text("Failed to parse pickup Excel. Ensure it contains Carton #, BOX #, GSM NUMBER, ICCID, Type columns.")
                # clear the awaiting flag to avoid repeated misrouting
                context.user_data.pop("awaiting_pickup", None)
                return
            if not rows:
                await update.message.reply_text("Invalid pickup Excel: missing required pickup columns or no GSM numbers found.")
                context.user_data.pop("awaiting_pickup", None)
                return
            # call DB helper (preserve existing insert_pickup_list behavior)
            try:
                filename = f"pickup_{username}_{report_date}.xlsx"
                res = await models.insert_pickup_list(db_path, bytes(b), filename, username)
                await update.message.reply_text(f"Pickup Excel processed: {res.get('inserted')} inserted, {res.get('duplicates')} duplicates")
            except Exception as ex:
                logger.exception("import_pickup failed: %s", ex)
                await update.message.reply_text("Import failed due to internal error.")
            finally:
                # clear awaiting flag after handling
                context.user_data.pop("awaiting_pickup", None)
            return

        # Default: treat uploaded document as sales Excel (existing behavior)
        from utils.excel_utils import parse_sales_excel
        parsed = await asyncio.to_thread(parse_sales_excel, b, report_date, name)
        # Support both old (3-tuple) and new (4-tuple) return shapes for backward compatibility
        if isinstance(parsed, tuple):
            if len(parsed) == 4:
                entries, errors, daily_regs, should_remind_regs = parsed
            elif len(parsed) == 3:
                entries, errors, daily_regs = parsed
                should_remind_regs = False
            else:
                raise ValueError(f"Unexpected parse_sales_excel return shape: {len(parsed)}")
            # If we have a Notes column but no registrations found, remind them
            if should_remind_regs:
                await update.message.reply_text("⚠️ Reminder: Did you forget to report your daily registrations? Please include them in the Notes column (e.g., 'REG: 10').")
        else:
            # unexpected non-tuple result
            raise ValueError("parse_sales_excel returned unexpected non-tuple result")
        logger.info(f"Parsed Excel: {len(entries)} entries, errors={errors}, daily_regs={daily_regs}")
    except Exception as ex:
        logger.exception("Failed parsing Excel: %s", ex)
        await update.message.reply_text("Failed to parse Excel file. Ensure it is a valid spreadsheet.")
        return
    if errors:
        # Split errors into validation errors and skip notices
        validation_errors = [e for e in errors if not e.startswith("Row")]
        skipped_notices = [e for e in errors if e.startswith("Row")]
        
        if validation_errors:
            await update.message.reply_text("Invalid Excel file: " + "; ".join(validation_errors))
            logger.warning(f"Excel parse errors: {validation_errors}")
            return
            
        # If we only have skipped rows, continue processing but show the notices
        if skipped_notices:
            logger.info(f"Skipped rows during parsing: {skipped_notices}")
            context.user_data["skipped_rows"] = skipped_notices  # Store for final summary
    if not entries:
        await update.message.reply_text("No valid entries found in the uploaded file.")
        logger.warning(f"No valid entries after parsing Excel for user {username}")
        return

    # ---------------- Step 4: extract daily registration ----------------
    # parse_sales_excel already attempts to extract daily_regs from the raw Notes cell
    # (and returns it). Only attempt to extract from parsed entry notes when parser didn't
    # find a value (daily_regs == 0).
    if not daily_regs:
        # Extract registration count from Notes field more robustly
        for e in entries:
            notes = str(e.get("notes", "")).strip()
            if not notes:
                continue
            # Normalize and look for a leading 'reg' token, tolerating spaces: e.g. 'REG : 10'
            low = notes.lower().replace(" ", "")
            if low.startswith("reg:"):
                try:
                    # find first integer in the original notes string
                    import re as _re
                    m = _re.search(r"(\d+)", notes)
                    if m:
                        daily_regs = int(m.group(1))
                        break
                except Exception:
                    pass
            # fallback: extract any integer in the notes
            try:
                import re as _re
                m = _re.search(r"(\d+)", notes)
                if m:
                    val = int(m.group(1))
                    if 0 < val < 1000:
                        daily_regs = val
                        break
            except Exception:
                pass

    if daily_regs > 0:
        logger.info(f"[REGS] Found daily_regs={daily_regs}")
        entries.append({
            "item_code": "registration",
            "number": daily_regs,
            "recharge_amount": 0,
            "notes": "daily registration from Notes cell"
        })

    # ---------------- Step 4.5: remove duplicate SIM rows from entries (parsing-time)
    # Duplicate rule: for SIM entries, if the same GSM/number appears more than once in the upload,
    # skip subsequent duplicates. For SWAP duplicates are allowed. This prevents duplicates from
    # being included in summaries prior to DB insertion.
    filtered_entries = []
    seen_identifiers = set()
    parse_duplicates_skipped = 0
    for e in entries:
        try:
            item_code = (e.get("item_code") or "").lower()
            identifier = None
            # prefer explicit gsm_number column
            gsm = e.get("gsm_number") or e.get("GSM") or None
            if gsm and str(gsm).strip():
                identifier = str(gsm).strip()
            else:
                # fallback to number column when it looks like a GSM (digits and length >=6)
                raw_number = e.get("number")
                if raw_number is not None:
                    s = str(raw_number).strip()
                    if s.isdigit() and len(s) >= 6:
                        identifier = s

            if identifier and item_code in ("sim", "simcard", "sim_card"):
                if identifier in seen_identifiers:
                    parse_duplicates_skipped += 1
                    continue
                seen_identifiers.add(identifier)

            # otherwise keep the row
            filtered_entries.append(e)
        except Exception:
            # If any unexpected error happens, keep the row to avoid data loss
            filtered_entries.append(e)

    # replace entries with filtered list so subsequent summaries and DB insertion exclude duplicates
    entries = filtered_entries

    # ---------------- Step 4.75: check DB for already-sold GSM numbers (global duplicates)
    # Build list of candidate GSM/number identifiers from the parsed entries
    duplicates_detected = []
    try:
        candidate_numbers = []
        for e in entries:
            try:
                item_code = (e.get("item_code") or "").lower()
                if item_code not in ("sim", "simcard", "sim_card"):
                    continue
                gsm = e.get("gsm_number") or e.get("GSM") or None
                store_number = None
                if gsm and str(gsm).strip():
                    store_number = str(gsm).strip()
                else:
                    raw_number = e.get("number")
                    if raw_number is not None:
                        s = str(raw_number).strip()
                        if s.isdigit() and len(s) >= 6:
                            store_number = s
                if store_number:
                    candidate_numbers.append(store_number)
            except Exception:
                continue

        # Deduplicate while preserving order
        seen = set()
        unique_candidates = []
        for n in candidate_numbers:
            if n in seen:
                continue
            seen.add(n)
            unique_candidates.append(n)

        if unique_candidates:
            existing = await models.get_sales_by_numbers(db_path, unique_candidates)
            # map number -> first matching row
            existing_map = {r.get("number"): r for r in existing}

            filtered = []
            for e in entries:
                try:
                    item_code = (e.get("item_code") or "").lower()
                    if item_code not in ("sim", "simcard", "sim_card"):
                        filtered.append(e)
                        continue
                    gsm = e.get("gsm_number") or e.get("GSM") or None
                    store_number = None
                    if gsm and str(gsm).strip():
                        store_number = str(gsm).strip()
                    else:
                        raw_number = e.get("number")
                        if raw_number is not None:
                            s = str(raw_number).strip()
                            if s.isdigit() and len(s) >= 6:
                                store_number = s

                    if store_number and store_number in existing_map:
                        # record duplicate info (number and original sale date/user)
                        row = existing_map.get(store_number)
                        duplicates_detected.append({
                            "number": store_number,
                            "report_date": row.get("report_date"),
                            "username": row.get("username"),
                        })
                        # skip inserting this row
                        continue
                    # otherwise keep row for insertion
                    filtered.append(e)
                except Exception:
                    # on any unexpected error, keep the row to avoid data loss
                    filtered.append(e)

            entries = filtered
    except Exception:
        logger.exception("Failed to detect DB-level duplicate GSM numbers; proceeding with insertion")

    # ---------------- Step 5: ensure staff ----------------
    try:
        staff_id = await models.ensure_staff(db_path, username, name)
        logger.info(f"Ensured staff in DB: {username} (id={staff_id})")
        # Rename the saved uploaded file to use the EmployeeName_Date.xlsx format
        try:
            # Prefer the staff name stored in DB (may include spaces); fall back to provided full name
            staff_rec = await models.get_staff_by_username(db_path, username)
            staff_name_for_file = (staff_rec.get('name') if staff_rec and staff_rec.get('name') else name) or username
            # sanitize to alphanumeric only (remove spaces/special chars)
            sanitized = ''.join([c for c in staff_name_for_file if c.isalnum()])
            # use .xlsx extension for saved copies
            ext = file_path.suffix if file_path.suffix else '.xlsx'
            new_name = f"{sanitized}_{report_date}{ext}"
            new_path = file_path.with_name(new_name)
            try:
                file_path.rename(new_path)
                logger.info(f"Renamed uploaded file to {new_path}")
                # update file_path variable so subsequent logic refers to new path
                file_path = new_path
            except Exception as rn_ex:
                logger.warning(f"Failed to rename uploaded file {file_path} to {new_path}: {rn_ex}")
        except Exception:
            logger.exception("Failed to compute employee name for uploaded file rename")
    except Exception as ex:
        logger.exception("Failed to ensure staff in DB: %s", ex)
        await update.message.reply_text("Internal error: could not register user. Try again later.")
        return
    # delete previous daily_regs for this staff/date (last-upload-wins)
    try:
        def _del_daily():
            conn = models.get_connection(db_path)
            cur = conn.cursor()
            cur.execute("DELETE FROM daily_regs WHERE staff_id = ? AND date = ?", (staff_id, report_date))
            conn.commit()
            conn.close()
        import asyncio as _asyncio
        await _asyncio.to_thread(_del_daily)
        logger.info(f"Deleted previous daily_regs for staff {staff_id} on {report_date}")
    except Exception:
        logger.exception("Failed to delete previous daily_regs for last-upload-wins")

    # ---------------- Step 6: last-upload-wins -> delete previous, then insert ----------------
    try:
        # CRITICAL: Insert sales and update inventory atomically
        # The delete_sales_for_staff_date is now handled inside insert_sales_and_update_inventory
        # to ensure proper transaction atomicity and prevent race conditions
        result = await models.insert_sales_and_update_inventory(db_path, staff_id, report_date, entries)
        logger.info(f"insert_sales_and_update_inventory result: {result}")
        # save daily registrations if any
        try:
            if daily_regs and int(daily_regs) > 0:
                await models.insert_daily_regs(db_path, staff_id, report_date, int(daily_regs))
                logger.info(f"Inserted daily_regs: {daily_regs}")
        except Exception:
            logger.exception("Failed to save daily registrations")
        # Persist per-shop daily totals (best-effort, non-fatal)
        try:
            # build staff -> shop map
            conn = models.get_connection(db_path)
            cur = conn.cursor()
            cur.execute("SELECT username, shop_id FROM staff")
            sm = cur.fetchall()
            staff_shop_map = {r['username']: r['shop_id'] for r in sm}
            conn.close()

            # fetch all sales for the date (includes credit rows)
            all_rows = await models.get_all_sales_by_date(db_path, report_date)
            shop_aggregates = {}
            for r in all_rows:
                uname = r.get('username')
                shop_id = staff_shop_map.get(uname) or 0
                code = (r.get('item_code') or '').lower()
                if shop_id not in shop_aggregates:
                    shop_aggregates[shop_id] = {'SIM': 0, 'SWAP': 0, 'Credit50': 0, 'Credit100': 0, 'Recharge': 0.0}
                if code in ('sim', 'simcard', 'sim_card'):
                    shop_aggregates[shop_id]['SIM'] += 1
                elif code == 'swap':
                    shop_aggregates[shop_id]['SWAP'] += 1
                elif code in ('credit50', 'credit_50', 'credit-50'):
                    # credit rows store count in number
                    shop_aggregates[shop_id]['Credit50'] += int(r.get('number') or 0)
                elif code in ('credit100', 'credit_100', 'credit-100'):
                    shop_aggregates[shop_id]['Credit100'] += int(r.get('number') or 0)
                shop_aggregates[shop_id]['Recharge'] += float(r.get('recharge_amount') or 0.0)

            # persist per-shop totals and grand total
            grand_total_amount = 0.0
            for sid, stats in shop_aggregates.items():
                amount = stats['SIM']*100 + stats['SWAP']*50 + stats['Credit50']*50 + stats['Credit100']*100 + stats['Recharge']
                grand_total_amount += amount
                try:
                    await models.insert_daily_total(db_path, report_date, sid if sid != 0 else None, float(amount))
                except Exception:
                    logger.exception("Failed to persist daily total for shop %s on %s", sid, report_date)
            # persist grand total (shop_id NULL)
            try:
                await models.insert_daily_total(db_path, report_date, None, float(grand_total_amount))
            except Exception:
                logger.exception("Failed to persist grand daily total for %s", report_date)
        except Exception:
            logger.exception("Failed computing/persisting per-shop daily totals (non-fatal)")
    except Exception as ex:
        logger.exception("Failed inserting sales/updating inventory: %s", ex)
        await update.message.reply_text("Internal error: failed to save sales. Some rows may not be recorded.")
        return

    # ---------------- Step 7: summarize sales ----------------
    # Use the saved rows from the DB to compute summary totals so the bot reply
    # matches exactly what was persisted. Fall back to parsed entries only
    # if the DB query fails for any reason.
    try:
        saved_rows = await models.get_sales_by_staff_date(db_path, username, report_date)
    except Exception:
        logger.exception("Failed to fetch saved sales for summary; falling back to parsed entries")
        saved_rows = None

    if saved_rows:
        s = {"SIM": 0, "SWAP": 0, "Credit50": 0, "Credit100": 0, "Recharge": 0.0}
        for r in saved_rows:
            code = (r.get('item_code') or '').lower()
            if code in ('sim', 'simcard', 'sim_card'):
                s['SIM'] += 1
            elif code == 'swap':
                s['SWAP'] += 1
            elif code in ('credit50', 'credit_50', 'credit-50'):
                try:
                    s['Credit50'] += int(r.get('number') or 0)
                except Exception:
                    pass
            elif code in ('credit100', 'credit_100', 'credit-100'):
                try:
                    s['Credit100'] += int(r.get('number') or 0)
                except Exception:
                    pass
            try:
                s['Recharge'] += float(r.get('recharge_amount') or 0.0)
            except Exception:
                pass
    else:
        # fallback to summarizing parsed entries (should be rare)
        s = _summarize_entries(entries)

    sim_total = s["SIM"] * 100
    swap_total = s["SWAP"] * 50
    credit50_total = s["Credit50"] * 50
    credit100_total = s["Credit100"] * 100
    recharge_sum = s["Recharge"]
    grand_total = sim_total + swap_total + credit50_total + credit100_total + recharge_sum
    def fmt_total(value):
        return f"{value:.1f}" if isinstance(value, float) else str(value)
    try:
        inv = await models.get_inventory(db_path, staff_id)
    except Exception:
        inv = {"sim": 0, "swap": 0, "credit_50": 0, "credit_100": 0}
    

    msg_lines = [
        "✅ Upload successful.",
        "",
        "📦 Stock Remaining:",
        f"   SIM:      {inv['sim']}",
        f"   SWAP:     {inv['swap']}",
        f"   Credit50: {inv['credit_50']}",
        f"   Credit100:{inv['credit_100']}",
        "",
        "💰 Sales Summary:",
        f"   SIMCARDS  x {s['SIM']}   @ 100 AF = {sim_total} AF",
        f"   SIMSWAPS  x {s['SWAP']}   @  50 AF = {swap_total} AF",
        f"   Credit50  x {s['Credit50']}   @  50 AF = {credit50_total} AF",
        f"   Credit100 x {s['Credit100']}   @ 100 AF = {credit100_total} AF",
        f"   RECHARGE           = {fmt_total(recharge_sum)} AF",
        "",
        f"🧾 Grand Total: {grand_total} AF"
    ]
    if daily_regs > 0:
        msg_lines.append(f"📝 Registrations today: {daily_regs}")
        
    # Send the main success message first
    await update.message.reply_text("\n".join(msg_lines))
    
    # Send warnings in a separate message if needed
    warning_lines = []
    
    # Show skipped rows from validation
    skipped_notices = context.user_data.pop("skipped_rows", [])
    if skipped_notices:
        warning_lines.extend(["⚠️ Processing Warnings:", *skipped_notices])
        
    if parse_duplicates_skipped > 0:
        if warning_lines:
            warning_lines.append("")
        warning_lines.append(f"⚠️ Note: {parse_duplicates_skipped} duplicate SIM entries were automatically handled.")
        
    # Send warnings as a separate message if we have any
    if warning_lines:
        await update.message.reply_text("\n".join(warning_lines))

        try:
            await update.message.reply_text("\n".join(msg_lines))
            # notify employee about duplicate rows if any
            # Prefer explicit duplicates_detected list (with dates) when available
            if duplicates_detected:
                lines = ["⚠️ Some rows were duplicates and were ignored.", "Duplicates:"]
                for d in duplicates_detected:
                    lines.append(f"GSM Number: {d.get('number')}, Sold on: {d.get('report_date')}, By: {d.get('username')}")
                lines.append("Only new entries were processed successfully.")
                await update.message.reply_text("\n".join(lines))
            elif isinstance(result, dict) and result.get("duplicates_skipped"):
                await update.message.reply_text("⚠️ Some rows were duplicates and were ignored. Only new entries were processed.")
        except Exception:
            logger.exception("Failed to send upload summary message to employee")

    # Notify admins (by stored chat_id) with the same summary
    try:
        admin_notify = os.getenv("ADMIN_NOTIFY_CHAT_ID")
        admin_text = f"User {username} uploaded sales for {report_date}.\n" + "\n".join(msg_lines)
        if admin_notify:
            # send to the designated admin chat id
            await send_message_safe(context.bot, admin_notify, admin_text)
        else:
            admin_chat_ids = await models.get_all_admin_chat_ids(db_path)
            for cid in admin_chat_ids:
                await send_message_safe(context.bot, cid, admin_text)
    except Exception:
        logger.exception("Failed to notify admins of uploaded sales")



def _require_admin(fn):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        db_path = os.getenv("DB_PATH", "teleshop.db")
        username = update.effective_user.username or str(update.effective_user.id)
        # check DB flag
        is_db_admin = await models.is_admin_by_username(db_path, username)
        # check env admin ids list
        admin_ids = os.getenv("ADMIN_IDS", "").split(",") if os.getenv("ADMIN_IDS") else []
        is_env_admin = str(update.effective_user.id) in admin_ids
        if not (is_db_admin or is_env_admin):
            await update.message.reply_text("You are not an admin.")
            return
        return await fn(update, context)

    return wrapper


async def add_stock_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_path = os.getenv("DB_PATH", "teleshop.db")
    if len(context.args) < 3:
        await update.message.reply_text("Usage: /add_stock <staff_username> <item> <qty>")
        return
    staff_username, item, qty = context.args[0], context.args[1], int(context.args[2])
    ok = await models.add_stock(db_path, staff_username, item, qty)
    if not ok:
        await update.message.reply_text("Failed to add stock. Check staff username or item.")
        return
    logger.info("/add_stock by %s: %s +%s to %s", update.effective_user.username, item, qty, staff_username)
    await update.message.reply_text(f"Added {qty} {item} to {staff_username}.")


async def remove_stock_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_path = os.getenv("DB_PATH", "teleshop.db")
    if len(context.args) < 3:
        await update.message.reply_text("Usage: /remove_stock <staff_username> <item> <qty>")
        return
    staff_username, item, qty = context.args[0], context.args[1], int(context.args[2])
    ok = await models.remove_stock(db_path, staff_username, item, qty)
    if not ok:
        await update.message.reply_text("Failed to remove stock. Check staff username, item, or quantity available.")
        return
    logger.info("/remove_stock by %s: %s -%s from %s", update.effective_user.username, item, qty, staff_username)
    await update.message.reply_text(f"Removed {qty} {item} from {staff_username}.")


async def view_stock_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_path = os.getenv("DB_PATH", "teleshop.db")
    if len(context.args) < 1:
        await update.message.reply_text("Usage: /view_stock <staff_username>")
        return
    staff_username = context.args[0]
    info = await models.view_stock_by_staff(db_path, staff_username)
    if not info:
        await update.message.reply_text("Staff not found")
        return
    await update.message.reply_text(f"{staff_username} — SIM {info['sim']} | SWAP {info['swap']} | Credit50 {info['credit_50']} | Credit100 {info['credit_100']}")


async def list_inventory_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_path = os.getenv("DB_PATH", "teleshop.db")
    rows = await models.list_inventory(db_path)
    if not rows:
        await update.message.reply_text("No inventory records.")
        return
    # nice styled table-like output but plain text
    header = f"{'User':<15} {'SIM':>5} {'SWAP':>6} {'C50':>6} {'C100':>6}"
    sep = "".join(["-" for _ in range(len(header))])
    lines = ["📦 Current Inventories:", header, sep]
    for r in rows:
        uname = r.get('username') or ''
        name = r.get('name') or ''
        lines.append(f"{uname:<15} {r.get('sim',0):>5} {r.get('swap',0):>6} {r.get('credit_50',0):>6} {r.get('credit_100',0):>6}  {name}")
    await update.message.reply_text("\n".join(lines))


async def msg_user_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command: /msg_user <username> <message>"""
    db_path = os.getenv("DB_PATH", "teleshop.db")
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /msg_user <username> <message>")
        return
    target = context.args[0]
    message = " ".join(context.args[1:])
    try:
        staff = await models.get_staff_by_username(db_path, target)
        if not staff or not staff.get("chat_id"):
            await update.message.reply_text("User not found or has no chat_id registered.")
            return
        ok = await send_message_safe(context.bot, staff.get("chat_id"), message)
        if ok:
            await update.message.reply_text("Message sent.")
        else:
            await update.message.reply_text("Failed to deliver message.")
    except Exception:
        logger.exception("msg_user failed")
        await update.message.reply_text("Internal error while sending message.")


async def msg_all_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command: /msg_all <message>"""
    db_path = os.getenv("DB_PATH", "teleshop.db")
    if len(context.args) < 1:
        await update.message.reply_text("Usage: /msg_all <message>")
        return
    message = " ".join(context.args)
    try:
        chat_ids = await models.get_all_staff_chat_ids(db_path)
        if not chat_ids:
            await update.message.reply_text("No staff have chat_id registered.")
            return
        sent = 0
        for cid in chat_ids:
            ok = await send_message_safe(context.bot, cid, message)
            if ok:
                sent += 1
        await update.message.reply_text(f"Message broadcast to {sent} users.")
    except Exception:
        logger.exception("msg_all failed")
        await update.message.reply_text("Internal error while broadcasting message.")


async def delete_sale_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_path = os.getenv("DB_PATH", "teleshop.db")
    # Support two modes:
    # 1) /delete_sale <sale_id>  -> delete single sale by id (backwards compatible)
    # 2) /delete_sale <username> <date> -> delete all sales for that staff on that date
    if len(context.args) < 1:
        await update.message.reply_text("Usage: /delete_sale <sale_id>  OR  /delete_sale <staff_username> <YYYY-MM-DD>")
        return

    # Mode 2: username + date
    if len(context.args) >= 2:
        target_username = context.args[0]
        date_str = context.args[1]
        # validate date
        try:
            parsed_date = dateutil.parser.parse(date_str).date().isoformat()
        except Exception:
            await update.message.reply_text("Invalid date format. Use YYYY-MM-DD or similar.")
            return

        # ensure staff exists
        staff = await models.get_staff_by_username(db_path, target_username)
        if not staff:
            await update.message.reply_text(f"Staff '{target_username}' not found.")
            return

        try:
            deleted = await models.delete_sales_for_staff_date(db_path, staff['id'], parsed_date)
            logger.info("/delete_sale by %s: deleted %s sales for %s on %s", update.effective_user.username, deleted, target_username, parsed_date)
            if deleted:
                await update.message.reply_text(f"Deleted {deleted} sales for {target_username} on {parsed_date}. Inventory adjusted.")
            else:
                await update.message.reply_text(f"No sales found for {target_username} on {parsed_date}.")
            return
        except Exception:
            logger.exception("Failed to delete sales for staff/date")
            await update.message.reply_text("Internal error while deleting sales for staff/date.")
            return

    # Mode 1: single sale id
    try:
        sale_id = int(context.args[0])
    except Exception:
        await update.message.reply_text("Sale id must be an integer.")
        return

    # Fetch sale for friendly message
    sale = await models.get_sale_by_id(db_path, sale_id)
    if not sale:
        await update.message.reply_text(f"Sale {sale_id} not found.")
        return

    # perform deletion
    ok = await models.delete_sale(db_path, sale_id)
    if not ok:
        await update.message.reply_text(f"Failed to delete sale {sale_id}.")
        return

    logger.info("/delete_sale by %s: sale %s (user=%s item=%s number=%s)", update.effective_user.username, sale_id, sale.get('username'), sale.get('item_code'), sale.get('number'))
    # number may be an identifier (mobile) or quantity; message should reflect change generally
    await update.message.reply_text(f"Sale {sale_id} deleted. Inventory for {sale.get('username')} adjusted.")


async def weekly_regs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: aggregate daily registrations between two dates (or show all). Usage: /weekly_regs [start_date] [end_date]"""
    db_path = os.getenv("DB_PATH", "teleshop.db")
    args = context.args
    try:
        if len(args) >= 2:
            start_date = args[0]
            end_date = args[1]
        else:
            # default: last 7 days
            import datetime as _dt
            end_date = _dt.date.today().isoformat()
            start_date = (_dt.date.today() - _dt.timedelta(days=7)).isoformat()
        rows = await models.get_regs_between(db_path, start_date, end_date)
        if not rows:
            await update.message.reply_text("No registrations found in the given range.")
            return
        lines = [f"Registrations {start_date} → {end_date}:"]
        for r in rows:
            lines.append(f"{r.get('username')} ({r.get('name')}): {r.get('total_regs')}")
        await update.message.reply_text("\n".join(lines))
    except Exception:
        logger.exception("weekly_regs failed")
        await update.message.reply_text("Internal error while computing weekly regs.")


async def borrow_add_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: /borrow_add <name> <amount> <note>"""
    db_path = os.getenv("DB_PATH", "teleshop.db")
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /borrow_add <name> <amount> [note]")
        return
    name = context.args[0]
    try:
        amount = float(context.args[1])
    except Exception:
        await update.message.reply_text("Amount must be a number.")
        return
    note = " ".join(context.args[2:]) if len(context.args) > 2 else None
    admin_id = update.effective_user.id
    try:
        ok = await models.borrow_add(db_path, str(admin_id), name, amount, None, note)
        if ok:
            await update.message.reply_text("Transaction recorded.")
        else:
            await update.message.reply_text("Failed to record transaction.")
    except Exception:
        logger.exception("borrow_add failed")
        await update.message.reply_text("Internal error while recording transaction.")


async def borrow_list_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_path = os.getenv("DB_PATH", "teleshop.db")
    admin_id = update.effective_user.id
    try:
        rows = await models.borrow_list_for_admin(db_path, str(admin_id))
        if not rows:
            await update.message.reply_text("No transactions recorded.")
            return
        lines = [f"Your transactions (most recent first):"]
        for r in rows:
            lines.append(f"{r.get('date')}: {r.get('person_name')} {r.get('amount')} — {r.get('note')}")
        await update.message.reply_text("\n".join(lines))
    except Exception:
        logger.exception("borrow_list failed")
        await update.message.reply_text("Internal error while fetching transactions.")


async def borrow_summary_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_path = os.getenv("DB_PATH", "teleshop.db")
    admin_id = update.effective_user.id
    args = context.args
    try:
        if len(args) >= 2:
            start_date = args[0]
            end_date = args[1]
        else:
            start_date = None
            end_date = None
        rows = await models.borrow_summary(db_path, str(admin_id), start_date, end_date)
        if not rows:
            await update.message.reply_text("No transactions in the specified range.")
            return
        lines = ["Summary by person:"]
        total = 0.0
        for r in rows:
            amt = float(r.get('total') or 0)
            total += amt
            lines.append(f"{r.get('person_name')}: {amt}")
        lines.append(f"Grand total: {total}")
        await update.message.reply_text("\n".join(lines))
    except Exception:
        logger.exception("borrow_summary failed")
        await update.message.reply_text("Internal error while computing summary.")


async def backoffice_add_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_path = os.getenv("DB_PATH", "teleshop.db")
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /backoffice_add <item> <qty>")
        return
    item = context.args[0]
    try:
        qty = int(context.args[1])
    except Exception:
        await update.message.reply_text("Quantity must be an integer.")
        return
    try:
        ok = await models.add_backoffice_stock(db_path, item, qty)
        if ok:
            await update.message.reply_text(f"Added {qty} {item} to backoffice stock.")
        else:
            await update.message.reply_text("Failed to add backoffice stock.")
    except Exception:
        logger.exception("backoffice_add failed")
        await update.message.reply_text("Internal error while adding backoffice stock.")


async def backoffice_list_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_path = os.getenv("DB_PATH", "teleshop.db")
    try:
        rows = await models.list_backoffice_stock(db_path)
        if not rows:
            await update.message.reply_text("No backoffice stock items.")
            return
        lines = [f"{r['item']}: {r['quantity']}" for r in rows]
        await update.message.reply_text("\n".join(lines))
    except Exception:
        logger.exception("backoffice_list failed")
        await update.message.reply_text("Internal error while fetching backoffice stock.")


@_require_admin
async def handle_pickup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command to import pickup Excel. Supports immediate attachment or two-step flow.
    Usage: either send `/import_pickup` with the Excel attached, or send `/import_pickup` then upload the file.
    """
    db_path = os.getenv("DB_PATH", "teleshop.db")
    doc = update.message.document
    import asyncio as _asyncio
    # If the command message already contained a document, process immediately.
    if doc:
        file = await doc.get_file()
        try:
            b = await file.download_as_bytearray()
        except Exception:
            await update.message.reply_text("Failed to download attached file.")
            return
        upload_dir = Path("uploads")
        upload_dir.mkdir(exist_ok=True)
        filename = f"pickup_{update.effective_user.username}_{datetime.date.today().isoformat()}.xlsx"
        file_path = upload_dir / filename
        try:
            with open(file_path, "wb") as f:
                f.write(b)
        except Exception:
            await update.message.reply_text("Failed to save file on server.")
            return
        # validate and insert
        from utils.excel_utils import parse_pickup_excel
        try:
            rows = await _asyncio.to_thread(parse_pickup_excel, bytes(b))
        except Exception as ex:
            logger.exception("Failed to parse pickup Excel: %s", ex)
            await update.message.reply_text("Failed to parse pickup Excel. Ensure it contains Carton #, BOX #, GSM NUMBER, ICCID, Type columns.")
            return
        if not rows:
            await update.message.reply_text("Invalid pickup Excel: missing required pickup columns or no GSM numbers found.")
            return
        try:
            res = await models.insert_pickup_list(db_path, bytes(b), filename, update.effective_user.username)
            await update.message.reply_text(f"Pickup Excel processed: {res.get('inserted')} inserted, {res.get('duplicates')} duplicates")
        except Exception as ex:
            logger.exception("import_pickup failed: %s", ex)
            await update.message.reply_text("Import failed due to internal error.")
        return

    # Otherwise, set a flag and ask the user to upload the pickup file in the next message.
    context.user_data["awaiting_pickup"] = True
    await update.message.reply_text("Please attach the pickup Excel file in your next message. I will process it as a pickup list.")


async def transfer_sims_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_path = os.getenv("DB_PATH", "teleshop.db")
    args = context.args
    if not args or len(args) < 3:
        await update.message.reply_text("Usage examples:\n/transfer_sims box 54 58 Teleshop_A\n/transfer_sims carton 12 Teleshop_B\n/transfer_sims gsm_range 749653372 749654035 Teleshop_C\n/transfer_sims list Teleshop_A 749653372,749653387")
        return
    mode = args[0].lower()
    try:
        if mode == 'box' and len(args) >= 4:
            start = args[1]
            end = args[2]
            target = args[3]
            where = "box_no BETWEEN ? AND ?"
            params = [start, end]
        elif mode == 'carton' and len(args) >= 3:
            carton = args[1]
            target = args[2]
            where = "carton_no = ?"
            params = [carton]
        elif mode == 'gsm_range' and len(args) >= 4:
            start = args[1]
            end = args[2]
            target = args[3]
            where = "gsm_number BETWEEN ? AND ?"
            params = [start, end]
        elif mode == 'list' and len(args) >= 3:
            target = args[1]
            gsm_list = args[2].split(',')
            placeholders = ','.join('?' for _ in gsm_list)
            where = f"gsm_number IN ({placeholders})"
            params = gsm_list
        else:
            await update.message.reply_text("Invalid parameters for transfer_sims.")
            return
        # map target to location string
        if target.lower().startswith('admin:'):
            target_loc = f"Admin:{target.split(':',1)[1]}"
        elif target.lower().startswith('shop:'):
            target_loc = f"Shop:{target.split(':',1)[1]}"
        elif target.lower().startswith('employee:'):
            target_loc = f"Employee:{target.split(':',1)[1]}"
        else:
            # default to Shop:<name>
            target_loc = f"Shop:{target}"

        res = await models.transfer_sims_by_clause(db_path, where, params, target_loc, update.effective_user.username)
        if res.get('error'):
            await update.message.reply_text(f"Transfer aborted: {res.get('error')}")
            return
        moved = res.get('moved')
        gsms = res.get('gsms')
        if moved > 500:
            # create CSV attachment
            csv_path = Path('uploads') / f"moved_sims_{datetime.date.today().isoformat()}.csv"
            with open(csv_path, 'w', encoding='utf-8') as f:
                f.write('gsm_number\n')
                for g in gsms:
                    f.write(f"{g}\n")
            await update.message.reply_text(f"Moved {moved} SIMs. Uploaded CSV: {csv_path}")
        else:
            sample = ', '.join(gsms[:20])
            await update.message.reply_text(f"Moved {moved} SIMs. Sample GSMs: {sample} ...")
    except Exception as ex:
        logger.exception("transfer_sims failed: %s", ex)
        await update.message.reply_text("Transfer failed due to internal error.")


async def sim_status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_path = os.getenv("DB_PATH", "teleshop.db")
    if not context.args:
        await update.message.reply_text("Usage: /sim_status <gsm|box|carton> <value>")
        return
    qtype = context.args[0].lower()
    if len(context.args) < 2:
        await update.message.reply_text("Provide a value to query.")
        return
    value = context.args[1]
    try:
        if qtype in ('gsm', 'box', 'carton'):
            res = await models.sim_status(db_path, qtype, value)
            if not res:
                await update.message.reply_text("No records found.")
                return
            if qtype == 'gsm':
                lines = [f"GSM: {res.get('gsm_number')} — Location: {res.get('current_location')} — Status: {res.get('status')}"]
                hist = res.get('history', [])
                for h in hist[:10]:
                    lines.append(f"{h.get('timestamp')}: {h.get('change_type')} {h.get('change_amount')} — {h.get('source')}")
                await update.message.reply_text('\n'.join(lines))
            else:
                await update.message.reply_text(str(res))
        else:
            await update.message.reply_text("Unknown query type. Use gsm, box, or carton.")
    except Exception as ex:
        logger.exception("sim_status_cmd failed: %s", ex)
        await update.message.reply_text("Query failed due to internal error.")


async def transfer_backoffice_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_path = os.getenv("DB_PATH", "teleshop.db")
    if len(context.args) < 3:
        await update.message.reply_text("Usage: /transfer_backoffice <user> <item> <qty>")
        return
    target = context.args[0]
    item = context.args[1]
    try:
        qty = int(context.args[2])
    except Exception:
        await update.message.reply_text("Quantity must be an integer.")
        return
    try:
        ok = await models.transfer_backoffice(db_path, item, qty, to_username=target)
        if not ok:
            await update.message.reply_text("Transfer failed. Check backoffice stock or target user.")
            return
        await update.message.reply_text(f"Transferred {qty} {item} from backoffice to {target}.")
    except Exception:
        logger.exception("transfer_backoffice failed")
        await update.message.reply_text("Internal error while transferring backoffice stock.")


async def report_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_path = os.getenv("DB_PATH", "teleshop.db")
    date_str = context.args[0] if context.args else None
    # flexible date parsing
    parsed_date = None
    if date_str:
        try:
            parsed_date = dateutil.parser.parse(date_str).date().isoformat()
        except Exception:
            await update.message.reply_text("Invalid date format. Use YYYY-MM-DD or similar.")
            return
    # New behavior: generate report only for staff where shop_id = 1 (Herat Teleshop)
    try:
        rows = await models.get_all_sales_by_date_for_shop(db_path, parsed_date, shop_id=1)
        if not rows:
            await update.message.reply_text(f"No sales found for {parsed_date} in Herat Teleshop.")
            return
        import pandas as _pd
        from pathlib import Path as _Path
        from telegram import InputFile as _InputFile

        # Convert to DataFrame and rename/select columns
        df = _pd.DataFrame([{
            'Mobile': r['number'] or r.get('gsm_number', ''),  # Number field or gsm_field
            'Amount': r['recharge_amount'],  # recharge_amount field
            'Date': r['report_date'],  # report_date field
            'Employee Name': r['employee']  # comes from st.name AS employee in query
        } for r in rows])

        # Ensure columns are in the exact order requested
        df = df[['Mobile', 'Amount', 'Date', 'Employee Name']]
        
        output_dir = _Path("reports")
        output_dir.mkdir(exist_ok=True)
        path = output_dir / f"recharge_report_herat_{parsed_date}.xlsx"
        df.to_excel(path, index=False)
        with open(path, "rb") as f:
            await update.message.reply_document(document=_InputFile(f, filename=path.name))
    except Exception as ex:
        logger.exception("report generation failed: %s", ex)
        await update.message.reply_text("Failed generating report. Try again later.")


@_require_admin
async def send_file_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: /send_file <username>

    Send an attached document to a single staff member. Supports two-step flow:
    - Run `/send_file <username>` then attach a file in the next message.
    - Or run the command with a document attached.
    """
    db_path = os.getenv("DB_PATH", "teleshop.db")
    if len(context.args) < 1:
        # allow two-step: admin will run command then attach file
        await update.message.reply_text("Usage: /send_file <username> (attach a document or run then attach)")
        return
    target = context.args[0]
    doc = update.message.document
    # If no document present, set two-step awaiting flag
    if not doc:
        context.user_data["awaiting_send_file"] = {"mode": "single", "target": target}
        await update.message.reply_text(f"Please attach the file to send to {target} in your next message.")
        return

    # Immediate send: download and push
    try:
        file = await doc.get_file()
        b = await file.download_as_bytearray()
    except Exception:
        logger.exception("Failed to download document for send_file")
        await update.message.reply_text("Failed to download attached file. Try again.")
        return

    # size check
    max_bytes = 48 * 1024 * 1024
    if len(b) > max_bytes:
        await update.message.reply_text("File too large to send (limit ~48MB).")
        return

    staff = await models.get_staff_by_username(db_path, target)
    if not staff or not staff.get('chat_id'):
        await update.message.reply_text("Target not found or has no chat_id registered.")
        return

    try:
        from telegram import InputFile as _InputFile
        await context.bot.send_document(chat_id=int(staff.get('chat_id')), document=_InputFile(bytes(b), filename=doc.file_name or 'file'))
        await update.message.reply_text(f"File sent to {target}.")
    except Exception:
        logger.exception("send_file failed")
        await update.message.reply_text("Failed to send file. See logs for details.")


@_require_admin
async def send_file_to_all_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: /sendfiletoall

    Broadcast an attached document to all staff chat_ids. Supports two-step flow.
    """
    db_path = os.getenv("DB_PATH", "teleshop.db")
    doc = update.message.document
    if not doc:
        context.user_data["awaiting_send_file"] = {"mode": "all"}
        await update.message.reply_text("Please attach the file to broadcast to all staff in your next message.")
        return

    try:
        file = await doc.get_file()
        b = await file.download_as_bytearray()
    except Exception:
        logger.exception("Failed to download document for broadcast")
        await update.message.reply_text("Failed to download attached file. Try again.")
        return

    max_bytes = 48 * 1024 * 1024
    if len(b) > max_bytes:
        await update.message.reply_text("File too large to broadcast (limit ~48MB).")
        return

    chat_ids = await models.get_all_staff_chat_ids(db_path)
    if not chat_ids:
        await update.message.reply_text("No staff chat_ids registered to broadcast.")
        return

    sent = 0
    failed = 0
    from telegram import InputFile as _InputFile
    for cid in chat_ids:
        try:
            await context.bot.send_document(chat_id=int(cid), document=_InputFile(bytes(b), filename=doc.file_name or 'file'))
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1
            logger.exception("Broadcast send failed for %s", cid)

    await update.message.reply_text(f"Broadcast complete: sent={sent}, failed={failed}")


async def all_sales_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_path = os.getenv("DB_PATH", "teleshop.db")
    date_str = context.args[0] if context.args else None
    parsed_date = None
    if date_str:
        try:
            parsed_date = dateutil.parser.parse(date_str).date().isoformat()
        except Exception:
            await update.message.reply_text("Invalid date format. Use YYYY-MM-DD or similar.")
            return
    rows = await models.get_all_sales_by_date(db_path, parsed_date)
    if not rows:
        await update.message.reply_text("No sales for this date.")
        return
    lines = [f"{r['id']}: {r['employee']} ({r['username']}) — {r['item_code']} {r['number']} pcs, recharge {r['recharge_amount']}" for r in rows]
    # paginate if too long (telegram message max ~4096 chars); simple chunking by lines
    page_size = 30
    total_pages = math.ceil(len(lines) / page_size)
    for i in range(total_pages):
        chunk = lines[i * page_size:(i + 1) * page_size]
        header = f"All sales for {parsed_date or 'today'} (page {i+1}/{total_pages}):\n"
        await update.message.reply_text(header + "\n".join(chunk))


async def total_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Generate a daily text report per employee and per shop.
    Usage: /total [date]
    """
    db_path = os.getenv("DB_PATH", "teleshop.db")
    date_str = context.args[0] if context.args else None
    parsed_date = None
    if date_str:
        try:
            parsed_date = dateutil.parser.parse(date_str).date().isoformat()
        except Exception:
            await update.message.reply_text("Invalid date format. Use YYYY-MM-DD or similar.")
            return
    else:
        parsed_date = datetime.date.today().isoformat()

    # get all sales for the date (we'll aggregate by staff/shop)
    rows = await models.get_all_sales_by_date(db_path, parsed_date)
    if not rows:
        await update.message.reply_text(f"No sales found for {parsed_date}.")
        return

    # Aggregate per employee and per shop
    # We'll need staff -> shop mapping
    conn = models.get_connection(db_path)
    cur = conn.cursor()
    cur.execute("SELECT id, username, shop_id FROM staff")
    staff_rows = cur.fetchall()
    staff_map = {r['username']: r['shop_id'] for r in staff_rows}
    # get shop names
    cur.execute("SELECT id, name FROM shops")
    shop_rows = cur.fetchall()
    shop_names = {r['id']: (r['name'] or f"Shop {r['id']}") for r in shop_rows}

    per_employee = {}
    per_shop = {}
    # also track recharge totals per shop
    per_shop_recharge = {}
    # track recharge totals per employee for total AF calculation
    per_employee_recharge = {}
    for r in rows:
        username = r['username']
        code = (r.get('item_code') or '').lower()
        recharge_amt = float(r.get('recharge_amount') or 0)
        per_employee_recharge[username] = per_employee_recharge.get(username, 0.0) + recharge_amt
        # Count sales, not sum GSM numbers
        if username not in per_employee:
            per_employee[username] = {'SIM': 0, 'SWAP': 0, 'Credit50': 0, 'Credit100': 0}
        if code in ('sim', 'simcard', 'sim_card'):
            per_employee[username]['SIM'] += 1
        elif code == 'swap':
            per_employee[username]['SWAP'] += 1
        elif code in ('credit50', 'credit_50', 'credit-50'):
            per_employee[username]['Credit50'] += int(r.get('number') or 0)
        elif code in ('credit100', 'credit_100', 'credit-100'):
            per_employee[username]['Credit100'] += int(r.get('number') or 0)

        shop = staff_map.get(username) or 0
        if shop not in per_shop:
            per_shop[shop] = {'SIM': 0, 'SWAP': 0, 'Credit50': 0, 'Credit100': 0}
            per_shop_recharge[shop] = 0.0
        if code in ('sim', 'simcard', 'sim_card'):
            per_shop[shop]['SIM'] += 1
        elif code == 'swap':
            per_shop[shop]['SWAP'] += 1
        elif code in ('credit50', 'credit_50', 'credit-50'):
            per_shop[shop]['Credit50'] += int(r.get('number') or 0)
        elif code in ('credit100', 'credit_100', 'credit-100'):
            per_shop[shop]['Credit100'] += int(r.get('number') or 0)
        # accumulate recharge at shop level
        per_shop_recharge[shop] = per_shop_recharge.get(shop, 0.0) + recharge_amt

    # Also aggregate daily registrations per user and per shop
    cur.execute("SELECT dr.staff_id, st.username, dr.reg_count FROM daily_regs dr JOIN staff st ON dr.staff_id = st.id WHERE dr.date = ?", (parsed_date,))
    reg_rows = cur.fetchall()
    # Ensure employees present in regs but not in sales are included
    per_employee_regs = {u: 0 for u in per_employee.keys()}
    per_shop_regs = {sid: {'regs': 0} for sid in per_shop.keys()}
    for rr in reg_rows:
        uname = rr['username']
        if uname not in per_employee:
            per_employee[uname] = {'SIM': 0, 'SWAP': 0, 'Credit50': 0, 'Credit100': 0}
        shop_id = staff_map.get(uname) or 0
        if shop_id not in per_shop:
            per_shop[shop_id] = {'SIM': 0, 'SWAP': 0, 'Credit50': 0, 'Credit100': 0}
    total_regs = 0
    for rr in reg_rows:
        regs = int(rr['reg_count'] or 0)
        total_regs += regs
        uname = rr['username']
        per_employee_regs.setdefault(uname, 0)
        per_employee_regs[uname] += regs
        shop_id = staff_map.get(uname) or 0
        per_shop_regs.setdefault(shop_id, {'regs': 0})
        per_shop_regs[shop_id]['regs'] += regs

    # Build a structured report grouped by shop per the requested format.
    # Get full staff info including names
    cur.execute("SELECT id, username, name, shop_id FROM staff")
    staff_rows = cur.fetchall()
    staff_info = {r['username']: {'shop_id': r['shop_id'], 'name': r['name'] or r['username']} for r in staff_rows}
    # get detailed shop names
    cur.execute("SELECT id, name FROM shops")
    shop_rows = cur.fetchall()
    shop_names = {r['id']: r['name'] for r in shop_rows}
    shop_ids_by_name = {(r['name'] or '').lower(): r['id'] for r in shop_rows}

    # Helper to sum shop metrics for a single shop id
    def sum_single_shop(shop_id):
        res = {'SIM': 0, 'SWAP': 0, 'Credit50': 0, 'Credit100': 0, 'Recharge': 0.0, 'REG': 0}
        v = per_shop.get(shop_id, {'SIM': 0, 'SWAP': 0, 'Credit50': 0, 'Credit100': 0})
        res.update(v)
        res['Recharge'] = per_shop_recharge.get(shop_id, 0.0)
        res['REG'] = per_shop_regs.get(shop_id, {}).get('regs', 0)
        return res

    # Helper to format employee line with proper alignment
    def format_employee_line(name, stats, regs, total_af: float = 0.0):
        parts = [f"👤 {name:<25}"]
        parts.append(f"SIM: {stats['SIM']:<3}")
        if stats['SWAP'] > 0:
            parts.append(f"SWAP: {stats['SWAP']:<3}")
        if regs > 0:
            parts.append(f"REG: {regs:<3}")
        if stats['Credit50'] > 0:
            parts.append(f"C50: {stats['Credit50']:<3}")
        if stats['Credit100'] > 0:
            parts.append(f"C100: {stats['Credit100']:<3}")
        # Append total AF if provided
        if total_af and total_af > 0:
            parts.append(f"[total {total_af:.0f} AF]")
        return " | ".join(parts)

    # Build lines
    lines = [f"📊 Sales Totals for {parsed_date}:", ""]

    # Function to render employee block for a shop id list
    def render_employees_block(title, shop_id_list):
        block = []
        block.append(f"👥 Employees ({title}):")
        # find employees in these shops and sort by name
        users = [(u, staff_info[u]['name']) for u, info in staff_info.items() 
                if info['shop_id'] in shop_id_list]
        users.sort(key=lambda x: x[1])  # sort by name
        if not users:
            block.append("(no employees)")
            return block
        for username, name in users:
            v = per_employee.get(username, {'SIM': 0, 'SWAP': 0, 'Credit50': 0, 'Credit100': 0})
            regs = per_employee_regs.get(username, 0)
            if v['SIM'] > 0 or v['SWAP'] > 0 or regs > 0 or v['Credit50'] > 0 or v['Credit100'] > 0:
                # compute total AF per employee: SIM*100 + SWAP*50 + C50*50 + C100*100 + recharge amounts
                total_af = (
                    v.get('SIM', 0) * 100
                    + v.get('SWAP', 0) * 50
                    + v.get('Credit50', 0) * 50
                    + v.get('Credit100', 0) * 100
                    + per_employee_recharge.get(username, 0.0)
                )
                block.append(format_employee_line(name, v, regs, total_af))
        block.append("")  # add spacing after each employee block
        return block

    # Function to render individual shop line with proper formatting
    def format_shop_line(shop_name, stats):
        parts = [f"🏪 {shop_name:<20}"]
        parts.append(f"SIM: {stats['SIM']:<4}")
        if stats['SWAP'] > 0:
            parts.append(f"SWAP: {stats['SWAP']:<4}")
        if stats['REG'] > 0:
            parts.append(f"REG: {stats['REG']:<4}")
        if stats['Credit50'] > 0:
            parts.append(f"C50: {stats['Credit50']:<4}")
        if stats['Credit100'] > 0:
            parts.append(f"C100: {stats['Credit100']:<4}")
        if stats['Recharge'] > 0:
            parts.append(f"Recharge: {stats['Recharge']:.0f} AF")
        return " | ".join(parts)

    # Herat shops individually first
    herat_shops = [(sid, name) for sid, name in shop_names.items() 
                   if name and 'herat' in name.lower()]
    if herat_shops:
        lines += render_employees_block('Herat', [sid for sid, _ in herat_shops])
        lines.append("")

    # Other shops: Farah, Ghor, Badghis, Refugee Camp (Islam Qala)
    for region in ['Farah', 'Ghor', 'Badghis']:
        shop_ids = [sid for sid, name in shop_names.items() 
                   if name and region.lower() in name.lower()]
        if shop_ids:
            lines += render_employees_block(region, shop_ids)
            lines.append("")

    # Refugee Camp/Islam Qala
    islam_qala_ids = [sid for sid, name in shop_names.items() 
                     if name and ('islam' in name.lower() or 'refugee' in name.lower())]
    if islam_qala_ids:
        lines += render_employees_block('Refugee Camp (Islam Qala)', islam_qala_ids)
        lines.append("")

    # Detailed shop section
    lines.append("🏪 Individual Shops:")
    
    # Show Herat shops individually
    for shop_id, shop_name in herat_shops:
        stats = sum_single_shop(shop_id)
        if any(stats[k] > 0 for k in ['SIM', 'SWAP', 'REG', 'Credit50', 'Credit100']):
            lines.append(format_shop_line(shop_name, stats))
    lines.append("")

    # Show other shops individually
    for region in ['Farah', 'Ghor', 'Badghis']:
        region_shops = [(sid, name) for sid, name in shop_names.items() 
                       if name and region.lower() in name.lower()]
        for shop_id, shop_name in region_shops:
            stats = sum_single_shop(shop_id)
            if any(stats[k] > 0 for k in ['SIM', 'SWAP', 'REG', 'Credit50', 'Credit100']):
                lines.append(format_shop_line(shop_name, stats))
                lines.append("")

    # Show Islam Qala/Refugee Camp shops
    for shop_id in islam_qala_ids:
        shop_name = shop_names.get(shop_id, 'Refugee Camp')
        stats = sum_single_shop(shop_id)
        if any(stats[k] > 0 for k in ['SIM', 'SWAP', 'REG', 'Credit50', 'Credit100']):
            lines.append(format_shop_line(shop_name, stats))
    lines.append("")

    # Totals section - keep existing format
    lines.append(f"Total REG: {total_regs}")
    lines.append("")

    # Helper to sum all metrics for a set of shop IDs
    def sum_shops(ids):
        res = {'SIM': 0, 'SWAP': 0, 'Credit50': 0, 'Credit100': 0, 'Recharge': 0.0, 'REG': 0}
        for sid in ids:
            v = per_shop.get(sid, {'SIM': 0, 'SWAP': 0, 'Credit50': 0, 'Credit100': 0})
            res['SIM'] += v.get('SIM', 0)
            res['SWAP'] += v.get('SWAP', 0)
            res['Credit50'] += v.get('Credit50', 0)
            res['Credit100'] += v.get('Credit100', 0)
            res['Recharge'] += per_shop_recharge.get(sid, 0.0)
            res['REG'] += per_shop_regs.get(sid, {}).get('regs', 0)
        return res

    # Zone-level summary - keep exactly as is
    herat_ids = [sid for sid, name in shop_names.items() if name and 'herat' in name.lower()]
    farah_ids = [sid for sid, name in shop_names.items() if name and 'farah' in name.lower()]
    ghor_ids = [sid for sid, name in shop_names.items() if name and 'ghor' in name.lower()]
    badghis_ids = [sid for sid, name in shop_names.items() if name and 'badghis' in name.lower()]
    
    herat_sim = sum_shops(herat_ids)['SIM']
    farah_sim = sum_shops(farah_ids)['SIM']
    ghor_sim = sum_shops(ghor_ids)['SIM']
    badghis_sim = sum_shops(badghis_ids)['SIM']
    refugee_sim = sum_shops(islam_qala_ids)['SIM'] if islam_qala_ids else 0
    zone_total = herat_sim + farah_sim + ghor_sim + badghis_sim + refugee_sim
    
    date_display = parsed_date.replace('-', '.')
    lines.append(f"Sales Report – {date_display} -Northwest Zone – Afghan Telecom:")
    lines.append(f"1. Herat : {herat_sim} total SIM cards")
    lines.append(f"2. Farah: {farah_sim} total SIM cards")
    lines.append(f"3. Ghor: {ghor_sim} total SIM cards")
    lines.append(f"4. Badghis: {badghis_sim} total SIM cards")
    lines.append(f"5. Refugee Camp (Islam Qala): {refugee_sim} SIM cards")
    lines.append(f"-Total Sales -({zone_total})")

    await update.message.reply_text("\n".join(lines))


async def inventory_summary_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_path = os.getenv("DB_PATH", "teleshop.db")
    s = await models.inventory_summary(db_path)
    # compute AFN totals
    sim_af = s['sim'] * 100
    swap_af = s['swap'] * 50
    c50_af = s['credit_50'] * 50
    c100_af = s['credit_100'] * 100
    grand = sim_af + swap_af + c50_af + c100_af
    await update.message.reply_text(f"Inventory summary — SIM {s['sim']} ({sim_af} AF) | SWAP {s['swap']} ({swap_af} AF) | C50 {s['credit_50']} ({c50_af} AF) | C100 {s['credit_100']} ({c100_af} AF)\nGrand Total AF: {grand}")


async def promote_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_path = os.getenv("DB_PATH", "teleshop.db")
    if len(context.args) < 1:
        await update.message.reply_text("Usage: /promote <staff_username>")
        return
    staff_username = context.args[0]
    ok = await models.set_admin(db_path, staff_username, True)
    if not ok:
        await update.message.reply_text("Failed to promote user. Check username.")
        return
    logger.info("/promote by %s: promoted %s", update.effective_user.username, staff_username)
    await update.message.reply_text(f"User {staff_username} promoted to admin.")


async def weekly_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Generate weekly (date-range) per-employee Excel report.
    Usage: /weekly YYYY-MM-DD YYYY-MM-DD
    """
    db_path = os.getenv("DB_PATH", "teleshop.db")
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /weekly YYYY-MM-DD YYYY-MM-DD")
        return
    try:
        start = dateutil.parser.parse(context.args[0]).date()
        end = dateutil.parser.parse(context.args[1]).date()
    except Exception:
        await update.message.reply_text("Invalid date(s). Use YYYY-MM-DD format.")
        return
    if end < start:
        await update.message.reply_text("End date must be same or after start date.")
        return
    # limit to 10 days
    delta = (end - start).days + 1
    if delta > 10:
        await update.message.reply_text("Date range too long; max 10 days allowed.")
        return

    # build date list
    dates = [(start + datetime.timedelta(days=i)).isoformat() for i in range(delta)]

    try:
        rows = await models.get_sales_counts_by_staff_dates(db_path, start.isoformat(), end.isoformat())
    except Exception as ex:
        logger.exception("Failed to fetch weekly data: %s", ex)
        await update.message.reply_text("Failed to generate weekly report due to internal error.")
        return

    # Organize data by username and date
    users = sorted({r['username'] for r in rows})
    # include users with regs only
    # fetch regs separately (already included in helper)
    data_map = {u: {d: {'SIM': 0, 'REG': 0, 'SWAP': 0} for d in dates} for u in users}
    for r in rows:
        u = r['username']
        d = r['report_date']
        if u not in data_map:
            data_map[u] = {dd: {'SIM': 0, 'REG': 0, 'SWAP': 0} for dd in dates}
        if d not in data_map[u]:
            # skip dates outside range
            continue
        data_map[u][d]['SIM'] = int(r.get('sim_count') or 0)
        data_map[u][d]['SWAP'] = int(r.get('swap_count') or 0)
        data_map[u][d]['REG'] = int(r.get('reg_count') or 0)

    # Build Excel with pandas: rows per employee with three subrows (SIM, REG, SWAP)
    try:
        import pandas as _pd
        from io import BytesIO as _BytesIO
    except Exception:
        await update.message.reply_text("Server missing pandas dependency; cannot generate Excel.")
        return

    # Compose a table: first column Employee, second column Metric (SIM/REG/SWAP), then one column per date
    cols = ['Employee', 'Metric'] + [d for d in dates]
    records = []
    for u in sorted(data_map.keys()):
        # SIM row
        row_sim = [u, 'SIM'] + [data_map[u][d]['SIM'] for d in dates]
        row_reg = ['', 'REG'] + [data_map[u][d]['REG'] for d in dates]
        row_swap = ['', 'SWAP'] + [data_map[u][d]['SWAP'] for d in dates]
        records.append(row_sim)
        records.append(row_reg)
        records.append(row_swap)

    df = _pd.DataFrame(records, columns=cols)
    buf = _BytesIO()
    out_name = f"weekly_report_{start.isoformat()}_to_{end.isoformat()}.xlsx"
    try:
        with _pd.ExcelWriter(buf, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Weekly')
        buf.seek(0)
    except Exception as ex:
        logger.exception("Failed to write Excel: %s", ex)
        await update.message.reply_text("Failed to generate Excel file.")
        return

    try:
        await update.message.reply_document(document=buf.getvalue(), filename=out_name)
    except Exception as ex:
        logger.exception("Failed to send weekly Excel: %s", ex)
        await update.message.reply_text("Failed to send weekly report. Ensure bot can send files.")


async def init_bot() -> Any:
    """Initialize the Telegram bot with all command handlers."""
    from .commands import Command, register_command

    token = os.getenv("TELEGRAM_TOKEN")
    if not token:
        raise RuntimeError("TELEGRAM_TOKEN is not set in .env")

    app = ApplicationBuilder().token(token).build()

    # Register user commands
    register_command(Command("start", "Register and capture your chat for notifications"))
    register_command(Command("help", "Show this help message"))
    register_command(Command("summary", "Show your current stock summary"))
    register_command(Command("my_stock", "Show your stock counts"))
    register_command(Command("my_sales", "Show your sales for a date", usage="[date]"))
    register_command(Command("missing_upload", "Upload sales Excel for a past date", usage="YYYY-MM-DD"))

    # Register admin commands
    register_command(Command("add_stock", "Add stock to a user", usage="<user> <item> <qty>", admin_only=True, category="Inventory"))
    register_command(Command("remove_stock", "Remove stock from a user", usage="<user> <item> <qty>", admin_only=True, category="Inventory"))
    register_command(Command("view_stock", "View a user's stock", usage="<user>", admin_only=True, category="Inventory"))
    register_command(Command("list_inventory", "List all inventories", admin_only=True, category="Inventory"))
    register_command(Command("delete_sale", "Delete sales and revert inventory", usage="<id>|<user> <date>", admin_only=True))

    # Add command handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("summary", summary))
    app.add_handler(CommandHandler("my_stock", my_stock))
    app.add_handler(CommandHandler("my_sales", my_sales))
    app.add_handler(CommandHandler("missing_upload", missing_upload))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    # admin handlers (wrapped with admin check in decorator usage)
    app.add_handler(CommandHandler("add_stock", _require_admin(add_stock_cmd)))
    app.add_handler(CommandHandler("remove_stock", _require_admin(remove_stock_cmd)))
    app.add_handler(CommandHandler("view_stock", _require_admin(view_stock_cmd)))
    app.add_handler(CommandHandler("list_inventory", _require_admin(list_inventory_cmd)))
    app.add_handler(CommandHandler("delete_sale", _require_admin(delete_sale_cmd)))
    # Register more admin commands
    register_command(Command("report", "Download daily recharge report", usage="[date]", admin_only=True, category="Reports"))
    register_command(Command("all_sales", "List all sales and credits", usage="[date]", admin_only=True, category="Reports"))
    register_command(Command("inventory_summary", "Show total inventory values", admin_only=True, category="Inventory"))
    register_command(Command("promote", "Promote a user to admin", usage="<user>", admin_only=True, category="Admin"))
    register_command(Command("transfer_stock", "Transfer stock to a user", usage="<user> <item> <qty>", admin_only=True, category="Inventory"))
    register_command(Command("total", "Generate daily text report per employee and shop", usage="[date]", admin_only=True, category="Reports"))
    register_command(Command("register_me", "(Re)register chat_id for notifications"))
    register_command(Command("msg_user", "Send message to specific user", usage="<username> <message>", admin_only=True, category="Admin"))
    register_command(Command("msg_all", "Broadcast message to all users", usage="<message>", admin_only=True, category="Admin"))
    # file send commands
    register_command(Command("send_file", "Send a document to a user (attach file)", usage="<username>", admin_only=True, category="Admin"))
    register_command(Command("sendfiletoall", "Broadcast a document to all users (attach file)", admin_only=True, category="Admin"))
    
    # Register borrow/money commands
    register_command(Command("borrow_add", "Record a money transaction", usage="<n> <amount> [note]", admin_only=True, category="Money"))
    register_command(Command("borrow_list", "List your recorded transactions", admin_only=True, category="Money"))
    register_command(Command("borrow_summary", "Summary totals per person", usage="[start] [end]", admin_only=True, category="Money"))
    
    # Register weekly report commands
    register_command(Command("weekly_regs", "Aggregate daily registrations", usage="[start] [end]", admin_only=True, category="Reports"))
    register_command(Command("weekly", "Generate weekly Excel report", usage="YYYY-MM-DD YYYY-MM-DD", admin_only=True, category="Reports"))
    
    # Register backoffice commands
    register_command(Command("backoffice_add", "Add central backoffice stock", usage="<item> <qty>", admin_only=True, category="Backoffice"))
    register_command(Command("backoffice_list", "List backoffice stock", admin_only=True, category="Backoffice"))
    register_command(Command("transfer_backoffice", "Transfer from backoffice", usage="<user> <item> <qty>", admin_only=True, category="Backoffice"))
    
    # Register SIM batch commands
    register_command(Command("import_pickup", "Import SIM pickup list Excel", admin_only=True, category="SIM"))
    register_command(Command("transfer_sims", "Transfer SIMs", usage="<mode> <params> <target>", admin_only=True, category="SIM"))
    register_command(Command("sim_status", "Query SIM status/location", usage="<gsm|box|carton> <value>", admin_only=True, category="SIM"))

    # Add handlers for all registered commands
    app.add_handler(CommandHandler("report", _require_admin(report_cmd)))
    app.add_handler(CommandHandler("all_sales", _require_admin(all_sales_cmd)))
    app.add_handler(CommandHandler("inventory_summary", _require_admin(inventory_summary_cmd)))
    app.add_handler(CommandHandler("promote", _require_admin(promote_cmd)))
    app.add_handler(CommandHandler("transfer_stock", transfer_stock_cmd))
    app.add_handler(CommandHandler("total", _require_admin(total_cmd)))
    app.add_handler(CommandHandler("register_me", register_me))
    app.add_handler(CommandHandler("msg_user", _require_admin(msg_user_cmd)))
    app.add_handler(CommandHandler("msg_all", _require_admin(msg_all_cmd)))
    # file send handlers
    app.add_handler(CommandHandler("send_file", _require_admin(send_file_cmd)))
    app.add_handler(CommandHandler("sendfiletoall", _require_admin(send_file_to_all_cmd)))
    app.add_handler(CommandHandler("borrow_add", _require_admin(borrow_add_cmd)))
    app.add_handler(CommandHandler("borrow_list", _require_admin(borrow_list_cmd)))
    app.add_handler(CommandHandler("borrow_summary", _require_admin(borrow_summary_cmd)))
    app.add_handler(CommandHandler("weekly_regs", _require_admin(weekly_regs_cmd)))
    app.add_handler(CommandHandler("weekly", _require_admin(weekly_cmd)))
    app.add_handler(CommandHandler("backoffice_add", _require_admin(backoffice_add_cmd)))
    app.add_handler(CommandHandler("backoffice_list", _require_admin(backoffice_list_cmd)))
    app.add_handler(CommandHandler("transfer_backoffice", _require_admin(transfer_backoffice_cmd)))
    app.add_handler(CommandHandler("import_pickup", _require_admin(handle_pickup)))
    app.add_handler(CommandHandler("transfer_sims", _require_admin(transfer_sims_cmd)))
    app.add_handler(CommandHandler("sim_status", _require_admin(sim_status_cmd)))

    return app
@_require_admin
async def transfer_stock_cmd(update, context):
    db_path = os.getenv("DB_PATH", "teleshop.db")
    if len(context.args) < 3:
        await update.message.reply_text("Usage: /transfer_stock <employee_username> <item> <qty>")
        return

    to_username = context.args[0]
    item = context.args[1]
    try:
        qty = int(context.args[2])
    except ValueError:
        await update.message.reply_text("Quantity must be a number.")
        return

    # use the actual caller's username (fallback to id string)
    from_username = update.effective_user.username or str(update.effective_user.id)

    try:
        ok = await models.transfer_stock(db_path, from_username, to_username, item, qty)
    except Exception as ex:
        logger.exception("transfer_stock failed: %s", ex)
        await update.message.reply_text("Transfer failed due to internal error.")
        return
    if not ok:
        await update.message.reply_text("Failed. Check your stock, item name, or employee username.")
        return

    logger.info("/transfer_stock by %s: %s -> %s %s", from_username, item, to_username, qty)
    await update.message.reply_text(f"Transferred {qty} {item} from you to {to_username}.")

    # Notify receiving employee if they have a stored chat_id
    try:
        staff = await models.get_staff_by_username(db_path, to_username)
        if staff and staff.get("chat_id"):
            sent = await send_message_safe(context.bot, staff.get("chat_id"), f"📦 You have received {qty} {item} from admin {from_username}.")
            if not sent:
                logger.info("Could not deliver transfer notification to %s (chat_id=%s)", to_username, staff.get("chat_id"))
        else:
            logger.info("Recipient %s has no chat_id on file; skipping notification", to_username)
    except Exception:
        logger.exception("Error while attempting to notify recipient about transfer")

