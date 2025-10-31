"""SQLite models and helpers for Teleshop system.

This module provides synchronous SQLite functions and wraps blocking calls
with asyncio.to_thread from the bot so they don't block the event loop.
"""
from __future__ import annotations

import sqlite3
import os
import time
from typing import Optional, List, Dict, Any
import datetime
import asyncio
import logging


logger = logging.getLogger(__name__)


DB_SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS shops (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    location TEXT
);

CREATE TABLE IF NOT EXISTS staff (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE,
    name TEXT,
    shop_id INTEGER,
    position TEXT,
    phone TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(shop_id) REFERENCES shops(id)
);

CREATE TABLE IF NOT EXISTS inventory (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    staff_id INTEGER NOT NULL,
    sim INTEGER DEFAULT 0,
    swap INTEGER DEFAULT 0,
    credit_50 INTEGER DEFAULT 0,
    credit_100 INTEGER DEFAULT 0,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(staff_id) REFERENCES staff(id)
);

CREATE TABLE IF NOT EXISTS sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    staff_id INTEGER NOT NULL,
    report_date TEXT NOT NULL,
    item_code TEXT,
    number INTEGER,
    contact_number TEXT,
    recharge_amount REAL DEFAULT 0,
    notes TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(staff_id) REFERENCES staff(id)
);
 
CREATE TABLE IF NOT EXISTS daily_regs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    staff_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    reg_count INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(staff_id) REFERENCES staff(id)
);

CREATE TABLE IF NOT EXISTS borrow_list (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    admin_id TEXT NOT NULL,
    person_name TEXT NOT NULL,
    amount REAL NOT NULL,
    date TEXT DEFAULT (DATE('now')),
    note TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS inventory_journal (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    staff_id INTEGER,
    item TEXT NOT NULL,
    change_amount INTEGER NOT NULL,
    change_type TEXT,
    source TEXT,
    timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
    source_ref INTEGER,
    FOREIGN KEY(staff_id) REFERENCES staff(id)
);

CREATE TABLE IF NOT EXISTS backoffice_stock (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    item TEXT UNIQUE NOT NULL,
    quantity INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS sim_batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    carton_no TEXT,
    box_no TEXT,
    gsm_number TEXT UNIQUE NOT NULL,
    iccid TEXT,
    type TEXT,
    current_location TEXT DEFAULT 'Backoffice',
    status TEXT DEFAULT 'in_stock',
    date_added TEXT DEFAULT CURRENT_TIMESTAMP,
    date_sent TEXT,
    note TEXT
);
"""


MIGRATIONS = [
    # add is_admin column to staff if missing
    (
        "alter_staff_add_is_admin",
        "ALTER TABLE staff ADD COLUMN is_admin INTEGER DEFAULT 0",
    ),
    # add chat_id column to store Telegram chat id for notifications
    (
        "alter_staff_add_chat_id",
        "ALTER TABLE staff ADD COLUMN chat_id TEXT",
    ),
    (
        "alter_inventory_journal_add_source_ref",
        "ALTER TABLE inventory_journal ADD COLUMN source_ref INTEGER",
    ),
    (
        "add_sim_batches_table",
        "CREATE TABLE IF NOT EXISTS sim_batches (id INTEGER PRIMARY KEY AUTOINCREMENT, carton_no TEXT, box_no TEXT, gsm_number TEXT UNIQUE NOT NULL, iccid TEXT, type TEXT, current_location TEXT DEFAULT 'Backoffice', status TEXT DEFAULT 'in_stock', date_added TEXT DEFAULT CURRENT_TIMESTAMP, date_sent TEXT, note TEXT)",
    ),
    (
        "alter_sales_add_contact_number",
        "ALTER TABLE sales ADD COLUMN contact_number TEXT",
    ),
    (
        "add_daily_totals_table",
        "CREATE TABLE IF NOT EXISTS daily_totals (id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT NOT NULL, shop_id INTEGER, total_amount REAL NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP)",
    ),
]


def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def verify_table_exists(cur: sqlite3.Cursor, table_name: str) -> bool:
    """Check if a table exists in the database."""
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    return bool(cur.fetchone())

def get_table_columns(cur: sqlite3.Cursor, table_name: str) -> List[str]:
    """Get list of column names for a table."""
    cur.execute(f"PRAGMA table_info({table_name})")
    return [row["name"] for row in cur.fetchall()]

def get_required_tables_and_columns() -> Dict[str, List[str]]:
    """Extract required table and column names from DB_SCHEMA."""
    tables = {}
    current_table = None
    for line in DB_SCHEMA.split("\n"):
        line = line.strip()
        if line.startswith("CREATE TABLE IF NOT EXISTS"):
            # Extract table name
            current_table = line.split()[5].strip('(').strip('`')
            tables[current_table] = []
        elif line and current_table and "," in line and not line.startswith("--"):
            # Extract column name
            col_name = line.split()[0].strip(',').strip('`')
            if col_name not in ("PRIMARY", "FOREIGN"):
                tables[current_table].append(col_name)
    return tables

async def init_db(db_path: str) -> None:
    """Initialize database schema and verify table/column existence.
    
    This function:
    1. Creates the database and parent directory if needed
    2. Verifies all required tables exist
    3. Verifies all required columns exist
    4. Applies any pending migrations
    
    The process is safe and non-destructive - it will not drop or modify
    existing tables/columns, only add missing ones.
    """
    def _init():
        # Ensure parent directory exists
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        
        # Connect and enable foreign keys
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("PRAGMA foreign_keys = ON")

        # Get required table structure
        required_tables = get_required_tables_and_columns()

        # First create any missing tables
        cur.executescript(DB_SCHEMA)
        conn.commit()

        # Now verify each table and its columns exist
        for table_name, required_cols in required_tables.items():
            if not verify_table_exists(cur, table_name):
                logger.error(f"Critical table {table_name} missing after schema init!")
                continue

            # Get actual columns
            actual_cols = set(get_table_columns(cur, table_name))
            required_cols = set(required_cols)
            
            # Log any missing columns
            missing = required_cols - actual_cols
            if missing:
                logger.warning(f"Table {table_name} missing columns: {missing}")

        # Run migrations for any missing columns
        for name, sql in MIGRATIONS:
            try:
                cur.execute(sql)
                conn.commit()
                logger.debug(f"Applied migration: {name}")
            except Exception as e:
                # Ignore "duplicate column" errors
                if "duplicate column name" not in str(e):
                    logger.warning(f"Migration {name} failed: {e}")
        
        conn.commit()
        conn.close()

    await asyncio.to_thread(_init)
    logger.info("Database initialized and verified at %s", db_path)


async def ensure_staff(db_path: str, username: str, name: Optional[str] = None) -> int:
    """Ensure a staff record exists; return staff_id."""
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("SELECT id FROM staff WHERE username = ?", (username,))
        row = cur.fetchone()
        if row:
            sid = row["id"]
        else:
            cur.execute(
                "INSERT INTO staff (username, name) VALUES (?, ?)", (username, name or username)
            )
            sid = cur.lastrowid
            # create initial inventory
            cur.execute(
                "INSERT INTO inventory (staff_id, sim, swap, credit_50, credit_100) VALUES (?, 0,0,0,0)",
                (sid,)
            )
        conn.commit()
        conn.close()
        return sid

    return await asyncio.to_thread(_fn)


async def add_backoffice_stock(db_path: str, item: str, qty: int) -> bool:
    """Admin: add or increase central backoffice stock."""
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("SELECT id, quantity FROM backoffice_stock WHERE item = ?", (item,))
        row = cur.fetchone()
        if row:
            cur.execute("UPDATE backoffice_stock SET quantity = quantity + ? WHERE id = ?", (qty, row[0]))
        else:
            cur.execute("INSERT INTO backoffice_stock (item, quantity) VALUES (?, ?)", (item, qty))
        # journal the addition
        try:
            cur.execute("INSERT INTO inventory_journal (staff_id, item, change_amount, change_type, source) VALUES (?, ?, ?, ?, ?)", (None, item, int(qty), 'add', 'backoffice'))
        except Exception:
            # journaling is best-effort
            pass
        conn.commit()
        conn.close()
        return True

    return await asyncio.to_thread(_fn)


async def insert_pickup_list(db_path: str, file_bytes: bytes, filename: str, uploaded_by_username: str) -> dict:
    """Parse pickup-list Excel bytes and insert into sim_batches.
    Returns dict: {inserted: int, duplicates: int, errors: list}
    """
    from utils.excel_utils import parse_pickup_excel

    def _fn():
        inserted = 0
        duplicates = 0
        errors = []
        conn = get_connection(db_path)
        cur = conn.cursor()
        try:
            rows = parse_pickup_excel(file_bytes)
            # find uploader staff id if possible
            cur.execute("SELECT id FROM staff WHERE username = ?", (uploaded_by_username,))
            r = cur.fetchone()
            staff_id = r[0] if r else None
            for row in rows:
                try:
                    cur.execute("INSERT INTO sim_batches (carton_no, box_no, gsm_number, iccid, type, note) VALUES (?, ?, ?, ?, ?, ?)", (
                        row.get('carton_no'), row.get('box_no'), row.get('gsm_number'), row.get('iccid'), row.get('type'), filename
                    ))
                    inserted += 1
                except Exception:
                    # likely duplicate gsm_number due to UNIQUE constraint
                    duplicates += 1
                    continue
            # journal the import
            try:
                note = f"{filename} uploaded_by:{uploaded_by_username} inserted:{inserted} duplicates:{duplicates}"
                cur.execute("INSERT INTO inventory_journal (staff_id, item, change_amount, change_type, source, source_ref) VALUES (?, ?, ?, ?, ?, ?)", (staff_id, 'SIM', int(inserted), 'add', 'pickup_import', None))
            except Exception:
                # don't fail the overall op if journaling fails
                pass
            conn.commit()
        except Exception as ex:
            conn.rollback()
            errors.append(str(ex))
        finally:
            conn.close()
        return {"inserted": inserted, "duplicates": duplicates, "errors": errors}

    return await asyncio.to_thread(_fn)


async def transfer_sims_by_clause(db_path: str, where_clause: str, params: list, target_location: str, performed_by_username: str) -> dict:
    """Transfer sims matching WHERE clause to target_location (e.g., 'Shop:Teleshop_A').
    Performs transactional update; returns dict {moved: int, gsms: [list]} or raises on insufficient match.
    """
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        try:
            # Select matching sims currently in Backoffice
            q = f"SELECT id, gsm_number FROM sim_batches WHERE {where_clause} AND current_location = 'Backoffice'"
            cur.execute(q, params)
            rows = cur.fetchall()
            if not rows:
                conn.close()
                return {"moved": 0, "gsms": [], "error": "No matching SIMs found in Backoffice"}
            gsm_list = [r['gsm_number'] for r in rows]
            # perform updates in transaction
            cur.execute("BEGIN")
            now = datetime.datetime.now().isoformat()
            cur.executemany("UPDATE sim_batches SET current_location = ?, status = ?, date_sent = ? WHERE gsm_number = ?", [ (target_location, 'sent', now, g) for g in gsm_list ])
            # journal: negative for backoffice, positive for target (staff_id if applicable)
            # backoffice negative
            cur.execute(
    "INSERT INTO inventory_journal (staff_id, item, change_amount, change_type, source, source_ref) "
    "VALUES (?, ?, ?, ?, ?, ?)",
    (1, 'SIM', -len(gsm_list), 'backoffice_transfer', 'backoffice', None)
)

            # target positive - if target_location indicates an employee or admin, try to map username
            target_staff_id = None
            if target_location.startswith('Employee:') or target_location.startswith('Admin:'):
                try:
                    tname = target_location.split(':',1)[1]
                    cur.execute("SELECT id FROM staff WHERE username = ?", (tname,))
                    tr = cur.fetchone()
                    if tr:
                        target_staff_id = tr['id']
                except Exception:
                    target_staff_id = None
            cur.execute(
    "INSERT INTO inventory_journal (staff_id, item, change_amount, change_type, source, source_ref) VALUES (?, ?, ?, ?, ?, ?)",
    (target_staff_id or 1, 'SIM', len(gsm_list), 'backoffice_transfer', 'backoffice', None)
)

            conn.commit()
            conn.close()
            return {"moved": len(gsm_list), "gsms": gsm_list}
        except Exception as ex:
            conn.rollback()
            conn.close()
            raise

    return await asyncio.to_thread(_fn)


async def sim_status(db_path: str, query_type: str, query_value: str) -> dict:
    """Query sim_batches by gsm_number or box_no or carton_no. Returns details or aggregates."""
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        try:
            if query_type == 'gsm':
                cur.execute("SELECT * FROM sim_batches WHERE gsm_number = ?", (query_value,))
                row = cur.fetchone()
                if not row:
                    return {}
                # get recent journal history for SIMs (note column may not exist in older schemas)
                cur.execute("SELECT id, staff_id, item, change_amount, change_type, source, timestamp, source_ref FROM inventory_journal WHERE item = 'SIM' ORDER BY timestamp DESC LIMIT 50")
                hist = cur.fetchall()
                res = dict(row)
                res['history'] = [dict(h) for h in hist]
                return res
            elif query_type == 'box':
                cur.execute("SELECT COUNT(*) as cnt, status FROM sim_batches WHERE box_no = ? GROUP BY status", (query_value,))
                rows = cur.fetchall()
                return {r['status']: r['cnt'] for r in rows}
            elif query_type == 'carton':
                cur.execute("SELECT COUNT(*) as cnt, status FROM sim_batches WHERE carton_no = ? GROUP BY status", (query_value,))
                rows = cur.fetchall()
                return {r['status']: r['cnt'] for r in rows}
            else:
                return {}
        finally:
            conn.close()

    return await asyncio.to_thread(_fn)


async def list_backoffice_stock(db_path: str) -> List[Dict[str, Any]]:
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("SELECT id, item, quantity FROM backoffice_stock ORDER BY item")
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    return await asyncio.to_thread(_fn)


async def get_backoffice_quantity(db_path: str, item: str) -> int:
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("SELECT quantity FROM backoffice_stock WHERE item = ?", (item,))
        row = cur.fetchone()
        conn.close()
        return int(row[0]) if row else 0

    return await asyncio.to_thread(_fn)


async def transfer_backoffice(db_path: str, item: str, qty: int, to_username: str = None, to_staff_id: int = None) -> bool:
    """Transfer qty from backoffice_stock to a staff user (by username or staff_id).
    Records journal entries for both backoffice (negative) and staff (positive).
    """
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        try:
            # check backoffice qty
            cur.execute("SELECT id, quantity FROM backoffice_stock WHERE item = ?", (item,))
            row = cur.fetchone()
            if not row or int(row[1] or 0) < qty:
                conn.close()
                return False
            backoffice_id = row[0]
            # reduce backoffice
            cur.execute("UPDATE backoffice_stock SET quantity = quantity - ? WHERE id = ?", (qty, backoffice_id))
            # journal negative entry for backoffice (staff_id NULL)
            cur.execute("INSERT INTO inventory_journal (staff_id, item, change_amount, change_type, source) VALUES (?, ?, ?, ?, ?)", (None, item, -int(qty), 'backoffice_transfer', 'backoffice'))

            # credit to staff inventory: find staff id
            if to_staff_id is None and to_username:
                cur.execute("SELECT id FROM staff WHERE username = ?", (to_username,))
                s = cur.fetchone()
                if not s:
                    conn.rollback()
                    conn.close()
                    return False
                to_staff_id_local = s[0]
            else:
                to_staff_id_local = to_staff_id

            # map item to column and add qty
            col = _map_item_to_column(item)
            if not col:
                conn.rollback()
                conn.close()
                return False
            cur.execute(f"UPDATE inventory SET {col} = {col} + ? WHERE staff_id = ?", (qty, to_staff_id_local))
            # journal positive entry for the staff (reference source as backoffice)
            cur.execute("INSERT INTO inventory_journal (staff_id, item, change_amount, change_type, source) VALUES (?, ?, ?, ?, ?)", (to_staff_id_local, col, int(qty), 'backoffice_transfer', 'backoffice'))

            # update inventory timestamp
            cur.execute("UPDATE inventory SET updated_at = CURRENT_TIMESTAMP WHERE staff_id = ?", (to_staff_id_local,))

            conn.commit()
            conn.close()
            return True
        except Exception as ex:
            conn.rollback()
            conn.close()
            logger.exception("transfer_backoffice failed: %s", ex)
            return False

    return await asyncio.to_thread(_fn)


async def insert_sales_and_update_inventory(
    db_path: str,
    staff_id: int,
    report_date: str,
    entries: List[Dict[str, Any]],
) -> None:
    """
    Insert sales rows and update inventory correctly.
    - Each Excel row deducts 1 unit based on item_code.
    - Prevents negative stock (will skip sale if stock too low).
    - Saves the Excel 'Number' column properly (case-insensitive).
    """
    def _fn():
        import traceback
        # Lightweight per-staff-date file lock to prevent concurrent uploads
        # from interfering with each other. We use atomic file creation (O_EXCL)
        # to acquire the lock; this works across processes on Windows and Unix.
        def _lock_path(db_path: str, staff_id: int, report_date: str) -> str:
            base = os.path.dirname(db_path) or "."
            lock_dir = os.path.join(base, "locks")
            try:
                os.makedirs(lock_dir, exist_ok=True)
            except Exception:
                pass
            # sanitize report_date for filesystem
            safe_date = report_date.replace('/', '_').replace(':', '_')
            return os.path.join(lock_dir, f"upload_{staff_id}_{safe_date}.lock")

        def _acquire_lock(lockfile: str, timeout: float = 10.0):
            start = time.time()
            while True:
                try:
                    # O_CREAT|O_EXCL ensures atomic creation; fails if exists
                    fd = os.open(lockfile, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                    # write pid/timestamp for debugging
                    try:
                        os.write(fd, f"{os.getpid()}:{time.time()}".encode())
                    except Exception:
                        pass
                    return fd
                except FileExistsError:
                    if time.time() - start > timeout:
                        raise TimeoutError(f"Timeout acquiring lock {lockfile}")
                    time.sleep(0.05)

        def _release_lock(fd: int, lockfile: str):
            try:
                if isinstance(fd, int):
                    try:
                        os.close(fd)
                    except Exception:
                        pass
                if os.path.exists(lockfile):
                    try:
                        os.remove(lockfile)
                    except Exception:
                        pass
            except Exception:
                pass
        conn = get_connection(db_path)
        cur = conn.cursor()
        # Try to acquire per-upload lock to serialize operations for this staff/date
        lockfd = None
        lockfile = None
        try:
            lockfile = _lock_path(db_path, staff_id, report_date)
            lockfd = _acquire_lock(lockfile, timeout=15.0)
        except Exception:
            # If we can't acquire a lock, abort to avoid racing inventory updates
            conn.close()
            raise
        
        # CRITICAL: Set isolation level to SERIALIZABLE for maximum safety
        # This ensures no other transaction can interfere with our last-upload-wins logic
        cur.execute("PRAGMA read_uncommitted = 0")
        cur.execute("PRAGMA synchronous = FULL")
        
        # --- CRITICAL: ENSURE LAST-UPLOAD-WINS LOGIC IS ATOMIC ---
        # Take a snapshot of starting inventory for verification
        cur.execute("SELECT sim, swap, credit_50, credit_100 FROM inventory WHERE staff_id = ?", (staff_id,))
        starting_inv = cur.fetchone()
        logger.info(f"[TRANSACTION START] staff_id={staff_id} starting inventory: {dict(starting_inv) if starting_inv else None}")
        
        # First check if there are existing sales that need to be reverted
        try:
            cur.execute("SELECT id FROM sales WHERE staff_id = ? AND report_date = ?", (staff_id, report_date))
            existing = cur.fetchall()
            existing_ids = [r['id'] for r in existing]
            
            if existing_ids:
                logger.info(f"[insert_sales_and_update_inventory] Found {len(existing_ids)} existing sales to revert")

                # Robust revert: compute counts directly from existing sales rows instead
                # of relying on inventory_journal which may be missing if journaling failed
                # previously. This is deterministic and avoids double-adds or misses.
                cur.execute("SELECT id, item_code, number FROM sales WHERE staff_id = ? AND report_date = ?", (staff_id, report_date))
                existing_sales = cur.fetchall()
                # Build counts per inventory column
                revert_counts = {}
                for sr in existing_sales:
                    item_code_raw = sr['item_code'] or ''
                    item_code = str(item_code_raw).strip().lower()
                    # SIM/SWAP are counted per row (1 each)
                    if item_code in ('sim', 'simcard', 'sim_card'):
                        revert_counts['sim'] = revert_counts.get('sim', 0) + 1
                    elif item_code == 'swap':
                        revert_counts['swap'] = revert_counts.get('swap', 0) + 1
                    elif item_code in ('credit50', 'credit_50', 'credit-50'):
                        # number stores count for credit rows
                        try:
                            revert_counts['credit_50'] = revert_counts.get('credit_50', 0) + int(sr['number'] or 0)
                        except Exception:
                            pass
                    elif item_code in ('credit100', 'credit_100', 'credit-100'):
                        try:
                            revert_counts['credit_100'] = revert_counts.get('credit_100', 0) + int(sr['number'] or 0)
                        except Exception:
                            pass
                    else:
                        # For any other item types that map to a known column, try to parse numeric 'number'
                        col_guess = _map_item_to_column(item_code)
                        if col_guess:
                            try:
                                revert_counts[col_guess] = revert_counts.get(col_guess, 0) + int(sr['number'] or 0)
                            except Exception:
                                # ignore non-numeric
                                pass

                # CRITICAL: Revert inventory BEFORE deleting sales to maintain data integrity
                for col, c in revert_counts.items():
                    if c:
                        cur.execute(f"UPDATE inventory SET {col} = {col} + ? WHERE staff_id = ?", (int(c), staff_id))
                        # Record revert in journal referencing the sale ids for traceability
                        try:
                            cur.execute(
                                "INSERT INTO inventory_journal (staff_id, item, change_amount, change_type, source) VALUES (?, ?, ?, ?, ?)",
                                (staff_id, col, int(c), 'revert', f"delete_sales:{','.join(map(str, existing_ids))}")
                            )
                        except Exception:
                            # journaling is best-effort
                            pass
                        logger.info(f"[insert_sales_and_update_inventory] Reverted {c} {col} for staff_id={staff_id}")

                # Now that inventory is reverted, we can safely delete the old sales
                cur.execute("DELETE FROM sales WHERE staff_id = ? AND report_date = ?", (staff_id, report_date))
                logger.info(f"[insert_sales_and_update_inventory] Deleted {len(existing_ids)} old sales")

                # Take a snapshot of inventory after revert for verification
                cur.execute("SELECT sim, swap, credit_50, credit_100 FROM inventory WHERE staff_id = ?", (staff_id,))
                inv_after_revert = cur.fetchone()
                logger.info(f"[insert_sales_and_update_inventory] Inventory after revert: {dict(inv_after_revert)}")
                
        except Exception as ex:
            # If ANYTHING fails during the revert process, we must rollback and abort
            conn.rollback()
            logger.error(f"[CRITICAL] Failed to revert previous sales: {ex}")
            conn.close()
            raise Exception("Failed to safely revert previous sales")
        # Get current inventory for the staff
        try:
            cur.execute(
                "SELECT sim, swap, credit_50, credit_100 FROM inventory WHERE staff_id = ?",
                (staff_id,),
            )
            inv = cur.fetchone()
            logger.info(f"[insert_sales_and_update_inventory] staff_id={staff_id} inventory snapshot: {dict(inv) if inv else inv}")
            logger.info(f"[insert_sales_and_update_inventory] entries: {entries}")
            if not inv:
                logger.error(f"No inventory found for staff {staff_id}")
                conn.close()
                raise ValueError(f"No inventory found for staff {staff_id}")

            skipped = []  # to track skipped items/reasons
            duplicates_skipped = 0
            insufficient_skipped = 0
            inserted_count = 0

            # in-memory inventory map to avoid race/compounding errors within this transaction
            inv_map = {
                'sim': int(inv['sim'] or 0),
                'swap': int(inv['swap'] or 0),
                'credit_50': int(inv['credit_50'] or 0),
                'credit_100': int(inv['credit_100'] or 0),
            }
            logger.info(f"[insert_sales_and_update_inventory] initial inv_map: {inv_map}")

            for idx, e in enumerate(entries):
                try:
                    item_code = (e.get("item_code") or "").lower()
                    # robust Number parsing: accept Number/number/NUM, default to 1 if invalid/zero
                    raw_number = e.get("Number") or e.get("number") or e.get("NUM") or None
                    try:
                        if raw_number is None or str(raw_number).strip() == '':
                            number = 1
                        else:
                            number = int(float(str(raw_number).strip()))
                    except Exception:
                        number = 1
                    if number <= 0:
                        number = 1

                    try:
                        recharge_amount = float(e.get("recharge_amount") or 0)
                    except Exception:
                        recharge_amount = 0.0
                    notes = e.get("Notes") or e.get("notes") or None

                    # Interpret parsed values:
                    # - The 'number' column may be either a quantity (e.g. 2) or a GSM/mobile identifier (long number).
                    #   If it looks like a GSM (all digits and length >= 6) we treat it as an identifier and deduct 1 unit.
                    #   Otherwise we treat it as a quantity and deduct that many units.
                    # - credit_50 and credit_100 are explicit integer counts per row and should be deducted accordingly.
                    raw_number_str = str(raw_number).strip() if raw_number is not None else ''
                    is_gsm = raw_number_str.isdigit() and len(raw_number_str) >= 6
                    # default deductions
                    deduct = 0
                    store_number = raw_number if raw_number is not None else number
                    credit50_deduct = int(e.get('credit_50') or 0)
                    credit100_deduct = int(e.get('credit_100') or 0)

                    if item_code in ("sim", "simcard", "sim_card"):
                        # If the provided Number looks like a GSM identifier, store it and deduct 1.
                        # Otherwise treat Number as a quantity and deduct that many SIMs.
                        if is_gsm:
                            deduct = 1
                            store_number = raw_number_str
                        else:
                            deduct = int(number)
                            store_number = number
                        # Also try to detect GSM in other columns if present
                        if not is_gsm:
                            for k, v in e.items():
                                if not v:
                                    continue
                                if str(v).strip().isdigit() and len(str(v).strip()) > 5:
                                    store_number = str(v).strip()
                                    break
                    elif item_code in ("swap",):
                        if is_gsm:
                            deduct = 1
                            store_number = raw_number_str
                        else:
                            deduct = int(number)
                            store_number = number
                    elif item_code in ("credit50", "credit_50", "credit-50"):
                        # Deduct credit_50 counts from inventory
                        deduct = int(credit50_deduct or 0)
                    elif item_code in ("credit100", "credit_100", "credit-100"):
                        # Deduct credit_100 counts from inventory
                        deduct = int(credit100_deduct or 0)
                    else:
                        # For other items (like recharge): treat deduct as numeric if present (rare)
                        try:
                            deduct = int(number) if int(number) > 0 else 0
                        except Exception:
                            deduct = 0
                        
                    logger.info(f"[insert_sales_and_update_inventory] Row {idx}: item_code={item_code}, raw_number={raw_number}, store_number={store_number}, deduct={deduct}")

                    logger.info(f"[insert_sales_and_update_inventory] Row {idx}: item_code={item_code}, number={number}, recharge_amount={recharge_amount}, notes={notes}, entry={e}, deduct={deduct}")

                    # Duplicate check only for SIM items with GSM identifiers (allow SWAP duplicates)
                    if is_gsm and store_number is not None and str(store_number).strip() != '' and item_code in ("sim", "simcard", "sim_card"):
                        try:
                            cur.execute("SELECT 1 FROM sales WHERE number = ? AND item_code IN ('sim', 'simcard', 'sim_card')", (store_number,))
                            if cur.fetchone():
                                skipped.append(f"dup_number:{store_number}")
                                duplicates_skipped += 1
                                logger.info(f"[SKIP] Duplicate sale number: {store_number} (row {idx})")
                                continue
                        except Exception as de:
                            logger.warning(f"Duplicate check failed for row {idx}: {de}")

                    # Determine inventory column and available quantity
                    col = _map_item_to_column(item_code)
                    logger.info(f"[insert_sales_and_update_inventory] Row {idx}: mapped item_code '{item_code}' to column '{col}'")
                    if not col:
                        skipped.append(f"{item_code}(invalid)")
                        logger.warning(f"[SKIP] Invalid item_code: {item_code} (row {idx})")
                        continue
                    available = inv_map.get(col, 0)
                    logger.info(f"[insert_sales_and_update_inventory] Row {idx}: available={available}, deduct={deduct}")

                    if available < deduct:
                        skipped.append(f"{item_code}(insufficient:{available}<{deduct})")
                        insufficient_skipped += 1
                        logger.warning(f"[SKIP] Insufficient stock for {item_code}: {available}<{deduct} (row {idx})")
                        continue

                    # Insert sale row (store the parsed number)
                    contact_number_val = e.get('contact_number') or e.get('Contact Number') or e.get('contact') or None
                    cur.execute(
                        """
                        INSERT INTO sales (staff_id, report_date, item_code, number, contact_number, recharge_amount, notes)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (staff_id, report_date, item_code, store_number, contact_number_val, recharge_amount, notes),
                    )
                    sale_id = cur.lastrowid
                    logger.info(f"[insert_sales_and_update_inventory] Row {idx}: Inserted sale_id={sale_id}")

                    # Deduct from DB and update in-memory map
                    cur.execute(f"UPDATE inventory SET {col} = {col} - ? WHERE staff_id = ?", (deduct, staff_id))
                    inv_map[col] = max(0, inv_map.get(col, 0) - deduct)
                    logger.info(f"[insert_sales_and_update_inventory] Row {idx}: Updated inventory {col} to {inv_map[col]}")

                    # record journal entry with source_ref linking to sale id
                    cur.execute(
                        "INSERT INTO inventory_journal (staff_id, item, change_amount, change_type, source, source_ref) VALUES (?, ?, ?, ?, ?, ?)",
                        (staff_id, col, -deduct, 'sale', 'excel', sale_id),
                    )
                    logger.info(f"[insert_sales_and_update_inventory] Row {idx}: Journaled inventory change for sale_id={sale_id}")

                    # If the entry includes a GSM, mark sim_batches sold (best-effort)
                    try:
                        gsm = None
                        if e.get('gsm_number'):
                            gsm = str(e.get('gsm_number')).strip()
                        else:
                            for k, v in e.items():
                                if not v:
                                    continue
                                lk = str(k).lower()
                                if 'gsm' in lk or lk in ('msisdn',):
                                    gsm = str(v).strip()
                                    break
                        if gsm:
                            cur.execute("SELECT id FROM sim_batches WHERE gsm_number = ?", (gsm,))
                            sb = cur.fetchone()
                            if sb:
                                cur.execute("UPDATE sim_batches SET status = 'sold', current_location = ? WHERE gsm_number = ?", ('Sold', gsm))
                                try:
                                    cur.execute("INSERT INTO inventory_journal (staff_id, item, change_amount, change_type, source, source_ref) VALUES (?, ?, ?, ?, ?, ?)", (staff_id, 'SIM', -1, 'sim_sale', 'sim_sale', sale_id))
                                    logger.info(f"[insert_sales_and_update_inventory] Row {idx}: Journaled sim_sale for GSM {gsm}")
                                except Exception as je:
                                    logger.warning(f"Failed to journal sim_sale for GSM {gsm}: {je}")
                    except Exception as gsm_ex:
                        logger.exception(f"sim marking failed for entry (row {idx}): {e} - {gsm_ex}")

                    # If the row also contains credit_50 / credit_100 counts, deduct them as separate sales and journal entries
                    try:
                        # Process credit 50
                        if credit50_deduct and int(credit50_deduct) > 0:
                            c50 = int(credit50_deduct)
                            avail_c50 = inv_map.get('credit_50', 0)
                            if avail_c50 < c50:
                                skipped.append(f"credit_50(insufficient:{avail_c50}<{c50})")
                                insufficient_skipped += 1
                                logger.warning(f"[SKIP] Insufficient credit_50 for row {idx}: {avail_c50}<{c50}")
                            else:
                                # insert sales row for credit_50 for traceability
                                cur.execute(
                                    """
                                    INSERT INTO sales (staff_id, report_date, item_code, number, contact_number, recharge_amount, notes)
                                    VALUES (?, ?, ?, ?, ?, ?, ?)
                                    """,
                                    (staff_id, report_date, 'credit_50', c50, None, 0.0, f"from_row:{sale_id}"),
                                )
                                sale_id_c50 = cur.lastrowid
                                cur.execute("UPDATE inventory SET credit_50 = credit_50 - ? WHERE staff_id = ?", (c50, staff_id))
                                inv_map['credit_50'] = max(0, inv_map.get('credit_50', 0) - c50)
                                cur.execute("INSERT INTO inventory_journal (staff_id, item, change_amount, change_type, source, source_ref) VALUES (?, ?, ?, ?, ?, ?)", (staff_id, 'credit_50', -c50, 'sale', 'excel', sale_id_c50))
                                logger.info(f"[insert_sales_and_update_inventory] Row {idx}: Deducted credit_50={c50}, new={inv_map['credit_50']}")

                        # Process credit 100
                        if credit100_deduct and int(credit100_deduct) > 0:
                            c100 = int(credit100_deduct)
                            avail_c100 = inv_map.get('credit_100', 0)
                            if avail_c100 < c100:
                                skipped.append(f"credit_100(insufficient:{avail_c100}<{c100})")
                                insufficient_skipped += 1
                                logger.warning(f"[SKIP] Insufficient credit_100 for row {idx}: {avail_c100}<{c100}")
                            else:
                                cur.execute(
                                    """
                                    INSERT INTO sales (staff_id, report_date, item_code, number, contact_number, recharge_amount, notes)
                                    VALUES (?, ?, ?, ?, ?, ?, ?)
                                    """,
                                    (staff_id, report_date, 'credit_100', c100, None, 0.0, f"from_row:{sale_id}"),
                                )
                                sale_id_c100 = cur.lastrowid
                                cur.execute("UPDATE inventory SET credit_100 = credit_100 - ? WHERE staff_id = ?", (c100, staff_id))
                                inv_map['credit_100'] = max(0, inv_map.get('credit_100', 0) - c100)
                                cur.execute("INSERT INTO inventory_journal (staff_id, item, change_amount, change_type, source, source_ref) VALUES (?, ?, ?, ?, ?, ?)", (staff_id, 'credit_100', -c100, 'sale', 'excel', sale_id_c100))
                                logger.info(f"[insert_sales_and_update_inventory] Row {idx}: Deducted credit_100={c100}, new={inv_map['credit_100']}")
                    except Exception as credit_ex:
                        logger.exception(f"Failed to process credits for row {idx}: {credit_ex}")

                    inserted_count += 1
                    logger.info(f"[INSERTED] Sale: staff_id={staff_id}, item={item_code}, number={deduct}, recharge={recharge_amount}, notes={notes}, gsm={e.get('gsm_number')}")
                except Exception as row_ex:
                    logger.error(f"[ERROR] Failed to process row {idx}: {e}\n{traceback.format_exc()}")

            # Update timestamp
            cur.execute("UPDATE inventory SET updated_at = CURRENT_TIMESTAMP WHERE staff_id = ?", (staff_id,))
            conn.commit()
            logger.info(f"Committed {inserted_count} sales for staff {staff_id} (skipped: {skipped}, duplicates_skipped={duplicates_skipped}, insufficient_skipped={insufficient_skipped})")

        except Exception as ex:
            conn.rollback()
            logger.error(f"[ROLLBACK] insert_sales_and_update_inventory failed: {ex}\n{traceback.format_exc()} | entries={entries}")
            raise ex
        finally:
            # release lock then close connection
            try:
                _release_lock(lockfd, lockfile)
            except Exception:
                pass
            conn.close()

        if skipped:
            logger.warning(f"Skipped sales for staff {staff_id}: {skipped}")
        # return summary for caller
        return {"skipped": skipped, "duplicates_skipped": duplicates_skipped, "insufficient_skipped": insufficient_skipped, "inserted": inserted_count}

    return await asyncio.to_thread(_fn)



async def remove_stock(db_path: str, staff_username: str, item: str, qty: int) -> bool:
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("SELECT id FROM staff WHERE username = ?", (staff_username,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return False
        sid = row["id"]
        col = _map_item_to_column(item)
        if not col:
            conn.close()
            return False
        # check current
        cur.execute(f"SELECT {col} FROM inventory WHERE staff_id = ?", (sid,))
        r = cur.fetchone()
        available = int(r[0] or 0)
        if available < qty:
            conn.close()
            return False
        cur.execute(f"UPDATE inventory SET {col} = {col} - ? WHERE staff_id = ?", (qty, sid))
        cur.execute("UPDATE inventory SET updated_at = CURRENT_TIMESTAMP WHERE staff_id = ?", (sid,))
        conn.commit()
        conn.close()
        logger.info("Removed stock: %s -%s from %s", item, qty, staff_username)
        return True

    return await asyncio.to_thread(_fn)


async def add_stock(db_path: str, staff_username: str, item: str, qty: int) -> bool:
    """Add stock to a staff inventory (test helper / existing callers)."""
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("SELECT id FROM staff WHERE username = ?", (staff_username,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return False
        sid = row[0]
        col = _map_item_to_column(item)
        if not col:
            conn.close()
            return False
        cur.execute(f"UPDATE inventory SET {col} = {col} + ? WHERE staff_id = ?", (qty, sid))
        cur.execute("UPDATE inventory SET updated_at = CURRENT_TIMESTAMP WHERE staff_id = ?", (sid,))
        conn.commit()
        conn.close()
        return True

    return await asyncio.to_thread(_fn)


def _map_item_to_column(item: str) -> Optional[str]:
    if not item:
        return None
    code = item.strip().lower()
    if code in ("sim", "simcard", "sim_card"):
        return "sim"
    if code in ("swap",):
        return "swap"
    if code in ("credit50", "credit_50", "credit-50"):
        return "credit_50"
    if code in ("credit100", "credit_100", "credit-100"):
        return "credit_100"
    return None


async def view_stock_by_staff(db_path: str, staff_username: str) -> Optional[Dict[str, Any]]:
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("SELECT id, name FROM staff WHERE username = ?", (staff_username,))
        s = cur.fetchone()
        if not s:
            conn.close()
            return None
        sid = s["id"]
        cur.execute("SELECT sim, swap, credit_50, credit_100, updated_at FROM inventory WHERE staff_id = ?", (sid,))
        inv = cur.fetchone()
        conn.close()
        if not inv:
            return None
        return {"username": staff_username, "name": s["name"], "sim": inv["sim"], "swap": inv["swap"], "credit_50": inv["credit_50"], "credit_100": inv["credit_100"], "updated_at": inv["updated_at"]}

    return await asyncio.to_thread(_fn)


async def list_inventory(db_path: str) -> List[Dict[str, Any]]:
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("SELECT st.username, st.name, inv.sim, inv.swap, inv.credit_50, inv.credit_100 FROM inventory inv JOIN staff st ON inv.staff_id = st.id")
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    return await asyncio.to_thread(_fn)


async def delete_sale(db_path: str, sale_id: int) -> bool:
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("SELECT staff_id, item_code, number, recharge_amount FROM sales WHERE id = ?", (sale_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return False
        staff_id = row["staff_id"]
        code = (row["item_code"] or "").lower()
        num = int(row["number"] or 0)
        # delete sale
        cur.execute("DELETE FROM sales WHERE id = ?", (sale_id,))
        # revert inventory by adding back
        col = _map_item_to_column(code)
        if col and num:
            cur.execute(f"UPDATE inventory SET {col} = {col} + ? WHERE staff_id = ?", (num, staff_id))
        cur.execute("UPDATE inventory SET updated_at = CURRENT_TIMESTAMP WHERE staff_id = ?", (staff_id,))
        conn.commit()
        conn.close()
        logger.info("Deleted sale %s and reverted %s x %s to staff %s", sale_id, num, code, staff_id)
        return True

    return await asyncio.to_thread(_fn)


async def set_admin(db_path: str, staff_username: str, is_admin: bool = True) -> bool:
    """Set or unset the is_admin flag for a staff member."""
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("SELECT id FROM staff WHERE username = ?", (staff_username,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return False
        cur.execute("UPDATE staff SET is_admin = ? WHERE username = ?", (1 if is_admin else 0, staff_username))
        conn.commit()
        conn.close()
        logger.info("Set admin=%s for %s", is_admin, staff_username)
        return True

    return await asyncio.to_thread(_fn)


async def get_sales_by_staff_date(db_path: str, staff_username: str, date: Optional[str] = None) -> List[Dict[str, Any]]:
    def _fn():
        d = date or datetime.date.today().isoformat()
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("SELECT st.username, sa.id, sa.report_date, sa.item_code, sa.number, sa.recharge_amount, sa.notes FROM sales sa JOIN staff st ON sa.staff_id = st.id WHERE st.username = ? AND sa.report_date = ?", (staff_username, d))
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    return await asyncio.to_thread(_fn)


async def get_sale_by_id(db_path: str, sale_id: int) -> Optional[Dict[str, Any]]:
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("SELECT sa.id, sa.staff_id, sa.report_date, sa.item_code, sa.number, sa.recharge_amount, st.username FROM sales sa JOIN staff st ON sa.staff_id = st.id WHERE sa.id = ?", (sale_id,))
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None

    return await asyncio.to_thread(_fn)


async def get_all_sales_by_date_for_shop(db_path: str, date: Optional[str] = None, shop_id: int = None) -> List[Dict[str, Any]]:
    def _fn():
        d = date or datetime.date.today().isoformat()
        conn = get_connection(db_path)
        cur = conn.cursor()
        if shop_id is None:
            cur.execute("SELECT sa.id, st.username as username, st.name as employee, sa.report_date, sa.item_code, sa.number, sa.contact_number, sa.recharge_amount, sa.notes FROM sales sa JOIN staff st ON sa.staff_id = st.id WHERE sa.report_date = ?", (d,))
        else:
            cur.execute("SELECT sa.id, st.username as username, st.name as employee, sa.report_date, sa.item_code, sa.number, sa.contact_number, sa.recharge_amount, sa.notes FROM sales sa JOIN staff st ON sa.staff_id = st.id WHERE sa.report_date = ? AND st.shop_id = ?", (d, shop_id))
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    return await asyncio.to_thread(_fn)


async def get_all_sales_by_date(db_path: str, date: Optional[str] = None) -> List[Dict[str, Any]]:
    def _fn():
        d = date or datetime.date.today().isoformat()
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("SELECT sa.id, st.username as username, st.name as employee, sa.report_date, sa.item_code, sa.number, sa.contact_number, sa.recharge_amount, sa.notes FROM sales sa JOIN staff st ON sa.staff_id = st.id WHERE sa.report_date = ?", (d,))
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    return await asyncio.to_thread(_fn)


async def inventory_summary(db_path: str) -> Dict[str, int]:
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("SELECT SUM(sim) as sim, SUM(swap) as swap, SUM(credit_50) as credit_50, SUM(credit_100) as credit_100 FROM inventory")
        row = cur.fetchone()
        conn.close()
        return {"sim": int(row["sim"] or 0), "swap": int(row["swap"] or 0), "credit_50": int(row["credit_50"] or 0), "credit_100": int(row["credit_100"] or 0)}

    return await asyncio.to_thread(_fn)

async def transfer_stock(db_path: str, from_username: str, to_username: str, item: str, qty: int) -> bool:
    """Transfer stock from one user to another."""
    # Get inventories
    from_inv = await view_stock_by_staff(db_path, from_username)
    to_inv = await view_stock_by_staff(db_path, to_username)

    if not from_inv or not to_inv:
        return False  # one of the users not found

    # Check if enough stock in from_inv
    # Normalize item to column name
    col = _map_item_to_column(item)
    if not col:
        logger.warning("transfer_stock: unknown item '%s'", item)
        return False

    # Check availability
    if int(from_inv.get(col, 0)) < qty:
        return False

    # Apply transfer in-memory
    from_inv[col] = int(from_inv.get(col, 0)) - qty
    to_inv[col] = int(to_inv.get(col, 0)) + qty

    # Update DB (best-effort)
    try:
        await update_inventory(db_path, from_username, from_inv)
        await update_inventory(db_path, to_username, to_inv)
    except Exception as ex:
        logger.exception("Failed to update inventory during transfer: %s", ex)
        return False
    return True
async def update_inventory(db_path: str, username: str, new_inv: dict) -> bool:
    """Update the inventory row for a given username."""
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("SELECT id FROM staff WHERE username = ?", (username,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return False
        sid = row["id"]
        cur.execute(
            "UPDATE inventory SET sim = ?, swap = ?, credit_50 = ?, credit_100 = ?, updated_at = CURRENT_TIMESTAMP WHERE staff_id = ?",
            (int(new_inv.get('sim', 0)), int(new_inv.get('swap', 0)), int(new_inv.get('credit_50', 0)), int(new_inv.get('credit_100', 0)), sid),
        )
        conn.commit()
        conn.close()
        return True

    return await asyncio.to_thread(_fn)


async def get_sales_counts_by_staff_dates(db_path: str, start_date: str, end_date: str) -> list:
    """Return aggregated counts per staff per date between start_date and end_date (inclusive).
    Each row: staff_id, username, report_date, sim_count, swap_count, reg_count
    """
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        # build list of dates
        cur.execute(
            "SELECT sa.report_date as report_date, st.username as username, "
            "SUM(CASE WHEN lower(sa.item_code) IN ('sim','simcard','sim_card') THEN 1 ELSE 0 END) as sim_count, "
            "SUM(CASE WHEN lower(sa.item_code) = 'swap' THEN 1 ELSE 0 END) as swap_count "
            "FROM sales sa JOIN staff st ON sa.staff_id = st.id "
            "WHERE sa.report_date BETWEEN ? AND ? "
            "GROUP BY st.username, sa.report_date "
            , (start_date, end_date)
        )
        rows = cur.fetchall()
        # also fetch registrations
        cur.execute("SELECT dr.date as report_date, st.username as username, dr.reg_count FROM daily_regs dr JOIN staff st ON dr.staff_id = st.id WHERE dr.date BETWEEN ? AND ?", (start_date, end_date))
        reg_rows = cur.fetchall()
        conn.close()
        res = [dict(r) for r in rows]
        regs_map = {(r['username'], r['report_date']): int(r['reg_count'] or 0) for r in reg_rows}
        for r in res:
            r['reg_count'] = regs_map.get((r['username'], r['report_date']), 0)
        return res

    return await asyncio.to_thread(_fn)


async def get_staff_by_username(db_path: str, username: str) -> Optional[Dict[str, Any]]:
    """Return staff row as dict or None."""
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("SELECT id, username, name, is_admin, chat_id FROM staff WHERE username = ?", (username,))
        row = cur.fetchone()
        conn.close()
        if not row:
            return None
        return dict(row)

    return await asyncio.to_thread(_fn)


async def set_staff_chat_id(db_path: str, username: str, chat_id: str) -> bool:
    """Store or update staff.chat_id for notifications."""
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("SELECT id FROM staff WHERE username = ?", (username,))
        if not cur.fetchone():
            conn.close()
            return False
        cur.execute("UPDATE staff SET chat_id = ? WHERE username = ?", (str(chat_id), username))
        conn.commit()
        conn.close()
        return True

    return await asyncio.to_thread(_fn)


async def get_all_admin_chat_ids(db_path: str) -> List[str]:
    """Return list of chat_ids for all admin users (non-empty chat_id)."""
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("SELECT chat_id FROM staff WHERE is_admin = 1 AND chat_id IS NOT NULL AND chat_id != ''")
        rows = cur.fetchall()
        conn.close()
        return [r[0] for r in rows if r[0]]

    return await asyncio.to_thread(_fn)


async def delete_sales_for_staff_date(db_path: str, staff_id: int, report_date: str) -> int:
    """Delete all sales rows for staff_id on report_date. Returns number deleted."""
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        # precise revert: find sales ids for the staff/date and sum journal entries linked to those sale ids
        cur.execute("SELECT id, item_code FROM sales WHERE staff_id = ? AND report_date = ?", (staff_id, report_date))
        sale_rows = cur.fetchall()
        sale_ids = [r['id'] for r in sale_rows]
        if sale_ids:
            # sum journal entries grouped by item where source_ref in sale_ids
            q = f"SELECT item, SUM(change_amount) as s FROM inventory_journal WHERE source_ref IN ({','.join('?' for _ in sale_ids)}) GROUP BY item"
            cur.execute(q, sale_ids)
            rows = cur.fetchall()
            counts = {r['item']: int(-r['s']) for r in rows}  # journal change_amount are negative for sales
        else:
            counts = {}

        # delete sales
        cur.execute("DELETE FROM sales WHERE staff_id = ? AND report_date = ?", (staff_id, report_date))

        # revert inventory counts and write revert journal entries linked to the original sale ids
        for code, c in counts.items():
            col = _map_item_to_column(code or "")
            if col and c:
                cur.execute(f"UPDATE inventory SET {col} = {col} + ? WHERE staff_id = ?", (c, staff_id))
                # record a revert journal entry; source_ref left NULL but change_type='revert' and source lists sale ids
                cur.execute("INSERT INTO inventory_journal (staff_id, item, change_amount, change_type, source) VALUES (?, ?, ?, ?, ?)", (staff_id, col, int(c), 'revert', f"delete_sales:{','.join(map(str, sale_ids))}"))
        conn.commit()
        conn.close()
        return sum(counts.values())

    return await asyncio.to_thread(_fn)


async def get_all_staff_chat_ids(db_path: str) -> List[str]:
    """Return list of chat_ids for all staff who have chat_id set."""
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("SELECT chat_id FROM staff WHERE chat_id IS NOT NULL AND chat_id != ''")
        rows = cur.fetchall()
        conn.close()
        return [r[0] for r in rows if r[0]]

    return await asyncio.to_thread(_fn)


async def insert_daily_regs(db_path: str, staff_id: int, date: str, reg_count: int) -> bool:
    """Insert or update daily_regs for a staff/date (keep only one row per staff/date)."""
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        # Enforce last-upload-wins: remove any existing rows for this staff/date
        cur.execute("DELETE FROM daily_regs WHERE staff_id = ? AND date = ?", (staff_id, date))
        # Insert a fresh row so created_at reflects the latest upload
        cur.execute("INSERT INTO daily_regs (staff_id, date, reg_count) VALUES (?, ?, ?)", (staff_id, date, reg_count))
        conn.commit()
        conn.close()
        return True

    return await asyncio.to_thread(_fn)


async def get_regs_between(db_path: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """Return aggregated registrations per staff between two dates (inclusive)."""
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("SELECT dr.staff_id, st.username, st.name, SUM(dr.reg_count) as total_regs FROM daily_regs dr JOIN staff st ON dr.staff_id = st.id WHERE dr.date BETWEEN ? AND ? GROUP BY dr.staff_id", (start_date, end_date))
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    return await asyncio.to_thread(_fn)


async def borrow_add(db_path: str, admin_id: str, person_name: str, amount: float, date: str = None, note: str = None) -> bool:
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        d = date or datetime.date.today().isoformat()
        cur.execute("INSERT INTO borrow_list (admin_id, person_name, amount, date, note) VALUES (?, ?, ?, ?, ?)", (str(admin_id), person_name, float(amount), d, note))
        conn.commit()
        conn.close()
        return True

    return await asyncio.to_thread(_fn)


async def borrow_list_for_admin(db_path: str, admin_id: str) -> List[Dict[str, Any]]:
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("SELECT id, person_name, amount, date, note FROM borrow_list WHERE admin_id = ? ORDER BY date DESC", (str(admin_id),))
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    return await asyncio.to_thread(_fn)


async def borrow_summary(db_path: str, admin_id: str, start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        if start_date and end_date:
            cur.execute("SELECT person_name, SUM(amount) as total FROM borrow_list WHERE admin_id = ? AND date BETWEEN ? AND ? GROUP BY person_name", (str(admin_id), start_date, end_date))
        else:
            cur.execute("SELECT person_name, SUM(amount) as total FROM borrow_list WHERE admin_id = ? GROUP BY person_name", (str(admin_id),))
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    return await asyncio.to_thread(_fn)
async def insert_daily_total(db_path: str, date: str, shop_id: Optional[int], total_amount: float) -> bool:
    """Insert a daily total row for a given shop/date. shop_id may be None for grand totals.
    Uses "last upload wins" - deletes any existing rows for same date/shop_id first.
    """
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        try:
            # First delete any existing rows for same date/shop_id
            if shop_id is not None:
                cur.execute("DELETE FROM daily_totals WHERE date = ? AND shop_id = ?", (date, shop_id))
            else:
                cur.execute("DELETE FROM daily_totals WHERE date = ? AND shop_id IS NULL", (date,))

            # Insert new total row
            cur.execute(
                "INSERT INTO daily_totals (date, shop_id, total_amount) VALUES (?, ?, ?)",
                (date, shop_id, float(total_amount)),
            )
            conn.commit()
            conn.close()
            return True
        except Exception:
            conn.rollback()
            conn.close()
            raise
    return await asyncio.to_thread(_fn)


async def get_daily_totals(db_path: str, date: Optional[str] = None, shop_id: Optional[int] = None) -> List[Dict[str, Any]]:
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        if date and shop_id is not None:
            cur.execute("SELECT * FROM daily_totals WHERE date = ? AND shop_id = ?", (date, shop_id))
        elif date:
            cur.execute("SELECT * FROM daily_totals WHERE date = ?", (date,))
        elif shop_id is not None:
            cur.execute("SELECT * FROM daily_totals WHERE shop_id = ?", (shop_id,))
        else:
            cur.execute("SELECT * FROM daily_totals")
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]
    return await asyncio.to_thread(_fn)
async def is_admin_by_username(db_path: str, username: str) -> bool:
    """Check if a staff member has admin privileges."""
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("SELECT is_admin FROM staff WHERE username = ?", (username,))
        row = cur.fetchone()
        conn.close()
        return bool(row and row["is_admin"])
    
    return await asyncio.to_thread(_fn)
async def get_inventory(db_path: str, staff_id: int) -> dict:
    """Get inventory by staff_id."""
    def _fn():
        conn = get_connection(db_path)
        cur = conn.cursor()
        cur.execute("SELECT sim, swap, credit_50, credit_100, updated_at FROM inventory WHERE staff_id = ?", (staff_id,))
        row = cur.fetchone()
        conn.close()
        if not row:
            return {"sim": 0, "swap": 0, "credit_50": 0, "credit_100": 0, "updated_at": None}
        return dict(row)
    return await asyncio.to_thread(_fn)
import pandas as pd

async def daily_recharge_report(db_path: str, date: str = None) -> str:
    """Generate an Excel file for all sales on a given date."""
    from pathlib import Path

    date = date or datetime.date.today().isoformat()
    conn = get_connection(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT st.username, st.name, sa.item_code, sa.number, sa.recharge_amount, sa.notes
        FROM sales sa
        JOIN staff st ON sa.staff_id = st.id
        WHERE sa.report_date = ?
    """, (date,))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        raise ValueError(f"No sales found for {date}")

    df = pd.DataFrame([dict(r) for r in rows])
    output_dir = Path("reports")
    output_dir.mkdir(exist_ok=True)
    path = output_dir / f"recharge_report_{date}.xlsx"
    df.to_excel(path, index=False)
    return str(path)
