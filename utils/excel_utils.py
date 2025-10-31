"""Utilities to parse uploaded Excel files into normalized entries."""
from __future__ import annotations

from typing import List, Dict, Any, Tuple
import re
import pandas as pd
import io
import logging

logger = logging.getLogger(__name__)


def extract_daily_regs(file_bytes: bytes, notes_aliases: List[str] = None) -> int:
    """Extract daily registration integer from the first Notes cell of the uploaded Excel.

    Returns 0 if not present or not an integer.
    """
    try:
        df = pd.read_excel(io.BytesIO(file_bytes))
    except Exception:
        return 0

    cols = {c.strip().lower(): c for c in df.columns}
    notes_candidates = notes_aliases or ["notes", "remark", "remarks"]
    notes_col = None
    for n in notes_candidates:
        if n.lower() in cols:
            notes_col = cols[n.lower()]
            break
    if not notes_col:
        return 0
    try:
        first_note = df[notes_col].iloc[0]
        if pd.isna(first_note):
            return 0
        first_note = str(first_note).strip()
        # Extract first integer anywhere in the string (handles 'REG : 10', 'Daily 15', etc.)
        m = re.search(r"(\d+)", first_note)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return 0
    except Exception:
        return 0
    return 0


def parse_pickup_excel(file_bytes: bytes) -> List[Dict[str, str]]:
    """Parse pickup-list Excel and return list of rows with keys: carton_no, box_no, gsm_number, iccid, type."""
    rows: List[Dict[str, str]] = []
    try:
        df = pd.read_excel(io.BytesIO(file_bytes))
    except Exception:
        return rows

    cols = {c.strip().lower(): c for c in df.columns}
    def _c(names):
        for n in names:
            if n.lower() in cols:
                return cols[n.lower()]
        return None

    carton_col = _c(["carton #", "carton", "carton_no", "carton no"])
    box_col = _c(["box #", "box", "box_no", "box no"])
    gsm_col = _c(["gsm number", "gsm_number", "gsm", "number"])
    iccid_col = _c(["iccid", "iccid number", "iccid_no"])
    type_col = _c(["type", "sim type", "sim_type"])

    def _cell_to_str(val):
        """Normalize a pandas cell to a clean string or None.

        - Returns None for NaN/None.
        - If value is int or a float that is integer-valued, returns the integer string without .0.
        - Otherwise returns stripped string of the value.
        """
        try:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return None
        except Exception:
            pass
        try:
            if isinstance(val, int):
                return str(val)
            if isinstance(val, float):
                if val.is_integer():
                    return str(int(val))
                return str(val).strip()
            s = str(val).strip()
            if s.endswith('.0') and s[:-2].isdigit():
                return s[:-2]
            return s if s != '' else None
        except Exception:
            try:
                s = str(val).strip()
                return s if s != '' else None
            except Exception:
                return None

    for _, r in df.iterrows():
        gsm = None
        try:
            gsm = _cell_to_str(r[gsm_col]) if gsm_col else None
        except Exception:
            gsm = None
        if not gsm:
            continue
        row = {
            "carton_no": str(r[carton_col]).strip() if carton_col and not pd.isna(r[carton_col]) else None,
            "box_no": str(r[box_col]).strip() if box_col and not pd.isna(r[box_col]) else None,
            "gsm_number": gsm,
            "iccid": str(r[iccid_col]).strip() if iccid_col and not pd.isna(r[iccid_col]) else None,
            "type": str(r[type_col]).strip() if type_col and not pd.isna(r[type_col]) else None,
        }
        rows.append(row)
    return rows


def parse_sales_excel(file_bytes: bytes, report_date: str, employee_name: str) -> Tuple[List[Dict[str, Any]], List[str], int]:
    """Parse uploaded Excel bytes into a list of entries.

    Returns (entries, errors, daily_regs). If errors is non-empty, parsing failed or columns missing.
    For GSM numbers: must be exactly 9 digits, otherwise row is skipped.
    """
    # extract daily registrations (first Notes cell) if present
    daily_regs = extract_daily_regs(file_bytes)
    # Track invalid rows for feedback
    skipped_rows = []
    
    try:
        df = pd.read_excel(io.BytesIO(file_bytes))
        logger.info(f"[parse_sales_excel] DataFrame columns: {list(df.columns)}")
        logger.info(f"[parse_sales_excel] DataFrame head: {df.head().to_dict()}")
    except Exception as e:
        logger.error("Failed to read excel: %s", e)
        return [], [f"Failed to read Excel file: {e}"], daily_regs

    # Local normalizer used by the sales parser (same behavior as pickup normalizer)
    def _cell_to_str(val):
        try:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return None
        except Exception:
            pass
        try:
            if isinstance(val, int):
                return str(val)
            if isinstance(val, float):
                if val.is_integer():
                    return str(int(val))
                return str(val).strip()
            s = str(val).strip()
            if s.endswith('.0') and s[:-2].isdigit():
                return s[:-2]
            return s if s != '' else None
        except Exception:
            try:
                s = str(val).strip()
                return s if s != '' else None
            except Exception:
                return None

    # normalize column names
    cols = {c.strip().lower(): c for c in df.columns}

    def _find_column(possible_names):
        for n in possible_names:
            if n.lower() in cols:
                return cols[n.lower()]
        return None

    # The Excel 'Number' column is used for GSM numbers (MSISDN) in our workflow.
    # We do not treat it as a quantity field. Quantity is implied by rows/item_code.
    number_col = _find_column(["Number", "number", "qty", "quantity"])
    recharge_col = _find_column(["Recharge", "recharge", "amount", "recharge_amount"])
    item_col = _find_column(["item_code", "item code", "item", "code"])
    gsm_col = _find_column(["gsm number", "gsm_number", "gsm", "msisdn", "phone"])
    credit50_col = _find_column(["credit50", "credit_50", "credit-50", "Credit_50", "Credit50"])
    credit100_col = _find_column(["credit100", "credit_100", "credit-100", "Credit_100", "Credit100"])
    notes_col = _find_column(["Notes", "notes", "remark", "remarks"])
    # optional contact number column aliases
    contact_col = _find_column(["contact_number", "contact number", "contact", "phone number", "phone"])

    errors: List[str] = []
    logger.info(f"[parse_sales_excel] Detected columns: number_col={number_col}, recharge_col={recharge_col}, item_col={item_col}, gsm_col={gsm_col}, notes_col={notes_col}")
    # Require number_col because it contains the GSM for each row in our system.
    if number_col is None:
        errors.append("Missing Number column (used for GSM mobile numbers).")
    # Recharge column is optional (not every row is a recharge)
    if errors:
        logger.error(f"[parse_sales_excel] Errors: {errors}")
        return [], errors, daily_regs

    entries: List[Dict[str, Any]] = []
    for idx, row in df.iterrows():
        row_num = idx + 2  # Excel row number (1-based header)

        # Normalize potential GSM values from GSM column or Number column
        try:
            raw_gsm_from_gsmcol = _cell_to_str(row[gsm_col]) if gsm_col else None
        except Exception:
            raw_gsm_from_gsmcol = None
        try:
            raw_gsm_from_numbercol = _cell_to_str(row[number_col]) if number_col else None
        except Exception:
            raw_gsm_from_numbercol = None

        def _clean_gsm(s: Any) -> str | None:
            """Return a digits-only GSM string or None.

            - Converts numeric floats/ints to strings, strips spaces.
            - Removes any non-digit characters and trailing .0 artifacts.
            - Returns the digits-only string, or None if empty.
            """
            if s is None:
                return None
            try:
                st = str(s).strip()
            except Exception:
                return None
            # strip trailing .0 if present and looks like float artifact
            if st.endswith('.0'):
                st = st[:-2]
            # remove spaces and non-digit characters
            digits = re.sub(r"\D", "", st)
            return digits if digits != "" else None

        gsm_candidate = _clean_gsm(raw_gsm_from_gsmcol) or _clean_gsm(raw_gsm_from_numbercol)

        # capture item code text early
        item_code_str = str(row[item_col]).strip() if item_col and not pd.isna(row[item_col]) else ""

        # Detect SIM/SWAP rows
        is_sim_like = item_code_str.lower() in ("sim", "simcard", "sim_card")
        is_swap_like = item_code_str.lower() in ("swap",)

        # For SIM/SWAP rows: require a valid GSM (digits-only, exactly 9 digits)
        gsm_number = None
        if is_sim_like or is_swap_like:
            if not gsm_candidate:
                skipped_rows.append(f"Row {row_num} skipped: Missing or invalid GSM number")
                continue
            if not (gsm_candidate.isdigit() and len(gsm_candidate) == 9):
                skipped_rows.append(f"Row {row_num} skipped: GSM value '{gsm_candidate}' must be exactly 9 digits")
                continue
            # valid GSM
            gsm_number = gsm_candidate
            # store GSM in 'number' as historical contract (DB expects it)
            number = gsm_candidate
        else:
            # Non SIM/SWAP rows: parse Number as numeric quantity if present
            number = 0
            try:
                num_raw = raw_gsm_from_numbercol
                if num_raw and str(num_raw).strip().isdigit():
                    number = int(float(str(num_raw).strip()))
                else:
                    number = 0
            except Exception:
                number = 0

        # parse recharge (clean and parse float)
        try:
            rec_str = _cell_to_str(row[recharge_col]) if recharge_col else None
            if rec_str is not None:
                # remove spaces and trailing .0 handled by _cell_to_str
                try:
                    recharge_amount = float(str(rec_str).strip())
                except Exception:
                    recharge_amount = 0.0
            else:
                recharge_amount = 0.0
        except Exception:
            try:
                recharge_amount = float(row[recharge_col]) if recharge_col and not pd.isna(row[recharge_col]) else 0.0
            except Exception:
                recharge_amount = 0.0

        # parse credit columns if present (only count if row remains valid)
        try:
            c50s = _cell_to_str(row[credit50_col]) if credit50_col else None
            credit_50 = int(c50s) if (c50s and str(c50s).strip().isdigit()) else 0
        except Exception:
            credit_50 = 0
        try:
            c100s = _cell_to_str(row[credit100_col]) if credit100_col else None
            credit_100 = int(c100s) if (c100s and str(c100s).strip().isdigit()) else 0
        except Exception:
            credit_100 = 0

        item_code = item_code_str
        notes = str(row[notes_col]).strip() if notes_col and not pd.isna(row[notes_col]) else ""
        # Validation: For SIM/SWAP, we've already required GSM. For other items, skip rows with
        # no meaningful data (no item_code, no recharge, no credits, and no numeric number).
        if not (is_sim_like or is_swap_like):
            if not ((item_col and item_code_str) or recharge_amount > 0 or credit_50 > 0 or credit_100 > 0 or (isinstance(number, int) and number > 0)):
                logger.info(f"[parse_sales_excel] Skipping row {idx}: empty/irrelevant row (no item/recharge/credits)")
                continue

        # NEW: parse contact number if present, keep None if missing/empty
        try:
            contact_number = None
            if contact_col and not pd.isna(row[contact_col]):
                cn = str(row[contact_col]).strip()
                contact_number = cn if cn != "" else None
        except Exception:
            contact_number = None

        entry = {
            "Employee": employee_name,
            "Date": report_date,
            "item_code": item_code,
            # Keep original 'number' value as the GSM string (DB will treat it as identifier and deduct 1 for SIMs)
            "number": number,
            "recharge_amount": recharge_amount,
            "credit_50": credit_50,
            "credit_100": credit_100,
            "notes": notes,
            "gsm_number": gsm_number if gsm_number else "",  # Use validated GSM number or empty
            "contact_number": contact_number,
        }
        logger.info(f"[parse_sales_excel] Parsed entry row {idx}: {entry}")
        entries.append(entry)
    logger.info(f"[parse_sales_excel] Total parsed entries: {len(entries)}, Skipped rows: {len(skipped_rows)}")
    
    # If we have skipped rows, add them to errors list
    errors = []
    if skipped_rows:
        errors.extend(skipped_rows)
        
    return entries, errors, daily_regs
