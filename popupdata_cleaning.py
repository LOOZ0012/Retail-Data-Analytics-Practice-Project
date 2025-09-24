### Purpose of script:
### 1) Remove diacritics for readability further downstream (e.g. "Éclat" -> "Eclat")
### 2) Parse start_date / end_date to ISO-8601 strings (YYYY-MM-DD) in new columns, for SQL compatibility

import os
from datetime import datetime
import unicodedata
import re

import chardet
import pandas as pd

INPUT_CSV  = "luxury_cosmetics_popups.csv" # Downloaded from Kaggle (https://www.kaggle.com/datasets/pratyushpuri/luxury-beauty-cosmetics-popup-events-kpi-2025)
OUTPUT_CSV = "luxury_cosmetics_popups_cleaned.csv"  # To be imported into SQL

ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

## --- Defining functions for 1) diacritic normalization 2) date parsing ---

# Defining function for encoding of initial CSV file by reading first 100,000 bytes
def detect_encoding(path: str, sample_size: int = 100_000) -> str | None:
    with open(path, "rb") as f:
        raw = f.read(sample_size)
    result = chardet.detect(raw)
    enc = result.get("encoding")
    print(f"-- Detected encoding = {enc!r}")
    return enc

# Defining function for removal of diacritics
def remove_diacritics(text):
    if isinstance(text, str):
        # NFKD decomposes characters so accents can be dropped
        # normalize whitespace after stripping accents
        out = "".join(
            c for c in unicodedata.normalize("NFKD", text)
            if not unicodedata.combining(c)
        )
        return " ".join(out.split())
    return text

# Defining function for parsing dates into SQL-readable ISO standard
def parse_date_ddmmy(s):
    # parse 'DD/M/YY' or 'DD/MM/YYYY' -> datetime.date
    # returns None for blank/unparsable values
    if s is None:
        return None
    s = str(s).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None

    for fmt in ("%d/%m/%y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass

    # fallback: tolerant parser with dayfirst
    d = pd.to_datetime(s, dayfirst=True, errors="coerce")
    return None if pd.isna(d) else d.date()

## --- Processing ---
def main():
    if not os.path.exists(INPUT_CSV):
        raise FileNotFoundError(os.path.abspath(INPUT_CSV))

    enc_guess = detect_encoding(INPUT_CSV)
    tried = [enc_guess, "utf-8-sig", "utf-8", "latin1", "windows-1252"]
    df = None
    last_err = None

    for enc in [e for e in tried if e]:
        try:
            df = pd.read_csv(INPUT_CSV, encoding=enc)
            print(f"-- Read_csv ok with encoding = {enc!r}")
            break
        except Exception as e:
            last_err = e
            print(f"failed with encoding={enc!r}: {e}")

    if df is None:
        # Final fallback without explicit encoding
        try:
            df = pd.read_csv(INPUT_CSV)
            print(f"read_csv ok with pandas default encoding")
        except Exception:
            raise last_err

    # --- Diacritic normalization ---
    object_cols = df.select_dtypes(include=["object"]).columns.tolist()
    print(f"-- Normalizing diacritics in {len(object_cols)} text column(s)")
    for c in object_cols:
        df[c] = df[c].apply(remove_diacritics)

    # --- Date standardization ---
    required = ("start_date", "end_date")
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Missing required column(s): {missing}")

    start_parsed = df["start_date"].apply(parse_date_ddmmy)
    end_parsed   = df["end_date"].apply(parse_date_ddmmy)

    df["start_date_iso"] = [d.isoformat() if d else "" for d in start_parsed]
    df["end_date_iso"]   = [d.isoformat() if d else "" for d in end_parsed]

    # --- Quick diagnostics ---
    unparsable_start = sum(1 for d, raw in zip(start_parsed, df["start_date"]) if d is None and str(raw).strip() != "")
    unparsable_end   = sum(1 for d, raw in zip(end_parsed,   df["end_date"])   if d is None and str(raw).strip() != "")

    bad_start_iso = df.loc[(df["start_date_iso"] != "") & (~df["start_date_iso"].str.match(ISO_RE)), ["event_id","start_date","start_date_iso"]]
    bad_end_iso   = df.loc[(df["end_date_iso"]   != "") & (~df["end_date_iso"].str.match(ISO_RE)),   ["event_id","end_date","end_date_iso"]]

    print(f"-- Rows={len(df)}, unparsable_start={unparsable_start}, unparsable_end={unparsable_end}")
    if not bad_start_iso.empty or not bad_end_iso.empty:
        print("ISO check failed; sample offending rows:")
        if not bad_start_iso.empty:
            print(bad_start_iso.head(5))
        if not bad_end_iso.empty:
            print(bad_end_iso.head(5))
        raise ValueError("ISO formatting check failed.")

    # --- Export CSV ---
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"-- Wrote {os.path.abspath(OUTPUT_CSV)}")


if __name__ == "__main__":
    main()
