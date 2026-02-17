
import pandas as pd 
import io
import re
import requests
from bs4 import BeautifulSoup
import os
import psycopg2
from psycopg2 import connect
from psycopg2.extras import execute_values


def get_latest_link():
    url = "https://www.ons.gov.uk/economy/economicoutputandproductivity/output/datasets/ukretailfootfall"
    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    soup = BeautifulSoup(response.text, "html.parser")

    # Find all links to .xlsx files
    links = soup.find_all("a", href=True)
    excel_links = [a['href'] for a in links if a['href'].endswith('.xlsx')]

    # The most recent is usually the first one
    if excel_links:
        latest_link = "https://www.ons.gov.uk" + excel_links[0]
        print("Most recent dataset link:", latest_link)
        return latest_link
    else:
        print("No Excel links found.")
        return ''
    
# Get latest excel for the ONS retail footfall dataset, load it, and print the sheet names to confirm it worked
def get_excel(url):
    try:
        # Download into memory
        resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=60)
        resp.raise_for_status()  # raises HTTPError for 4xx/5xx

        # Optional: lightweight content-type check (won't always be accurate)
        ctype = resp.headers.get('Content-Type', '')
        if 'excel' not in ctype and 'spreadsheetml' not in ctype:
            print(f"Warning: unexpected content type: {ctype}")

        # Read the entire workbook: sheet_name=None returns a dict of DataFrames
        xls = pd.read_excel(io.BytesIO(resp.content), sheet_name=None, engine='openpyxl')

        # Output the titles (sheet names)
        print("Sheets found:", ",\n ".join(xls.keys()))

        # Example: preview each sheet
        # for name, df in xls.items():
        #     print(f"\n=== Sheet: {name} ===")
        #     print(df.head())
        
    except requests.exceptions.Timeout:
        print("Error: download timed out.")
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Network error: {e}")
    except ValueError as e:
        # pandas read_excel errors (e.g., corrupted file)
        print(f"Excel parse error: {e}")
    return xls

def get_version(xls):
    version = ""
    try:
        raw = xls['Cover'].iloc[3,0]  # A5 -> row index 4, column 0

        m = re.search(r"(\d{1,2}\s+[A-Za-z]+\s+\d{4})", raw)
        if m:       
            version = pd.to_datetime(m.group(1), dayfirst=True).strftime('%Y-%m-%d')
        else:
            # fallback: try to coerce any parsable date inside the string
            parsed = pd.to_datetime(raw, dayfirst=True, errors='coerce')
            version = parsed.strftime('%Y-%m-%d') if not pd.isna(parsed) else ""
    except Exception as e:
        print('Could not extract publication date from Cover sheet:', e)
        version = ""
    return version

 

import re
from typing import Dict
import pandas as pd

# These are the only site-type labels present in the ONS sheets
SITE_TYPES = [
    "District or Local Centre",
    "Retail Parks",
    "Town and City Centres",
]

# ---------- Helpers ----------

def _normalize_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Given a sheet already loaded to a DataFrame (with unknown intro rows),
    find the row that contains 'Date', use that row as the header,
    and return the clean table that starts immediately below it.
    """
    raw = df.copy()

    # Locate the first occurrence of 'Date' anywhere in the sheet
    header_row = None
    header_col = None
    for i in range(raw.shape[0]):
        for j in range(raw.shape[1]):
            val = raw.iat[i, j]
            if isinstance(val, str) and val.strip() == "Date":
                header_row, header_col = i, j
                break
        if header_row is not None:
            break
    if header_row is None:
        raise ValueError("Could not find a header row containing 'Date'")

    # Trim columns left of the 'Date' cell (gets rid of notes/blank cols)
    trimmed = raw.iloc[:, header_col:].copy()

    # Promote the 'Date' row to header and drop rows above it
    trimmed.columns = trimmed.iloc[header_row].astype(str).tolist()
    trimmed = trimmed.iloc[header_row + 1 :].reset_index(drop=True)

    # Drop unnamed/empty columns and fully empty rows
    trimmed = trimmed.loc[:, ~trimmed.columns.to_series().str.contains(r"^Unnamed", na=False)]
    trimmed = trimmed.dropna(how="all")

    # Ensure the 'Date' column exists (spelling/case is as in the workbook)
    if "Date" not in trimmed.columns:
        raise ValueError("Normalized table does not contain a 'Date' column after header detection")

    return trimmed

# 
_site_type_re = re.compile(
    r"(.*)\s+(District or Local Centre|Retail Parks|Town and City Centres)$"
)

def _melt_week_region(df: pd.DataFrame) -> pd.DataFrame:
    out = df.melt(id_vars=["Date"], var_name="region", value_name="footfall_index")
    out["site_type"] = "all"
    out["period_type"] = "week"
    return out

def _melt_week_site(df: pd.DataFrame) -> pd.DataFrame:
    out = df.melt(id_vars=["Date"], var_name="site_type", value_name="footfall_index")
    out["region"] = "UK total"
    out["period_type"] = "week"
    return out

def _melt_week_region_site(df: pd.DataFrame) -> pd.DataFrame:
    long_df = df.melt(id_vars=["Date"], var_name="combo", value_name="footfall_index")

    def split_combo(s):
        if pd.isna(s):
            return pd.Series({"region": None, "site_type": None})
        s = str(s)
        m = _site_type_re.match(s)
        if m:
            return pd.Series({"region": m.group(1).strip(), "site_type": m.group(2).strip()})
        # Safe fallback if formatting shifts slightly
        for st in SITE_TYPES:
            if s.endswith(st):
                return pd.Series({"region": s[: -len(st)].strip(), "site_type": st})
        return pd.Series({"region": s, "site_type": None})

    parts = long_df["combo"].apply(split_combo)
    long_df = pd.concat([long_df.drop(columns=["combo"]), parts], axis=1)
    long_df["period_type"] = "week"
    return long_df

def _melt_month_region(df: pd.DataFrame) -> pd.DataFrame:
    out = df.melt(id_vars=["Date"], var_name="region", value_name="footfall_index")
    out["site_type"] = "all"
    out["period_type"] = "month"
    return out

def _melt_month_site(df: pd.DataFrame) -> pd.DataFrame:
    out = df.melt(id_vars=["Date"], var_name="site_type", value_name="footfall_index")
    out["region"] = "UK total"
    out["period_type"] = "month"
    return out

def _melt_month_region_site(df: pd.DataFrame) -> pd.DataFrame:
    long_df = df.melt(id_vars=["Date"], var_name="combo", value_name="footfall_index")
    parts = long_df["combo"].apply(
        lambda s: pd.Series(_site_type_re.match(str(s)).groups(), index=["region", "site_type"])
        if _site_type_re.match(str(s))
        else pd.Series({"region": str(s), "site_type": None})
    )
    long_df = pd.concat([long_df.drop(columns=["combo"]), parts], axis=1)
    long_df["period_type"] = "month"
    return long_df

# ---------- Public function (what you’ll call) ----------

def parse_footfall_data(xls: Dict[str, pd.DataFrame], version: str = "") -> pd.DataFrame:
    """
    Parameters
    ----------
    xls : dict[str, DataFrame]
        Output of pd.read_excel(..., sheet_name=None). Keys are sheet names.
    version : str
        Version string (e.g., publication date) to stamp onto every row.

    Returns
    -------
    DataFrame with columns:
      period_start_dt, period_end_dt, period_type, region, site_type, footfall_index, version
    """

    # Helper to find the correct sheet by (partial) name, case-insensitive.
    def _pick(name_part: str) -> str:
        for k in xls.keys():
            if name_part.lower() in str(k).lower():
                return k
        raise KeyError(f"Sheet containing '{name_part}' not found in workbook")

    # 1) Normalize each sheet to get a clean table with a 'Date' header.
    wk_reg  = _normalize_table(xls[_pick("Weekly by region")])
    wk_site = _normalize_table(xls[_pick("Weekly by site type")])
    wk_r_s  = _normalize_table(xls[_pick("Weekly by region and site")])

    mo_reg  = _normalize_table(xls[_pick("Monthly by region")])
    mo_site = _normalize_table(xls[_pick("Monthly by site type")])
    mo_r_s  = _normalize_table(xls[_pick("Monthly by region and site")])

    # 2) Melt all six into long
    w_reg  = _melt_week_region(wk_reg)
    w_site = _melt_week_site(wk_site)
    w_r_s  = _melt_week_region_site(wk_r_s)

    m_reg  = _melt_month_region(mo_reg)
    m_site = _melt_month_site(mo_site)
    m_r_s  = _melt_month_region_site(mo_r_s)

    combined = pd.concat([w_reg, w_site, w_r_s, m_reg, m_site, m_r_s], ignore_index=True)

    # 3) Dates: weekly dates are END; monthly dates are START
    combined = combined.rename(columns={"Date": "date"})
    combined["date"] = pd.to_datetime(combined["date"], errors="coerce")

    combined["period_start_dt"] = combined["date"].where(
        combined["period_type"].eq("month"),
        combined["date"] - pd.to_timedelta(6, unit="D"),
    )
    combined["period_end_dt"] = combined["date"].where(
        combined["period_type"].eq("week"),
        combined["date"],
    )
    is_month = combined["period_type"].eq("month")
    combined.loc[is_month, "period_end_dt"] = combined.loc[is_month, "period_start_dt"] + pd.offsets.MonthEnd(1)

    # 4) Clean up text, types, and add version
    combined["region"] = combined["region"].astype(str).str.strip()
    combined["site_type"] = combined["site_type"].astype(str).str.strip()
    combined.loc[combined["region"].eq("nan"), "region"] = None
    combined.loc[combined["site_type"].eq("nan"), "site_type"] = None

    combined["footfall_index"] = pd.to_numeric(combined["footfall_index"], errors="coerce")
    combined["version"] = version

    out = combined[
        ["period_start_dt", "period_end_dt", "period_type", "region", "site_type", "footfall_index", "version"]
    ].copy()

    # Emit dates as date (not datetime)
    out["period_start_dt"] = pd.to_datetime(out["period_start_dt"]).dt.date
    out["period_end_dt"]   = pd.to_datetime(out["period_end_dt"]).dt.date

    return out


    
def upsert_dataframe(df: pd.DataFrame, table: str = "uk_retail_footfall"):
    """
    Bulk insert with ON CONFLICT to avoid duplicates.
    Conflict target matches our PK in the CREATE TABLE above.
    """
    cols = ["period_start_dt","period_end_dt","period_type","region","site_type","footfall_index","version"]
    records = [tuple(row[c] for c in cols) for _, row in df.iterrows()]
    SCHEMA = os.environ.get("SUPABASE_SCHEMA", "public")
    sql = f"""
    insert into {SCHEMA}.{table}
        ({", ".join(cols)})
    values %s
    on conflict (period_type, period_end_dt, region, site_type, version)
    do update set
        footfall_index = excluded.footfall_index,
        period_start_dt = excluded.period_start_dt,
        inserted_at = now();
    """

    with connect(DB_URL) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, records, page_size=1000)
        conn.commit()

# ----------------------------------------------------------------------
# 4) Orchestrate end-to-end

def main():
    print("Resolving latest ONS link…")
    url = get_latest_link()
    print("Downloading workbook:", url)
    xls = get_excel(url)
    version = get_version(xls) or ""
    print("Publication version:", version)

    print("Parsing…")
    df = parse_footfall_data(xls, version=version)
    print(f"Rows ready to load: {len(df):,}")

    print("Upserting into Supabase…")
    upsert_dataframe(df)
    print("Done.")

if __name__ == "__main__":
    DB_URL = os.environ['SUPABASE_DB_URL']
    if not DB_URL:
        raise RuntimeError("Missing SUPABASE_DB_URL env var / secret")
    main()
