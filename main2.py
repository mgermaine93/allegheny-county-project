"""
ETL pipeline for Allegheny County Property Sale Transactions (WPRDC).
Written by Matthew Germaine (with assistance from Claude Code)

Question:

which municipalities saw the fastest growth in median sale price between 2012-13 and 2021-22?

Run with `python main2.py`. Safe to run any number of times.
"""

# standard library imports
import json
import re
from dataclasses import dataclass
from pathlib import Path

# third-party libraries
import duckdb
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# --- Config ---

PROJECT_DIR = Path(__file__).parent

# raw files land here, next to this script, regardless of where it's run from
RAW_DIR = PROJECT_DIR / "data" / "raw"


@dataclass
class APIConfig:
    """
    Settings for the WPRDC (CKAN) API.  No API key needed from what I could find.
    """
    base_url: str = "https://data.wprdc.org/api/3/action"
    # "Property Sales Transactions" resource from the real-estate-sales dataset
    resource_id: str = "5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1"
    timeout_seconds: int = 60 # seconds
    max_retries: int = 5

    def __post_init__(self):
        """
        Validate configuration after initialization.
        """
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive.")


@dataclass
class PipelineConfig:
    """
    Settings for the load, transform, and analysis steps.
    """
    db_path: Path = PROJECT_DIR / "data" / "sales.duckdb"

    # I got this from Mary Beth Snyder, who was listed as the Data Steward for the "Property Sales Transactions" dataset.
    codes_path: Path = PROJECT_DIR / "5_sales-validation-codes-dictionary.csv"
    # codes seen in the data that aren't in the 2015 dictionary, reviewed by hand.
    # anything not in the dictionary and not listed here fails the pipeline.
    reviewed_extra_codes: tuple = (
        ("PV", "Invalid Sale", "CLEAN AND GREEN VALID PREFERENTIAL"), # preferential assessment, same treatment as PA
    )
    # the question: where did arm's-length prices grow fastest between these two windows?
    # windows are pooled across two years to smooth out thin municipalities.
    # chosen when the 2026-10-26 snapshot had only ~1/8th the usual volume for 2023+ (backfilled on 2026-11-02).
    base_years: tuple = (2012, 2013)
    end_years: tuple = (2021, 2022)
    min_sales_per_window: int = 30 # municipalities with fewer valid sales in either window are left out
    min_valid_price: float = 1000.0 # "valid" sales under $1,000 are almost certainly nominal transfers
    max_row_drop_pct: float = 5.0 # fail if a new snapshot is this much smaller than the last one
    max_suspect_record_date_pct: float = 1.0 # fail if more rows than this have a missing/impossible record date

    def __post_init__(self):
        """
        Validate configuration after initialization.
        """
        if min(self.end_years) <= max(self.base_years):
            raise ValueError("end_years must come after base_years.")
        if self.min_sales_per_window <= 0:
            raise ValueError("min_sales_per_window must be positive.")


# initialize the configs
API_CONFIG = APIConfig()
PIPELINE_CONFIG = PipelineConfig()


class DataQualityError(Exception):
    """
    Raised when a data quality check fails. Stops the run before anything is published.
    """


# --- Extract ---

def make_session(config: APIConfig) -> requests.Session:
    """
    Builds an HTTP session that automatically retries failed requests.

    The WPRDC server intermittently returns 502/503/504 errors (and 429 when rate limiting), so every
    GET made through this session is retried up to config.max_retries times with exponential backoff
    (2s, 4s, 8s, ...) before giving up. Only GETs are retried, since the pipeline never sends anything else.

    Args:
        config: API settings; only max_retries is used here.

    Returns:
        A requests.Session with the retry policy mounted on all https:// URLs.
    """
    retry = Retry(
        total=config.max_retries,
        backoff_factor=2, # waits 2s, 4s, 8s, ... between attempts
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def extract_raw(config: APIConfig, raw_dir: Path) -> Path:
    """
    Extract step: downloads the current version of the sales data and saves it unchanged.

    1. Calls the CKAN resource_show endpoint to get the resource's metadata, including its download
       URL and last_modified timestamp.
    2. Names the target file after that date (sales_YYYY-MM-DD.csv). If the file already exists, this
       version has been downloaded before, so it returns immediately without downloading again.
    3. Otherwise streams the CSV dump to a .part file in 1 MB chunks (so the ~86 MB file never sits in
       memory), and renames it only once the download is complete. An interrupted download therefore
       never leaves a partial file under the real name.
    4. Saves the resource metadata next to it as sales_YYYY-MM-DD.meta.json, for traceability.

    The file is written byte-for-byte as served: no parsing or cleaning happens here.

    Args:
        config: API settings (base URL, resource ID, timeout, retries).
        raw_dir: Directory the raw files are saved in; created if it doesn't exist.

    Returns:
        Path to the raw CSV for the current version of the dataset.

    Raises:
        requests.HTTPError: if the metadata lookup or download still fails after retries.
        RuntimeError: if the CKAN API responds but reports success = false.
    """
    session = make_session(config)
    response = session.get(f"{config.base_url}/resource_show", params={"id": config.resource_id}, timeout=config.timeout_seconds)
    response.raise_for_status()
    body = response.json()
    if not body.get("success"):
        raise RuntimeError(f"API error: {body.get('error')}")
    meta = body["result"]

    raw_dir.mkdir(parents=True, exist_ok=True)
    target = raw_dir / f"sales_{meta['last_modified'][:10]}.csv"
    if target.exists():
        print(f"Already have {target.name}, skipping download")
        return target

    tmp = target.with_suffix(".part")
    with session.get(meta["url"], stream=True, timeout=config.timeout_seconds) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20): # 1 MB at a time, so the file never sits in memory
                f.write(chunk)
    tmp.rename(target) # only a complete download gets the real name
    # keep the resource metadata alongside the file for traceability
    (raw_dir / f"{target.stem}.meta.json").write_text(json.dumps(meta, indent=2))
    print(f"Saved {target.name}")
    return target


# --- Load ---

def load_raw_snapshot(con: duckdb.DuckDBPyConnection, raw_path: Path) -> str:
    """
    Load step: appends one raw CSV to the raw.sales table, exactly as published.

    Every column is loaded as text (all_varchar), so values like ZIP codes, deed pages, and malformed
    dates are stored exactly as they appear in the file. Type conversion happens later in
    build_clean_sales. Each row is tagged with the snapshot date taken from the file name, plus a
    loaded_at timestamp. raw.sales keeps every snapshot ever loaded and is never modified.

    The steps:
    1. Creates the raw schema and an empty raw.sales table (matching the CSV's columns) if they don't exist.
    2. If rows for this snapshot date are already in raw.sales, skips the load. This is what keeps
       re-runs from duplicating data.
    3. Before inserting, confirms the parse is trustworthy. The CKAN dump has mixed line endings, so
       DuckDB's strict CSV mode has to be off. To make up for it, the number of parsed rows must equal
       the number of lines in the file minus the header, and no column name can have stray whitespace
       (e.g. a leftover \\r).
    4. Inserts the rows. The insert is a single statement, so a snapshot is either fully loaded or not at all.

    Args:
        con: Open DuckDB connection.
        raw_path: Path to a raw CSV saved by extract_raw (named sales_YYYY-MM-DD.csv).

    Returns:
        The snapshot date string (YYYY-MM-DD) identifying these rows in raw.sales.

    Raises:
        DataQualityError: if the parsed row count doesn't match the file's line count, or column names
            have stray whitespace. Nothing is inserted in either case.
    """
    snapshot_date = raw_path.stem.removeprefix("sales_") # files are named sales_<last_modified date>.csv
    # the CKAN dump ends the header line with \r\n but every data row with \n, which DuckDB's strict mode rejects.
    # strict mode is off, so the row count check below is what guarantees nothing was mangled.
    read_raw = "read_csv(?, delim = ',', quote = '\"', escape = '\"', header = true, all_varchar = true, strict_mode = false)"

    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS raw.sales AS
        SELECT *, ''::VARCHAR AS snapshot_date, now() AS loaded_at FROM {read_raw} LIMIT 0
    """, [str(raw_path)])

    already_loaded = con.execute("SELECT count(*) FROM raw.sales WHERE snapshot_date = ?", [snapshot_date]).fetchone()[0]
    if already_loaded:
        print(f"Snapshot {snapshot_date} already in raw.sales ({already_loaded:,} rows), skipping load")
        return snapshot_date

    # no field in this dataset contains a newline, so every line after the header should be exactly one row
    with open(raw_path, "rb") as f:
        expected_rows = sum(1 for _ in f) - 1
    parsed_rows = con.execute(f"SELECT count(*) FROM {read_raw}", [str(raw_path)]).fetchone()[0]
    if parsed_rows != expected_rows:
        raise DataQualityError(f"{raw_path.name} has {expected_rows:,} lines but parsed into {parsed_rows:,} rows")
    columns = [row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {read_raw}", [str(raw_path)]).fetchall()]
    if any(c != c.strip() for c in columns):
        raise DataQualityError(f"Column names have stray whitespace: {columns}")

    con.execute(f"""
        INSERT INTO raw.sales
        SELECT *, ? AS snapshot_date, now() AS loaded_at FROM {read_raw}
    """, [snapshot_date, str(raw_path)])
    loaded = con.execute("SELECT count(*) FROM raw.sales WHERE snapshot_date = ?", [snapshot_date]).fetchone()[0]
    print(f"Loaded snapshot {snapshot_date} into raw.sales ({loaded:,} rows)")
    return snapshot_date


def read_sale_codes(config: PipelineConfig) -> pd.DataFrame:
    """
    Parses the county's sale validation codes dictionary into a lookup table with one row per code.

    The dictionary says whether each sale code means a valid (arm's-length) sale, an invalid one, or
    one that hasn't been reviewed yet. The analysis depends on it to separate market sales from family
    transfers, sheriff sales, and so on. The file needs some handling:
    - It's Windows-1252 encoded, not UTF-8.
    - It starts with paragraphs of explanation, so the header row ("Validity Group,Code,...") is found
      by searching for it rather than assuming a line number.
    - Discontinued codes are lumped into one "all other codes" row whose description ends with a list
      like "(UX, VB, VH, ...)". That list is extracted and each code becomes its own "Invalid Sale" row,
      as the dictionary instructs.
    - Codes from config.reviewed_extra_codes (seen in the data but missing from the dictionary, e.g. PV)
      are added as well.

    Args:
        config: Pipeline settings; uses codes_path and reviewed_extra_codes.

    Returns:
        DataFrame with columns validity_group, sale_code, and code_desc, one row per code.

    Raises:
        DataQualityError: if the "all other codes" row can't be found, or any code appears more than
            once (e.g. a reviewed code that the dictionary has since added).
    """
    lines = config.codes_path.read_text(encoding="cp1252").splitlines()
    header_row = next(i for i, line in enumerate(lines) if line.startswith("Validity Group,"))
    codes = pd.read_csv(config.codes_path, skiprows=header_row, encoding="cp1252", dtype=str).dropna(subset=["Code"])
    codes = codes.rename(columns={"Validity Group": "validity_group", "Code": "sale_code", "Code Description": "code_desc"})

    discontinued = codes[codes["sale_code"] == "all other codes"]
    if len(discontinued) != 1:
        raise DataQualityError("Couldn't find the discontinued-codes row in the sale codes dictionary.")
    # the description ends with the list of codes, e.g. "(UX, VB, VH, ...)"
    description = discontinued["Additional Description"].iloc[0]
    codes = codes[["validity_group", "sale_code", "code_desc"]]
    discontinued_codes = [c.strip() for c in re.search(r"\(([^)]*)\)", description).group(1).split(",")]
    discontinued_rows = pd.DataFrame({
        "validity_group": "Invalid Sale", # per the dictionary: "If any occur they should be considered invalid"
        "sale_code": discontinued_codes,
        "code_desc": "DISCONTINUED CODE",
    })
    reviewed_rows = pd.DataFrame(config.reviewed_extra_codes, columns=["sale_code", "validity_group", "code_desc"])

    codes = pd.concat([codes[codes["sale_code"] != "all other codes"], discontinued_rows, reviewed_rows], ignore_index=True)
    codes["sale_code"] = codes["sale_code"].str.strip()
    if codes["sale_code"].duplicated().any():
        raise DataQualityError(f"Sale code listed twice: {codes.loc[codes['sale_code'].duplicated(), 'sale_code'].tolist()}")
    return codes


# --- Transform ---

def build_clean_sales(con: duckdb.DuckDBPyConnection, snapshot_date: str, codes: pd.DataFrame) -> None:
    """
    Transform step: rebuilds the clean layer from a single raw snapshot.

    Creates two tables, replacing any previous versions:
    - clean.sale_codes: the parsed codes dictionary from read_sale_codes.
    - clean.sales: one row per sale in the snapshot, with:
        - snake_case column names
        - dates and prices converted to real types (TRY_CAST turns unparseable values into NULL, which
          run_checks then catches, instead of failing here)
        - surrounding whitespace trimmed and blank strings turned into NULL
        - validity_group joined in from the codes dictionary (NULL if the code is unknown)
        - record_date_suspect: true when the record date is blank, earlier than the sale date, or
          before 2000

    No rows are dropped or deduplicated, so clean.sales always has exactly as many rows as the
    snapshot. run_checks verifies this.

    It's a full rebuild rather than an incremental upsert because the county corrects and deletes past
    records between refreshes, and the source _id changes every time. Rebuilding from the latest
    snapshot picks up corrections and deletions automatically.

    Args:
        con: Open DuckDB connection, normally inside run_pipeline's transaction.
        snapshot_date: Which snapshot in raw.sales to build from.
        codes: Sale codes lookup from read_sale_codes.
    """
    con.execute("CREATE SCHEMA IF NOT EXISTS clean")
    con.register("codes_df", codes)
    con.execute("CREATE OR REPLACE TABLE clean.sale_codes AS SELECT * FROM codes_df")
    con.unregister("codes_df")

    con.execute("""
        CREATE OR REPLACE TABLE clean.sales AS
        SELECT
            trim(s.PARID) AS parcel_id,
            TRY_CAST(s.SALEDATE AS DATE) AS sale_date,
            TRY_CAST(s.RECORDDATE AS DATE) AS record_date,
            TRY_CAST(nullif(trim(s.PRICE), '') AS DOUBLE) AS price,
            nullif(trim(s.DEEDBOOK), '') AS deed_book,
            nullif(trim(s.DEEDPAGE), '') AS deed_page,
            trim(s.SALECODE) AS sale_code,
            trim(s.SALEDESC) AS sale_desc,
            c.validity_group,
            trim(s.INSTRTYP) AS instrument_type,
            trim(s.INSTRTYPDESC) AS instrument_desc,
            trim(s.MUNICODE) AS muni_code,
            trim(s.MUNIDESC) AS muni_name, -- source pads names with trailing spaces
            trim(s.SCHOOLCODE) AS school_code,
            trim(s.SCHOOLDESC) AS school_name,
            nullif(trim(s.FULL_ADDRESS), '') AS full_address,
            nullif(trim(s.PROPERTYZIP), '') AS zip,
            -- blank, before the sale, or obviously mistyped (one row is dated year 0212)
            (record_date IS NULL OR record_date < sale_date OR year(record_date) < 2000) AS record_date_suspect,
            s.snapshot_date
        FROM raw.sales s
        LEFT JOIN clean.sale_codes c ON trim(s.SALECODE) = c.sale_code
        WHERE s.snapshot_date = ?
    """, [snapshot_date])


def build_price_growth(con: duckdb.DuckDBPyConnection, config: PipelineConfig) -> None:
    """
    Transform step: builds the output table analytics.muni_price_growth, which answers the question.

    The question is which municipalities saw the fastest growth in median arm's-length sale price
    between the base window (default 2012-13) and the end window (default 2021-22).

    How it's calculated, one stage per part of the SQL:
    1. valid_sales: keeps only sales the county coded "Valid Sale" and priced at or above
       config.min_valid_price, and labels each one 'base' or 'end' by its sale year. Family transfers,
       sheriff sales, multi-parcel deeds and the like are excluded because their prices don't reflect
       market value.
    2. by_window: per municipality, counts the sales and takes the median price in each window.
       Medians are used instead of means because a few very large sales (up to ~$149M) would
       dominate a mean.
    3. Final select: keeps only municipalities with at least config.min_sales_per_window sales in
       both windows (fewer gives an unreliable median), then computes the total % change, the
       annualized growth rate between the window midpoints, and a rank (1 = fastest growth).

    Columns: growth_rank, muni_code, muni_name, base_sales, end_sales, base_median_price,
    end_median_price, pct_change, annual_growth_pct.

    Args:
        con: Open DuckDB connection with clean.sales already built.
        config: Pipeline settings; uses base_years, end_years, min_valid_price, and min_sales_per_window.
    """
    con.execute("CREATE SCHEMA IF NOT EXISTS analytics")
    con.execute(f"""
        CREATE OR REPLACE TABLE analytics.muni_price_growth AS
        WITH valid_sales AS (
            SELECT
                muni_code,
                muni_name,
                price,
                CASE
                    WHEN year(sale_date) IN {config.base_years} THEN 'base'
                    WHEN year(sale_date) IN {config.end_years} THEN 'end'
                END AS time_window
            FROM clean.sales
            WHERE validity_group = 'Valid Sale' AND price >= ?
        ),
        by_window AS (
            SELECT
                muni_code,
                any_value(muni_name) AS muni_name,
                count(*) FILTER (WHERE time_window = 'base') AS base_sales,
                count(*) FILTER (WHERE time_window = 'end') AS end_sales,
                median(price) FILTER (WHERE time_window = 'base') AS base_median_price,
                median(price) FILTER (WHERE time_window = 'end') AS end_median_price
            FROM valid_sales
            WHERE time_window IS NOT NULL
            GROUP BY muni_code
        )
        SELECT
            rank() OVER (ORDER BY end_median_price / base_median_price DESC) AS growth_rank,
            muni_code,
            muni_name,
            base_sales,
            end_sales,
            base_median_price,
            end_median_price,
            round(100 * (end_median_price / base_median_price - 1), 1) AS pct_change,
            -- annualized, measured between the midpoints of the two windows
            round(100 * (pow(end_median_price / base_median_price, 1.00 / ({sum(config.end_years) / 2} - {sum(config.base_years) / 2})) - 1), 2) AS annual_growth_pct
        FROM by_window
        WHERE base_sales >= ? AND end_sales >= ?
        ORDER BY growth_rank
    """, [config.min_valid_price, config.min_sales_per_window, config.min_sales_per_window])


# --- Checks ---

def run_checks(con: duckdb.DuckDBPyConnection, snapshot_date: str, config: PipelineConfig) -> None:
    """
    Runs the data quality checks against the newly built clean and analytics tables.

    Every check runs, and all failures are collected and reported together, so one run shows every
    problem rather than only the first. Any failure raises DataQualityError, which makes run_pipeline
    roll back its transaction. The previously published tables then stay in place.

    Checks that fail the run:
    - raw and clean row counts match, and the snapshot isn't empty
    - (parcel_id, sale_date, deed_book, deed_page) is unique; this is the real key, since the source
      _id is reassigned on every refresh
    - every parcel ID is 16 characters
    - every sale date parsed and isn't in the future
    - no negative prices
    - every sale code has a validity group (dataset-specific: the data uses codes the dictionary
      doesn't list, such as PV)
    - no more than config.max_suspect_record_date_pct percent of rows have a suspect record date
      (dataset-specific: blank dates, dates before the sale, and a year-0212 typo)
    - the snapshot is no more than config.max_row_drop_pct percent smaller than the previous one
    - the output table isn't empty and has no impossible medians
    - the end window has full sales coverage (at least 20,000 sales in its last year)

    Warning only (prints but doesn't fail): recent years look incomplete. That doesn't affect the
    chosen windows, so it shouldn't block publishing.

    Args:
        con: Open DuckDB connection with the clean and analytics tables built.
        snapshot_date: The snapshot being published, used to compare against raw.sales and the previous snapshot.
        config: Pipeline settings with the check thresholds.

    Raises:
        DataQualityError: listing every check that failed.
    """
    def scalar(sql: str, params: list = None):
        """
        Runs a query that returns a single value (e.g. a count) and returns that value.
        """
        return con.execute(sql, params or []).fetchone()[0]

    failures = []

    # nothing gained or lost between raw and clean
    raw_rows = scalar("SELECT count(*) FROM raw.sales WHERE snapshot_date = ?", [snapshot_date])
    clean_rows = scalar("SELECT count(*) FROM clean.sales")
    if raw_rows != clean_rows:
        failures.append(f"raw has {raw_rows:,} rows but clean has {clean_rows:,}")
    if raw_rows == 0:
        failures.append("snapshot is empty")

    # the source's _id is reassigned on every refresh, so this is the key we rely on instead
    dup_keys = scalar("""
        SELECT count(*) FROM (
            SELECT 1 FROM clean.sales
            GROUP BY parcel_id, sale_date, deed_book, deed_page HAVING count(*) > 1
        )
    """)
    if dup_keys:
        failures.append(f"{dup_keys:,} duplicate (parcel_id, sale_date, deed_book, deed_page) keys")

    bad_parcels = scalar("SELECT count(*) FROM clean.sales WHERE parcel_id IS NULL OR length(parcel_id) <> 16")
    if bad_parcels:
        failures.append(f"{bad_parcels:,} parcel IDs that aren't 16 characters")

    bad_sale_dates = scalar("SELECT count(*) FROM clean.sales WHERE sale_date IS NULL OR sale_date > current_date")
    if bad_sale_dates:
        failures.append(f"{bad_sale_dates:,} sale dates missing, unparseable, or in the future")

    negative_prices = scalar("SELECT count(*) FROM clean.sales WHERE price < 0")
    if negative_prices:
        failures.append(f"{negative_prices:,} negative prices")

    # written for this dataset: the data uses codes the 2015 dictionary doesn't list (e.g. PV),
    # and an unrecognized code would silently drop sales out of (or into) the valid-sale analysis
    unknown_codes = con.execute("""
        SELECT sale_code, count(*) FROM clean.sales WHERE validity_group IS NULL GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    if unknown_codes:
        failures.append(f"sale codes not in the dictionary or reviewed list: {unknown_codes}")

    # also from this dataset: blank record dates, record dates before the sale, and a year-0212 typo.
    # a handful is expected; a jump means something changed upstream
    suspect_pct = scalar("SELECT 100.0 * avg(record_date_suspect::INT) FROM clean.sales")
    if suspect_pct > config.max_suspect_record_date_pct:
        failures.append(f"{suspect_pct:.2f}% of rows have a suspect record date (limit {config.max_suspect_record_date_pct}%)")

    # a new snapshot that's much smaller than the last one is more likely a broken export than real deletions
    previous_rows = scalar("""
        SELECT count(*) FROM raw.sales
        WHERE snapshot_date = (SELECT max(snapshot_date) FROM raw.sales WHERE snapshot_date < ?)
    """, [snapshot_date])
    if previous_rows and clean_rows < previous_rows * (1 - config.max_row_drop_pct / 100):
        failures.append(f"row count fell from {previous_rows:,} to {clean_rows:,}")

    # the output table itself
    output_rows = scalar("SELECT count(*) FROM analytics.muni_price_growth")
    if output_rows == 0:
        failures.append("analytics.muni_price_growth is empty")
    bad_medians = scalar("SELECT count(*) FROM analytics.muni_price_growth WHERE base_median_price <= 0 OR end_median_price IS NULL")
    if bad_medians:
        failures.append(f"{bad_medians:,} municipalities with an impossible median price")

    # warning only: the 2026-10-26 snapshot was missing most sales after 2022 despite monthly updates.
    # recent gaps don't affect the 2012-2022 analysis, so they shouldn't block the run
    latest_full_year = scalar("""
        SELECT max(yr) FROM (
            SELECT year(sale_date) AS yr FROM clean.sales GROUP BY 1 HAVING count(*) >= 20000
        )
    """)
    if latest_full_year is None or latest_full_year < max(config.end_years):
        failures.append(f"end window runs through {max(config.end_years)} but the last year with full coverage is {latest_full_year}")
    elif latest_full_year < pd.Timestamp.today().year - 1:
        print(f"WARNING: last year with full sales coverage is {latest_full_year}; recent sales look incomplete")

    if failures:
        raise DataQualityError("Data quality checks failed:\n  - " + "\n  - ".join(failures))
    print(f"All data quality checks passed ({clean_rows:,} rows, {output_rows} municipalities in output)")


# --- Run ---

def run_pipeline(config: PipelineConfig = PIPELINE_CONFIG) -> None:
    """
    Runs the whole pipeline end to end: extract -> land raw -> transform -> check -> publish.

    1. extract_raw downloads the current version of the dataset, unless it's already on disk.
    2. read_sale_codes parses the codes dictionary.
    3. load_raw_snapshot appends the file to raw.sales, unless that snapshot is already loaded. This
       is committed on its own, so a later failure never throws away a good download.
    4. build_clean_sales, build_price_growth, and run_checks run inside one transaction. If every
       check passes it commits, publishing the new tables. If anything fails it rolls back, so the
       last good tables stay in place, and re-raises the error.
    5. Prints the top 10 municipalities from the output table.

    Safe to run any number of times: an unchanged source means no new download, no new raw rows, and
    the same clean and output tables.

    Args:
        config: Pipeline settings; defaults to PIPELINE_CONFIG.

    Raises:
        DataQualityError: if a load-time or post-build quality check fails.
        requests.HTTPError / RuntimeError: if the download fails (see extract_raw).
    """
    raw_path = extract_raw(API_CONFIG, RAW_DIR)
    codes = read_sale_codes(config)

    config.db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(config.db_path)) as con:
        # raw landing is committed on its own, so a failed check later doesn't throw away the download
        snapshot_date = load_raw_snapshot(con, raw_path)

        # clean + analytics are rebuilt and checked in one transaction: all of it is published or none of it is
        con.execute("BEGIN TRANSACTION")
        try:
            build_clean_sales(con, snapshot_date, codes)
            build_price_growth(con, config)
            run_checks(con, snapshot_date, config)
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        print("\nFastest-growing municipalities (median valid sale price):")
        print(con.execute("""
            SELECT growth_rank, muni_name, base_sales, end_sales, base_median_price, end_median_price, pct_change, annual_growth_pct
            FROM analytics.muni_price_growth ORDER BY growth_rank LIMIT 10
        """).df().to_string(index=False))


if __name__ == "__main__":
    run_pipeline()
