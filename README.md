# Allegheny County Property Sales Pipeline

A small, reproducible pipeline that ingests the [Allegheny County Property Sale Transactions](https://data.wprdc.org/dataset/real-estate-sales) dataset from the WPRDC CKAN API, lands it unchanged, models it into a clean layer, checks its quality, and publishes one analytical table.

## The question

> **Which Allegheny County municipalities saw the fastest growth in median arm's-length property sale price between 2012–13 and 2021–22?**

The answer is the table `analytics.muni_price_growth`.

## Quick start

Requires Python 3.11+ (developed on 3.12). No cloud accounts or API keys necessary.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main2.py
```

The run downloads the data (~86 MB), builds `data/sales.duckdb`, runs the quality checks, and prints the top 10 municipalities. Run it again and nothing changes.

To query the results:

```python
import duckdb
con = duckdb.connect("data/sales.duckdb", read_only=True)
con.sql("SELECT * FROM analytics.muni_price_growth ORDER BY growth_rank").show()
```

## How the requirements are met

| Requirement                                            | Where                                                                                                                                                                                                                 |
| ------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Ingest programmatically, no manual downloads           | `extract_raw()` in `main2.py` gets the resource's download URL from the CKAN `resource_show` API, then streams the file.                                                                                              |
| Land raw data unchanged, then a cleaned layer          | The CSV is saved byte-for-byte to `data/raw/`. `raw.sales` stores every column as text, exactly as published. `clean.sales` holds the typed, trimmed version.                                                         |
| Re-runnable, no duplicates or corruption               | Raw snapshots are keyed by the source's `last_modified` date and skipped if already loaded. The clean and analytics layers are rebuilt and checked inside one transaction. See [Handling updates](#handling-updates). |
| Data quality checks that fail on bad data              | `run_checks()` in `main2.py`. Two checks exist because of things found in this dataset. See [Data quality checks](#data-quality-checks).                                                                              |
| One output table answering a stated question           | `analytics.muni_price_growth`. See [The output table](#the-output-table).                                                                                                                                             |
| README covering decisions, surprises, scale, omissions | This file.                                                                                                                                                                                                            |
| AI disclosure                                          | [AI disclosure](#ai-disclosure).                                                                                                                                                                                      |

## Pipeline overview

```
CKAN resource_show ──► data/raw/sales_<last_modified>.csv   (byte-for-byte, + .meta.json)
                                │
                                ▼
                         raw.sales          all text, one copy per snapshot, never modified
                                │
5_sales-validation-codes ──► clean.sale_codes
                                │
                                ▼
                         clean.sales        typed, trimmed, validity group, suspect-date flag
                                │
                                ▼
                  analytics.muni_price_growth
                                │
                                ▼
                         run_checks()  ──► COMMIT, or ROLLBACK leaving the last good tables
```

| File                                      | Purpose                                                                                                                                              |
| ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `main2.py`                                | The whole pipeline, in run order: config → extract (download) → load (raw + sale codes) → transform (clean + analytics) → checks → `run_pipeline()`. |
| `5_sales-validation-codes-dictionary.csv` | The county's sale validation code dictionary, obtained from the dataset's Data Steward (see below).                                                  |

## The output table

`analytics.muni_price_growth`, one row per municipality:

| Column                                  | Meaning                                                         |
| --------------------------------------- | --------------------------------------------------------------- |
| `growth_rank`                           | 1 = fastest growth                                              |
| `muni_code`, `muni_name`                | Municipality (Pittsburgh is split into wards, as in the source) |
| `base_sales`, `end_sales`               | Number of valid sales in 2012–13 and 2021–22                    |
| `base_median_price`, `end_median_price` | Median valid sale price in each window                          |
| `pct_change`                            | Total % change in median price                                  |
| `annual_growth_pct`                     | Annualized growth between the window midpoints (9 years)        |

Rules:

- Only sales the county coded as **Valid Sale** (codes `0`, `U`, `UR`) are counted.
- Sales under $1,000 are excluded.
- Only municipalities with at least 30 valid sales in **both** windows are included. That leaves 92 of 175.

All thresholds live in `PipelineConfig` in `main2.py`.

### Results (snapshot of 2026-10-26)

Later snapshots shift a few positions slightly. On the 2026-11-02 snapshot the top 5 and the county-wide figures are unchanged, and 24th Ward and Clairton swap places at 6 and 7.

County-wide, the median valid sale price rose from **$140,000** in 2012–13 to **$220,000** in 2021–22 (+57%).

Fastest growth:

| Rank | Municipality           | Sales (base → end) | Median price (base → end) | Change | Annual |
| ---- | ---------------------- | ------------------ | ------------------------- | ------ | ------ |
| 1    | 6th Ward - Pittsburgh  | 46 → 71            | $91,500 → $395,000        | +332%  | 17.7%  |
| 2    | 9th Ward - Pittsburgh  | 75 → 105           | $103,000 → $328,000       | +218%  | 13.7%  |
| 3    | 18th Ward - Pittsburgh | 47 → 66            | $42,000 → $133,125        | +217%  | 13.7%  |
| 4    | 10th Ward - Pittsburgh | 92 → 192           | $94,750 → $268,000        | +183%  | 12.3%  |
| 5    | 11th Ward - Pittsburgh | 72 → 110           | $150,500 → $408,000       | +171%  | 11.7%  |
| 6    | 2nd Ward - Clairton    | 30 → 58            | $27,000 → $68,500         | +154%  | 10.9%  |
| 7    | 24th Ward - Pittsburgh | 36 → 55            | $52,250 → $130,000        | +149%  | 10.7%  |
| 8    | Tarentum               | 31 → 64            | $40,000 → $93,500         | +134%  | 9.9%   |
| 9    | Swissvale              | 86 → 130           | $72,050 → $165,000        | +129%  | 9.6%   |
| 10   | McKees Rocks           | 39 → 74            | $35,000 → $80,000         | +129%  | 9.6%   |

Slowest growth: Pittsburgh's 2nd Ward (+13%), Aleppo (+25%), Harmar (+29%), Collier (+36%), Franklin Park (+36%).

The pattern: the fastest growth is in lower-priced Pittsburgh wards and older river towns starting from a low base. Already-expensive suburbs grew slowest in percentage terms. See [What I didn't do](#what-i-didnt-do-and-why) for why "median price grew" is not the same as "homes there gained value".

<details>
<summary><b>Full results table: all 92 municipalities</b> (<code>analytics.muni_price_growth</code>, 2026-10-26 snapshot)</summary>

| Rank | Municipality | Sales (base → end) | Median price (base → end) | Change | Annual |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | 6th Ward - Pittsburgh | 46 → 71 | $91,500 → $395,000 | +331.7% | 17.65% |
| 2 | 9th Ward - Pittsburgh | 75 → 105 | $103,000 → $328,000 | +218.4% | 13.73% |
| 3 | 18th Ward - Pittsburgh | 47 → 66 | $42,000 → $133,125 | +217.0% | 13.68% |
| 4 | 10th Ward - Pittsburgh | 92 → 192 | $94,750 → $268,000 | +182.8% | 12.25% |
| 5 | 11th Ward - Pittsburgh | 72 → 110 | $150,500 → $408,000 | +171.1% | 11.72% |
| 6 | 2nd Ward - Clairton | 30 → 58 | $27,000 → $68,500 | +153.7% | 10.90% |
| 7 | 24th Ward - Pittsburgh | 36 → 55 | $52,250 → $130,000 | +148.8% | 10.66% |
| 8 | Tarentum | 31 → 64 | $40,000 → $93,500 | +133.8% | 9.89% |
| 9 | Swissvale | 86 → 130 | $72,050 → $165,000 | +129.0% | 9.64% |
| 10 | McKees Rocks | 39 → 74 | $35,000 → $80,000 | +128.6% | 9.62% |
| 11 | Stowe | 37 → 97 | $45,000 → $101,000 | +124.4% | 9.40% |
| 12 | 19th Ward - Pittsburgh | 247 → 465 | $82,500 → $180,000 | +118.2% | 9.06% |
| 13 | Jefferson Hills | 103 → 173 | $133,000 → $285,000 | +114.3% | 8.84% |
| 14 | Brackenridge | 42 → 50 | $47,750 → $100,500 | +110.5% | 8.62% |
| 15 | 29th Ward - Pittsburgh | 69 → 157 | $65,000 → $135,000 | +107.7% | 8.46% |
| 16 | 27th Ward - Pittsburgh | 77 → 153 | $85,000 → $175,000 | +105.9% | 8.35% |
| 17 | Munhall | 89 → 162 | $68,500 → $137,450 | +100.7% | 8.05% |
| 18 | 32nd Ward - Pittsburgh | 50 → 96 | $83,500 → $167,000 | +100.0% | 8.01% |
| 19 | 8th Ward - Pittsburgh | 69 → 66 | $156,001 → $310,000 | +98.7% | 7.93% |
| 20 | 20th Ward - Pittsburgh | 80 → 197 | $65,350 → $127,500 | +95.1% | 7.71% |
| 21 | Liberty | 31 → 43 | $67,000 → $130,000 | +94.0% | 7.64% |
| 22 | Indiana | 85 → 91 | $190,000 → $360,000 | +89.5% | 7.36% |
| 23 | Dormont | 142 → 146 | $124,950 → $234,750 | +87.9% | 7.26% |
| 24 | Coraopolis | 47 → 99 | $80,000 → $150,000 | +87.5% | 7.23% |
| 25 | Wilkins | 54 → 125 | $87,450 → $163,700 | +87.2% | 7.21% |
| 26 | 26th Ward - Pittsburgh | 65 → 104 | $76,500 → $141,500 | +85.0% | 7.07% |
| 27 | Penn Hills | 307 → 813 | $78,500 → $145,000 | +84.7% | 7.06% |
| 28 | Emsworth | 37 → 39 | $106,000 → $195,000 | +84.0% | 7.01% |
| 29 | Ingram | 41 → 37 | $100,000 → $182,500 | +82.5% | 6.91% |
| 30 | Brentwood | 83 → 203 | $89,900 → $162,000 | +80.2% | 6.76% |
| 31 | 28th Ward - Pittsburgh | 69 → 140 | $93,878 → $168,950 | +80.0% | 6.75% |
| 32 | West Mifflin | 161 → 297 | $83,500 → $150,000 | +79.6% | 6.73% |
| 33 | Aspinwall | 53 → 44 | $210,000 → $372,500 | +77.4% | 6.58% |
| 34 | 4th Ward - Pittsburgh | 99 → 132 | $140,000 → $247,500 | +76.8% | 6.54% |
| 35 | Harrison | 158 → 185 | $82,150 → $145,000 | +76.5% | 6.52% |
| 36 | Baldwin Boro | 206 → 335 | $110,000 → $192,360 | +74.9% | 6.41% |
| 37 | O''Hara | 183 → 183 | $237,500 → $414,500 | +74.5% | 6.38% |
| 38 | Springdale Boro | 44 → 48 | $79,950 → $138,500 | +73.2% | 6.30% |
| 39 | White Oak | 55 → 136 | $92,500 → $160,000 | +73.0% | 6.28% |
| 40 | South Park | 125 → 255 | $142,000 → $243,000 | +71.1% | 6.15% |
| 41 | 15th Ward - Pittsburgh | 73 → 165 | $134,000 → $228,723 | +70.7% | 6.12% |
| 42 | Shaler | 238 → 468 | $146,750 → $250,000 | +70.4% | 6.10% |
| 43 | Bellevue | 55 → 87 | $114,900 → $195,000 | +69.7% | 6.05% |
| 44 | South Fayette | 215 → 402 | $195,000 → $329,450 | +68.9% | 6.00% |
| 45 | Hampton | 261 → 332 | $200,000 → $337,500 | +68.8% | 5.99% |
| 46 | Oakmont | 56 → 155 | $163,000 → $274,000 | +68.1% | 5.94% |
| 47 | Greentree | 91 → 105 | $149,000 → $249,000 | +67.1% | 5.87% |
| 48 | Carnegie | 92 → 151 | $126,500 → $211,000 | +66.8% | 5.85% |
| 49 | North Versailles | 67 → 142 | $75,000 → $124,500 | +66.0% | 5.79% |
| 50 | Castle Shannon | 116 → 156 | $108,000 → $179,250 | +66.0% | 5.79% |
| 51 | 14th Ward - Pittsburgh | 318 → 401 | $269,550 → $446,500 | +65.6% | 5.77% |
| 52 | Sharpsburg | 33 → 30 | $80,000 → $132,500 | +65.6% | 5.77% |
| 53 | West View | 74 → 127 | $117,750 → $195,000 | +65.6% | 5.76% |
| 54 | West Deer | 104 → 168 | $162,435 → $268,000 | +65.0% | 5.72% |
| 55 | Forest Hills | 63 → 166 | $120,000 → $197,950 | +65.0% | 5.72% |
| 56 | Plum | 233 → 431 | $133,900 → $220,000 | +64.3% | 5.67% |
| 57 | Ross | 359 → 676 | $144,000 → $235,000 | +63.2% | 5.59% |
| 58 | Whitehall | 187 → 295 | $132,500 → $216,000 | +63.0% | 5.58% |
| 59 | Richland | 232 → 221 | $202,450 → $326,000 | +61.0% | 5.44% |
| 60 | McCandless | 333 → 530 | $202,000 → $325,000 | +60.9% | 5.43% |
| 61 | Edgewood | 45 → 52 | $188,000 → $299,659 | +59.4% | 5.32% |
| 62 | Avalon | 41 → 83 | $110,000 → $174,900 | +59.0% | 5.29% |
| 63 | Sewickley | 72 → 92 | $267,500 → $425,000 | +58.9% | 5.28% |
| 64 | Mt.Lebanon | 380 → 579 | $217,425 → $345,000 | +58.7% | 5.26% |
| 65 | Pine | 245 → 336 | $381,000 → $600,000 | +57.5% | 5.18% |
| 66 | Robinson | 227 → 230 | $169,900 → $266,000 | +56.6% | 5.11% |
| 67 | Bridgeville | 49 → 92 | $119,878 → $187,500 | +56.4% | 5.10% |
| 68 | Crafton | 50 → 88 | $122,288 → $191,250 | +56.4% | 5.09% |
| 69 | Scott | 211 → 354 | $132,000 → $206,250 | +56.3% | 5.08% |
| 70 | Moon | 344 → 422 | $185,000 → $287,888 | +55.6% | 5.04% |
| 71 | Monroeville | 228 → 529 | $130,000 → $200,000 | +53.8% | 4.90% |
| 72 | 17th Ward - Pittsburgh | 73 → 164 | $172,700 → $265,500 | +53.7% | 4.89% |
| 73 | Bethel Park | 361 → 605 | $160,000 → $245,000 | +53.1% | 4.85% |
| 74 | Kennedy | 145 → 133 | $167,000 → $255,000 | +52.7% | 4.82% |
| 75 | Upper St. Clair | 225 → 455 | $259,999 → $397,000 | +52.7% | 4.82% |
| 76 | Pleasant Hills | 80 → 177 | $152,126 → $230,000 | +51.2% | 4.70% |
| 77 | Churchill | 46 → 82 | $152,514 → $230,000 | +50.8% | 4.67% |
| 78 | North Fayette | 155 → 264 | $159,150 → $239,950 | +50.8% | 4.67% |
| 79 | Marshall | 96 → 240 | $302,000 → $450,000 | +49.0% | 4.53% |
| 80 | Elizabeth Twp | 108 → 170 | $119,950 → $176,750 | +47.4% | 4.40% |
| 81 | Ohio | 118 → 129 | $301,695 → $439,900 | +45.8% | 4.28% |
| 82 | 7th Ward - Pittsburgh | 147 → 227 | $243,000 → $353,000 | +45.3% | 4.24% |
| 83 | 16th Ward - Pittsburgh | 57 → 108 | $131,750 → $190,350 | +44.5% | 4.17% |
| 84 | Pennsbury Village | 39 → 57 | $105,000 → $150,000 | +42.9% | 4.04% |
| 85 | Wilkinsburg | 77 → 143 | $82,900 → $118,000 | +42.3% | 4.00% |
| 86 | Fox Chapel | 103 → 101 | $540,000 → $750,000 | +38.9% | 3.72% |
| 87 | Findlay | 56 → 115 | $215,000 → $295,000 | +37.2% | 3.58% |
| 88 | Franklin Park | 159 → 261 | $323,000 → $440,000 | +36.2% | 3.49% |
| 89 | Collier | 81 → 166 | $259,990 → $354,000 | +36.2% | 3.49% |
| 90 | Harmar | 57 → 45 | $170,000 → $219,000 | +28.8% | 2.85% |
| 91 | Aleppo | 38 → 30 | $184,750 → $231,000 | +25.0% | 2.51% |
| 92 | 2nd Ward - Pittsburgh | 30 → 80 | $282,500 → $319,390 | +13.1% | 1.37% |

</details>

## Data quality checks

Any failed check raises `DataQualityError`. The transaction rolls back, so the previously published clean and analytics tables stay in place. The raw snapshot is committed separately beforehand, so a failed check never throws away a download.

| Check                                                                                           | Why                                                                                                                                                                                              |
| ----------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Parsed row count equals the file's line count, and column names have no stray whitespace        | Guards the relaxed CSV parsing (see [Surprises](#surprises))                                                                                                                                     |
| Raw and clean row counts match                                                                  | Nothing gained or lost in the transform                                                                                                                                                          |
| `(parcel_id, sale_date, deed_book, deed_page)` is unique                                        | This is the real key. The source `_id` is reassigned on every refresh.                                                                                                                           |
| Parcel IDs are 16 characters                                                                    | Documented format                                                                                                                                                                                |
| Sale dates parse and aren't in the future                                                       |                                                                                                                                                                                                  |
| No negative prices                                                                              |                                                                                                                                                                                                  |
| **Every sale code is in the dictionary, its discontinued list, or a hand-reviewed list**        | _Dataset-specific._ The data uses `PV`, which the 2015 dictionary doesn't list. An unrecognized code would silently move sales in or out of the analysis. This check stopped the first real run. |
| **At most 1% of rows have a suspect record date** (blank, before the sale date, or before 2000) | _Dataset-specific._ About 0.5% of rows have these problems today, including one dated year 0212. A jump would mean something changed upstream.                                                   |
| A new snapshot is no more than 5% smaller than the previous one                                 | A shrinking file is more likely a broken export than real deletions                                                                                                                              |
| The output table is non-empty with positive medians                                             |                                                                                                                                                                                                  |
| The analysis end window has full sales coverage                                                 | Fails if the window is moved into the sparse 2023+ years                                                                                                                                         |
| _(warning only)_ Recent sales coverage looks incomplete                                         | Fires today. It doesn't affect the 2012–2022 analysis, so it doesn't block the run.                                                                                                              |

## Handling updates

WPRDC refreshes this dataset monthly. The county also **corrects and deletes past records** between refreshes, and the source `_id` column is regenerated each time. So:

1. `extract_raw()` checks the resource's `last_modified` date. If that version is already on disk, there's no download.
2. A new version is saved under its own name (`sales_<last_modified>.csv`) and appended to `raw.sales` as a new snapshot. Older snapshots are kept, so I can always see what changed or rebuild from history.
3. `clean.sales` and the output table are **fully rebuilt** from the newest snapshot. Corrections and deletions are picked up automatically, with no upsert logic.
4. The snapshot-size check catches a truncated or broken export before it's published.

## Three most important design decisions

### 1. Immutable raw snapshots + full rebuild of the clean layer

**Chosen:** Keep every published version of the file unchanged, and rebuild `clean.sales` from the newest one each run.

**Rejected:**

- _Incremental upsert keyed on `_id`._ `_id` isn't stable: this snapshot's IDs run from 17,472,841 to 17,870,840 for only 397,885 rows, a sign the table is reloaded with new IDs each refresh.
- _Incremental upsert keyed on the natural key._ An upsert never sees records the county deleted, so stale rows would pile up.

At ~400k rows a full rebuild takes about 3 seconds, so the simpler approach is also the correct one.

### 2. DuckDB, with SQL transforms published atomically

**Chosen:** A single DuckDB file. The transforms are SQL, and the clean layer, analytics layer and checks all run in one transaction.

**Rejected:**

- _SQLite._ It comes with Python, but it has no `median()` and is weaker for analytical SQL.
- _Postgres in Docker._ Real infrastructure for a single-user, 86 MB job.
- _dbt._ Adds a lot of structure for three models.
- _pandas-only with CSV outputs._ No atomic publish, so a failed check could leave half-written files.

I also chose to download the full CSV dump rather than page through `datastore_search` (13+ requests at 32k rows each). The SQL endpoint, `datastore_search_sql`, returned HTTP 403.

### 3. Defining "price" using the county's own sale validation codes

**Chosen:** Count only sales the county coded as valid (arm's-length). Use medians, pooled over two-year windows. Require 30 sales in each window. End the analysis in 2022.

**Rejected:**

- _All transfers with a price filter (e.g. > $10,000)._ Many non-market transfers have realistic prices: sheriff sales, multi-parcel deeds, corporate transfers, estate sales. The county has already reviewed every sale, and ignoring that work would be a mistake.
- _Means._ A few outliers dominate; the maximum price is $148.8M.
- _Single-year comparisons._ Too noisy for small municipalities.
- _Ending in 2024 or 2025._ When I built the analysis, sales after 2022 were badly incomplete (see below).

## Surprises

Figures below are from the 2026-10-26 snapshot (397,885 rows), unless noted.

- **Sales almost disappear after 2022.** Every year from 2012 to 2022 has 33,000–40,000 sales. Then 2023 has 4,653, 2024 has 4,740, 2025 has 509 and 2026 has 3. The dataset says it's updated monthly, and record dates run into October 2026, but recent sales are mostly missing. This could be a recording lag or an upstream problem, and it's why I chose to leave everything after 2022 out of the analysis.
- **...and then they came back.** The 2026-11-02 refresh added about 109,000 rows (507,028 in total), backfilling 2023–2026 to roughly 31,000 sales a year. The pipeline handled this as designed: it loaded the refresh as a new snapshot, rebuilt the clean and output tables, and passed every check, all with no code changes. The source `_id`s were reassigned again (now 19,391,857–19,899,013), which confirms they can't be used as a key.
- **Only about 17% of transfers are market sales.** "Love and affection" (family) transfers alone number 79,118. Only 67,430 sales are code `0` (valid), and 92,042 transfers have a price of $1 or less.
- **The key to reading the data wasn't published with it.** The API documentation refers to a "Sales Validation Codes Dictionary" several times, but I couldn't find it on the dataset page. Without it, there's no reliable way to tell an arm's-length sale from a family transfer or sheriff sale, which the whole analysis depends on. As a result, I contacted Mary Beth Snyder (the dataset's listed Data Steward) and she sent me the file. Since it isn't available through the API, it's committed to the repo as reference data, the one input the pipeline doesn't download.
- **The dictionary doesn't match the data.**
  - `PV` (32 rows) isn't in the dictionary.
  - About 7,000 rows still use codes the dictionary calls discontinued (`Z`, `28`, `ND`, `5`, `X`, …).
  - The data labels `UR` as "OTHER" while the dictionary says "OTHER VALID".
  - The dictionary file itself is Windows-1252 encoded, not UTF-8.
- **The CSV has mixed line endings.** The header line ends with `\r\n` and every data row with `\n`, so DuckDB's strict CSV parser refuses the file. I turned strict mode off and added a row-count check to make up for it.
- **The source `_id` is not a stable key** (see design decision 1).
- **Dirty dates.** 1,246 blank record dates, one record date in the year 0212, and 713 sales recorded _before_ they happened. The data also includes 2012 sales, though the dataset description says "2013–present".
- **Smaller issues:**
  - 282,499 municipality names are padded with trailing spaces.
  - 2,697 prices are blank.
  - 561 rows have no deed book.
  - 18,860 deeds cover more than one parcel, each carrying the full deed price, which is why multi-parcel sales are coded invalid.

## At 100x scale or with daily refreshes

At ~40M rows:

- **Stop storing full copies.** Today every snapshot is stored in full. I'd land raw files as partitioned Parquet in object storage and load only _changes_: compare each snapshot to the last one on the natural key and record inserts, updates and deletes.
- **Build incrementally.** I'd rebuild only affected partitions (e.g. by sale year) instead of everything, probably as dbt incremental models.
- **Use a warehouse.** DuckDB would still handle the transforms on one machine for a while. For many users I'd move to Postgres or a cloud warehouse.

With daily refreshes:

- **Cheap no-op runs.** The source only changes monthly, so a daily run would make one `resource_show` call and exit if `last_modified` hasn't changed. This already works, but snapshots are named by **date**, so two publishes on the same day would collide. I'd switch to the full timestamp or a content hash.
- **Scheduling and alerting.** I'd use an orchestrator (Dagster, Airflow or cron), page someone when a check fails, and write check results to a table so I could track trends rather than only pass/fail.
- **Smarter thresholds.** Several checks would compare against recent history (e.g. row count within N standard deviations of the last 12 snapshots) instead of fixed percentages.

## What I didn't do, and why

- **No update to the end window after the backfill.** Now that 2023–2025 are filled in, comparing 2012–13 against 2024–25 would be the natural next step (it's a one-line change to `end_years` in `PipelineConfig`). The 2026-11-02 refresh arrived on the submission deadline, which didn't leave time to re-check the results and write-up against a new window.
- **No repeat-sales or quality-adjusted price index.** Median price measures _which_ properties sold as well as how prices moved. If a ward's sales shift toward new construction or renovations, its median rises even if existing homes didn't gain value. A repeat-sales index (comparing the same parcel's sales over time) would answer the question more accurately, but it's a significant modeling project.
- **No property-type filter.** The sales dataset doesn't include property class (residential, commercial, etc.). Getting it means joining the separate property assessments dataset. The valid-sale filter removes most non-residential noise, but not all.
- **No inflation adjustment.** Prices are nominal dollars. Cumulative CPI inflation from 2012–13 to 2021–22 was roughly 20–25%, which would shrink every growth figure but barely change the ranking.
- **No quarantine table.** Rows with suspect record dates are flagged (`record_date_suspect`) rather than moved aside. Record date isn't used in the analysis, so dropping them would only lose valid sale prices.
- **No geography or maps.** Municipality names are enough to answer the question; mapping would need boundary shapefiles.
- **No scheduling, CI, or snapshot cleanup.** These are out of scope for a local, run-on-demand exercise. Raw snapshots will accumulate (~86 MB each) until a cleanup policy is added.

## AI disclosure

I used **Claude Code (Claude Opus 5.5)** as a coding assistant. Specifically:

- **Data profiling:** It ran exploratory queries against the API and the raw file. That surfaced the post-2022 coverage gap, the unstable `_id`, the undocumented `PV` code and the dirty dates.
- **Code:** It wrote most of the first draft of `main2.py` (raw loading, sale code parsing, clean and analytics SQL, quality checks) on top of my extract code. It also diagnosed the mixed-line-ending CSV problem, and added function docstrings which I reviewed.
- **This README:** It wrote the first draft, which I then edited. It also did a final review: it re-ran the pipeline from a fresh clone and checked the README's claims against the data.

I chose the dataset and the question; I reviewed and approved the design decisions before any code was written; I refined the code and pipeline once it was written; and I discovered the need for the "Sales Validation Codes" dictionary.
