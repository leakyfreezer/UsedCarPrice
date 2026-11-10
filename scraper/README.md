# Auto24.ee Academic Vehicle Data Scraper

A polite, modular Python data ingestion tool built for academic data science coursework. It extracts used vehicle specifications from **Auto24.ee** and normalizes them into a clean tabular schema aligned with the **eBay Kleinanzeigen used cars dataset**.

---

## Key Features

1. **Multi-Day Incremental Collection & Deduplication:**
   - `--limit N` (default `1000`) defines the number of **new, previously unseen** listings to collect in the current session.
   - On startup, the scraper loads all previously collected `listing_id`s from both your existing output file (`.csv` or `.parquet`) and a persistent state file (`data/.auto24_state.json`).
   - Already-collected listings are skipped at zero network cost on search pages, and new records are appended cleanly to your existing dataset.
2. **Academic & Ethical Guardrails:**
   - Strict randomized jitter delay (`2.0` to `4.0` seconds) between requests.
   - Zero Personal Identifiable Information (PII) collected: seller names, phone numbers, registration plates (`numbrimärk`), and VINs (`tehasetähis`) are strictly excluded.
3. **Two Extraction Modes:**
   - **Fast Search-Row Mode (Default):** Extracts all core schema attributes directly from Auto24 search result rows (`50` vehicles per HTTP request, collecting `1,000` vehicles in ~20 requests / ~1 minute).
   - **Detail-Page Enrichment Mode (`--fetch-details`):** Optionally visits each unseen vehicle's detail page (`/soidukid/<id>`) to extract registration month (`MM/YYYY`) and accident/damage status (`avariiline`).
4. **Resilient Transport & Checkpointing:**
   - Uses `curl_cffi` (impersonating Chrome TLS fingerprints) + `tenacity` exponential backoff for HTTP `429`/`5xx` responses.
   - Flushes progress to disk every `50` records (or immediately on `Ctrl+C` interruption).

---

## Prerequisites & Setup

Requires **Python 3.10+**. Install dependencies via `pip`:

```powershell
python -m pip install -r requirements.txt
```

---

## Usage Examples

### Day 1: Collect your first 1,000 listings
```powershell
python auto24_scraper.py --category passenger --limit 1000 --output data/auto24_vehicles.csv
```

### Day 2: Collect the *next* 1,000 new listings into the same file
Simply run the **exact same command** on the next day:
```powershell
python auto24_scraper.py --category passenger --limit 1000 --output data/auto24_vehicles.csv
```
- **How it works:** The scraper loads the 1,000 `listing_id`s from Day 1, scans from Page 1 (catching any brand-new listings posted overnight while skipping the 1,000 cars already in your CSV), and automatically continues into deeper pages until **1,000 brand-new listings** have been added (bringing `data/auto24_vehicles.csv` to 2,000 unique rows).
- **Optional Fast Page Resume (`--resume-page`):** If you want to jump straight to the page number where yesterday's run finished instead of scanning Page 1 for newly posted cars first:
  ```powershell
  python auto24_scraper.py --category passenger --limit 1000 --output data/auto24_vehicles.csv --resume-page
  ```

### Export to Parquet or Enrich with Detail Pages
```powershell
# Save directly to Apache Parquet format
python auto24_scraper.py --category all_cars --limit 1000 --output data/auto24_vehicles.parquet

# Fetch individual detail pages for registration month & damage indicators (slower)
python auto24_scraper.py --category passenger --limit 200 --fetch-details --output data/auto24_detailed.csv

# Two-phase sold check: sweeps 60 search pages (3,000 cars) to confirm active cars + price drops,
# then checks detail pages (up to --limit 200) only for missing IDs
python auto24_scraper.py --check-sold --sweep-pages 60 --limit 200 --output data/auto24_vehicles.csv
```

---

## CLI Reference

| Flag | Default | Description |
| :--- | :--- | :--- |
| `--category` | `passenger` | Vehicle category: `passenger` (cars), `suv`, `commercial`, or `all_cars`. |
| `--limit` | `1000` | Number of **new (unseen)** vehicle listings to collect in this session. |
| `--output` | `data/auto24_vehicles.csv` | Target `.csv` or `.parquet` file (appended incrementally). |
| `--state-file` | `data/.auto24_state.json` | JSON state file tracking seen IDs and last completed page. |
| `--start-page` | `1` | Search result page number to begin scanning from. |
| `--resume-page` | `False` | Jump directly to `last_completed_page + 1` from the previous run. |
| `--fetch-details` | `False` | Fetch each unseen listing's detail page for `month` and `damaged` fields. |
| `--check-sold` | `False` | Two-phase sold check: sweeps search pages first (`50` cars/page), then checks detail pages only for missing IDs. |
| `--sweep-pages` | `60` | Number of search pages (`50` cars/page) to sweep in Phase 1 of `--check-sold`. |
| `--checkpoint-every` | `50` | Flush buffered records to disk every $N$ new listings. |
| `--min-delay` | `2.0` | Minimum polite delay (seconds) between requests. |
| `--max-delay` | `4.0` | Maximum polite delay (seconds) between requests. |

---

## Output Data Schema

| Column | Type | Description & Estonian Normalization |
| :--- | :--- | :--- |
| `listing_id` | `string` | Unique Auto24 vehicle ID extracted from `/soidukid/<id>` |
| `make` | `string` | Normalized manufacturer name (`Toyota`, `BMW`, `Mercedes-Benz`) |
| `model` | `string` | Vehicle model (`RAV4`, `320d`, `Passat`) |
| `year` | `Int64` | 4-digit year of initial registration (`Esmane reg.`) |
| `month` | `Int64` | Month of initial registration (`1`–`12`, populated with `--fetch-details`) |
| `price_eur` | `Float64` | Initial asking/discount price in EUR when first scraped |
| `latest_price_eur` | `Float64` | Most recent asking price in EUR (updated during `--check-sold` search sweeps) |
| `mileage_km` | `Int64` | Odometer reading in km (parsed from `"89 271 km"`) |
| `power_kw` | `Float64` | Engine power in kilowatts (parsed from `"2.0 129kW"`) |
| `power_ps` | `Float64` | Engine power in metric horsepower ($\text{kW} \times 1.35962$, aligns with `powerPS`) |
| `fuel_type` | `string` | `petrol`, `diesel`, `electric`, `hybrid`, `plug_in_hybrid`, `cng`, `lpg`, `other` |
| `transmission` | `string` | `manual`, `automatic`, `semi_automatic` |
| `body_type` | `string` | `sedan`, `wagon`, `hatchback`, `suv`, `minivan`, `coupe`, `cabrio`, `van`, `pickup` |
| `drivetrain` | `string` | `fwd` (*esivedu*), `rwd` (*tagavedu*), `awd` (*nelikvedu*) |
| `damaged` | `boolean` | `True` if marked as accident-damaged (*avariiline*), else `False` |
| `scraped_at` | `string` | UTC ISO 8601 timestamp (`YYYY-MM-DDTHH:MM:SSZ`) when first scraped |
| `is_sold` | `boolean` | `True` once `--check-sold` detects `HTTP 410 Gone` (*"See sõiduk on juba müüdud!"*), else `False` |
| `sold_checked_at` | `string` | UTC ISO 8601 timestamp when active/sold status was last verified |
| `sold_at` | `string` | UTC ISO 8601 timestamp when the vehicle was first detected as sold |

---

## Running Unit Tests

```powershell
python -m unittest discover -v
```
