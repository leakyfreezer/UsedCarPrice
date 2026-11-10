#!/usr/bin/env python3
"""
Auto24.ee Polite Academic Web Scraper & Data Normalizer (v2)
============================================================

Extracts used vehicle specifications from Auto24.ee and maps them to a standardized
tabular schema compatible with the eBay Kleinanzeigen used cars dataset.

Key Features:
- Strict academic/ethical guardrails: randomized rate limiting (2.0-4.5s), zero PII extraction
  (no seller names, phone numbers, registration plates, or VINs).
- Multi-day incremental collection: remembers all previously collected `listing_id`s across
  runs via both the existing output dataset (CSV/Parquet) and a persistent state file
  (`.auto24_state.json`). Running with `--limit 1000` on Day 2 collects 1,000 *new* unseen
  listings and appends them to the dataset without duplicating old records.
- Two-Phase Sold-Status & Price-Drop Tracking (`--check-sold`):
  1. Phase 1 (Search-Page Sweep): Scans 50 listings per request on search pages to confirm
     which dataset vehicles are still active (`is_sold = False`) and updates `latest_price_eur`
     if a seller reduced their asking price — requiring zero detail-page requests for active cars.
  2. Phase 2 (Missing-ID Detail Check): Only requests `/soidukid/<id>` for vehicles from your
     dataset that were missing from the search sweep, confirming `HTTP 410 Gone` ("Sold").
- Resilient HTTP transport: uses `curl_cffi` (impersonating Chrome TLS fingerprints) with
  exponential backoff retries (`tenacity`) on HTTP 429/502/503 and network errors.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import random
import re
import sys
import time
from typing import Any, Iterable
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag
from curl_cffi import requests as cffi_requests
import pandas as pd
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("auto24_scraper")

# ---------------------------------------------------------------------------
# Constants, Categories & Decoupled DOM Selectors
# ---------------------------------------------------------------------------
BASE_URL = "https://www.auto24.ee"
SEARCH_PATH = "/kasutatud/nimekiri.php"
ITEMS_PER_PAGE = 50
KW_TO_PS_FACTOR = 1.35962

# Auto24 category parameter (`a=<code>`) mapping
CATEGORY_CODES: dict[str, int] = {
    "passenger": 100,      # Sõiduautod (Passenger cars)
    "suv": 109,            # Maasturid (SUVs)
    "commercial": 101,     # Kaubikud (Commercial vans)
    "all_cars": 100109,    # Sõiduautod + Maasturid combined
}


@dataclass(frozen=True)
class Selectors:
    """Decoupled CSS selectors for Auto24.ee search and detail pages."""

    # Search list page (`/kasutatud/nimekiri.php`)
    result_row: str = "div.result-row"
    main_link: str = "a.main"
    row_link: str = "a.row-link, a.small-image"
    model_span: str = "span.model"
    engine_span: str = "span.engine"
    price_span: str = ".finance .price, .pv .price"
    year_span: str = ".extra span.year, .tbl-columns span.year"
    mileage_span: str = ".extra span.mileage"
    fuel_span: str = ".extra span.fuel"
    fuel_short_span: str = ".extra span.fuel_short_icon, .tbl-columns span.fuel_short_icon"
    transmission_span: str = ".extra span.transmission"
    transmission_short_span: str = (
        ".extra span.transmission_short_icon, .tbl-columns span.transmission_short_icon"
    )
    bodytype_span: str = ".extra span.bodytype"
    drivetrain_span: str = ".extra span.drive"
    paginator_counter: str = ".paginator .page-cntr"
    next_page_link: str = ".paginator__next a[rel='next'], a[rel='next']"

    # Vehicle detail page (`/soidukid/<id>`)
    detail_table_rows: str = "table.main-data tr, .main-data tr"
    detail_label: str = "td.label, .label"
    detail_value: str = "td.field .value, td.value, .value, td.field"
    detail_other_info: str = ".other-info, .accident-info, .vehicle-condition"


SELECTORS = Selectors()

# ---------------------------------------------------------------------------
# Estonian -> Standardized English Translation Maps (eBay Kleinanzeigen aligned)
# ---------------------------------------------------------------------------
FUEL_MAP: list[tuple[str, str]] = [
    ("pistikhübriid", "plug_in_hybrid"),
    ("hübriid", "hybrid"),
    ("elekter", "electric"),
    ("bensiin", "petrol"),
    ("diisel", "diesel"),
    ("cng", "cng"),
    ("maagaas", "cng"),
    ("lpg", "lpg"),
    ("vedelgaas", "lpg"),
    ("gaas", "lpg"),
    ("vesinik", "hydrogen"),
    ("etanool", "other"),
]

FUEL_SHORT_MAP: dict[str, str] = {
    "b": "petrol",
    "d": "diesel",
    "e": "electric",
    "h": "hybrid",
    "h/b": "hybrid",
    "h/d": "hybrid",
    "ph": "plug_in_hybrid",
    "g": "cng",
}

TRANSMISSION_MAP: list[tuple[str, str]] = [
    ("poolautomaat", "semi_automatic"),
    ("automaat", "automatic"),
    ("manuaal", "manual"),
]

TRANSMISSION_SHORT_MAP: dict[str, str] = {
    "a": "automatic",
    "m": "manual",
    "p": "semi_automatic",
}

BODY_TYPE_MAP: list[tuple[str, str]] = [
    ("mahtuniversaal", "minivan"),
    ("universaal", "wagon"),
    ("luukpära", "hatchback"),
    ("sedaan", "sedan"),
    ("kabriolett", "cabrio"),
    ("kupee", "coupe"),
    ("maastur", "suv"),
    ("pikap", "pickup"),
    ("kaubik", "van"),
    ("väikebuss", "bus"),
    ("limusiin", "sedan"),
    ("targa", "cabrio"),
]

DRIVETRAIN_MAP: list[tuple[str, str]] = [
    ("nelikvedu", "awd"),
    ("esivedu", "fwd"),
    ("tagavedu", "rwd"),
]

# Schema column ordering and pandas nullable dtypes
SCHEMA_DTYPES: dict[str, str] = {
    "listing_id": "string",
    "make": "string",
    "model": "string",
    "year": "Int64",
    "month": "Int64",
    "price_eur": "Float64",
    "latest_price_eur": "Float64",
    "mileage_km": "Int64",
    "power_kw": "Float64",
    "power_ps": "Float64",
    "fuel_type": "string",
    "transmission": "string",
    "body_type": "string",
    "drivetrain": "string",
    "damaged": "boolean",
    "scraped_at": "string",
    "is_sold": "boolean",
    "sold_checked_at": "string",
    "sold_at": "string",
}


@dataclass
class VehicleRecord:
    """Normalized vehicle listing aligned with the eBay Kleinanzeigen dataset schema."""

    listing_id: str
    make: str | None = None
    model: str | None = None
    year: int | None = None
    month: int | None = None
    price_eur: float | None = None
    latest_price_eur: float | None = None
    mileage_km: int | None = None
    power_kw: float | None = None
    power_ps: float | None = None
    fuel_type: str | None = None
    transmission: str | None = None
    body_type: str | None = None
    drivetrain: str | None = None
    damaged: bool | None = None
    scraped_at: str = ""
    is_sold: bool | None = False
    sold_checked_at: str | None = None
    sold_at: str | None = None


# ---------------------------------------------------------------------------
# Pure Normalization & Parsing Functions
# ---------------------------------------------------------------------------
def clean_whitespace(text: str | None) -> str:
    """Collapse standard and non-breaking spaces into single spaces."""
    if not text:
        return ""
    return re.sub(r"[\s\xa0\u202f]+", " ", text).strip()


def extract_listing_id(href: str | None) -> str | None:
    """
    Extract unique numeric/alphanumeric listing ID from Auto24 URL paths.
    Examples:
      '/soidukid/4364079' -> '4364079'
      '/kasutatud/auto.php?id=4364079' -> '4364079'
    """
    if not href:
        return None
    match = re.search(r"(?:/soidukid/|id=)(\d+)", href)
    return match.group(1) if match else None


def parse_price_eur(raw_price: str | None) -> float | None:
    """
    Parse Estonian price strings into float EUR.
    Examples:
      '27 800 €' -> 27800.0
      '12\xa0500\xa0EUR (sisaldab KM 22%)' -> 12500.0
    """
    cleaned = clean_whitespace(raw_price)
    if not cleaned:
        return None
    compact = re.sub(r"(?<=\d)\s+(?=\d)", "", cleaned)
    match = re.search(r"(\d+(?:[.,]\d+)?)", compact)
    if not match:
        return None
    val = float(match.group(1).replace(",", "."))
    return val if val > 0 else None


def parse_mileage_km(raw_mileage: str | None) -> int | None:
    """
    Parse Estonian odometer strings into integer kilometers.
    Examples:
      '89 271 km' -> 89271
      '180\xa0000\xa0km' -> 180000
    """
    cleaned = clean_whitespace(raw_mileage)
    if not cleaned:
        return None
    compact = re.sub(r"(?<=\d)\s+(?=\d)", "", cleaned)
    match = re.search(r"(\d+)", compact)
    return int(match.group(1)) if match else None


def parse_registration_date(raw_date: str | None) -> tuple[int | None, int | None]:
    """
    Parse initial registration string into (year, month).
    Examples:
      '06/2019' -> (2019, 6)
      '2019'    -> (2019, None)
    """
    cleaned = clean_whitespace(raw_date)
    if not cleaned:
        return None, None
    mm_yyyy = re.search(r"\b(0?[1-9]|1[0-2])\s*/\s*((?:19|20)\d{2})\b", cleaned)
    if mm_yyyy:
        return int(mm_yyyy.group(2)), int(mm_yyyy.group(1))
    yyyy = re.search(r"\b((?:19|20)\d{2})\b", cleaned)
    if yyyy:
        return int(yyyy.group(1)), None
    return None, None


def parse_power_kw_and_ps(raw_engine: str | None) -> tuple[float | None, float | None]:
    """
    Extract engine power in kW and compute metric horsepower (PS).
    Examples:
      '2.0 129kW' -> (129.0, 175.4)
      '3.0 R6 (210 kW)' -> (210.0, 285.5)
    """
    cleaned = clean_whitespace(raw_engine)
    if not cleaned:
        return None, None
    match = re.search(r"(\d+(?:[.,]\d+)?)\s*kW\b", cleaned, flags=re.IGNORECASE)
    if not match:
        return None, None
    kw = float(match.group(1).replace(",", "."))
    if kw <= 0:
        return None, None
    ps = round(kw * KW_TO_PS_FACTOR, 1)
    return kw, ps


def normalize_fuel_type(raw_fuel: str | None, raw_short: str | None = None) -> str | None:
    """Translate Estonian fuel description into standardized English category."""
    cleaned = clean_whitespace(raw_fuel).lower()
    if cleaned:
        for est_term, eng_term in FUEL_MAP:
            if est_term in cleaned:
                return eng_term
        return "other"
    short_clean = clean_whitespace(raw_short).lower()
    if short_clean and short_clean in FUEL_SHORT_MAP:
        return FUEL_SHORT_MAP[short_clean]
    return None


def normalize_transmission(
    raw_transmission: str | None, raw_short: str | None = None
) -> str | None:
    """Translate Estonian gearbox description into standardized English category."""
    cleaned = clean_whitespace(raw_transmission).lower()
    if cleaned:
        for est_term, eng_term in TRANSMISSION_MAP:
            if est_term in cleaned:
                return eng_term
    short_clean = clean_whitespace(raw_short).lower()
    if short_clean and short_clean in TRANSMISSION_SHORT_MAP:
        return TRANSMISSION_SHORT_MAP[short_clean]
    return None


def normalize_body_type(raw_body: str | None) -> str | None:
    """Translate Estonian body type into standardized English category."""
    cleaned = clean_whitespace(raw_body).lower()
    if not cleaned:
        return None
    for est_term, eng_term in BODY_TYPE_MAP:
        if est_term in cleaned:
            return eng_term
    return "other"


def normalize_drivetrain(raw_drive: str | None) -> str | None:
    """Translate Estonian drivetrain term into fwd / rwd / awd."""
    cleaned = clean_whitespace(raw_drive).lower()
    if not cleaned:
        return None
    for est_term, eng_term in DRIVETRAIN_MAP:
        if est_term in cleaned:
            return eng_term
    return None


def normalize_make(raw_make: str | None) -> str | None:
    """Normalize vehicle make while preserving uppercase acronyms (BMW, VW, BYD, MAN)."""
    cleaned = clean_whitespace(raw_make)
    if not cleaned:
        return None
    upper_acronyms = {"BMW", "VW", "BYD", "GMC", "MG", "DS", "MAN", "GAZ", "UAZ", "VAZ", "ZAZ"}
    if cleaned.upper() in upper_acronyms:
        return cleaned.upper()
    return (
        "-".join(word.capitalize() for word in cleaned.split("-"))
        if "-" in cleaned
        else cleaned.title()
    )


def detect_sold_status(status_code: int, html: str) -> bool:
    """
    Determine whether an Auto24 listing detail response indicates a sold/inactive vehicle.
    Auto24 returns HTTP 410 Gone or 404 Not Found (with 'See sõiduk on juba müüdud!')
    once a listing is sold or removed.
    """
    if status_code in {404, 410}:
        return True
    lower_html = html.lower()
    sold_markers = (
        "see sõiduk on juba müüdud",
        "kuulutus ei ole aktiivne",
        "kuulutust ei leitud",
    )
    return any(marker in lower_html for marker in sold_markers)


# ---------------------------------------------------------------------------
# HTML Parsing (Search Result Rows & Detail Pages)
# ---------------------------------------------------------------------------
def parse_search_row(row: Tag, scraped_at: str | None = None) -> VehicleRecord | None:
    """
    Parse a single `<div class="result-row">` element from an Auto24 search page.
    Returns None if no valid `listing_id` is found.
    """
    main_anchor = row.select_one(SELECTORS.main_link)
    fallback_anchor = row.select_one(SELECTORS.row_link)
    href = (
        main_anchor.get("href")
        if main_anchor and main_anchor.get("href")
        else (fallback_anchor.get("href") if fallback_anchor else None)
    )
    listing_id = extract_listing_id(str(href) if href else None)
    if not listing_id:
        return None

    now_iso = scraped_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    make_raw: str | None = None
    model_raw: str | None = None
    engine_raw: str | None = None

    if main_anchor:
        model_el = main_anchor.select_one(SELECTORS.model_span)
        engine_el = main_anchor.select_one(SELECTORS.engine_span)
        model_raw = clean_whitespace(model_el.get_text()) if model_el else None
        engine_raw = clean_whitespace(engine_el.get_text()) if engine_el else None

        for span in main_anchor.find_all("span", recursive=False):
            classes = set(span.get("class") or [])
            if not classes.intersection({"model", "engine"}):
                make_raw = clean_whitespace(span.get_text())
                break

    year_el = row.select_one(SELECTORS.year_span)
    mileage_el = row.select_one(SELECTORS.mileage_span)
    price_el = row.select_one(SELECTORS.price_span)
    fuel_el = row.select_one(SELECTORS.fuel_span)
    fuel_short_el = row.select_one(SELECTORS.fuel_short_span)
    trans_el = row.select_one(SELECTORS.transmission_span)
    trans_short_el = row.select_one(SELECTORS.transmission_short_span)
    body_el = row.select_one(SELECTORS.bodytype_span)
    drive_el = row.select_one(SELECTORS.drivetrain_span)

    year, month = parse_registration_date(year_el.get_text() if year_el else None)
    power_kw, power_ps = parse_power_kw_and_ps(engine_raw)
    parsed_price = parse_price_eur(price_el.get_text() if price_el else None)

    return VehicleRecord(
        listing_id=listing_id,
        make=normalize_make(make_raw),
        model=model_raw or None,
        year=year,
        month=month,
        price_eur=parsed_price,
        latest_price_eur=parsed_price,
        mileage_km=parse_mileage_km(mileage_el.get_text() if mileage_el else None),
        power_kw=power_kw,
        power_ps=power_ps,
        fuel_type=normalize_fuel_type(
            fuel_el.get_text() if fuel_el else None,
            fuel_short_el.get_text() if fuel_short_el else None,
        ),
        transmission=normalize_transmission(
            trans_el.get_text() if trans_el else None,
            trans_short_el.get_text() if trans_short_el else None,
        ),
        body_type=normalize_body_type(body_el.get_text() if body_el else None),
        drivetrain=normalize_drivetrain(drive_el.get_text() if drive_el else None),
        damaged=False,
        scraped_at=now_iso,
        is_sold=False,
        sold_checked_at=now_iso,
        sold_at=None,
    )


def parse_search_page(html: str) -> tuple[list[VehicleRecord], int | None]:
    """
    Parse all vehicle rows and total page count from an Auto24 search page HTML.
    Returns `(records, max_pages)`.
    """
    soup = BeautifulSoup(html, "html.parser")
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    records: list[VehicleRecord] = []

    for row in soup.select(SELECTORS.result_row):
        record = parse_search_row(row, scraped_at=now_iso)
        if record:
            records.append(record)

    max_pages: int | None = None
    cntr = soup.select_one(SELECTORS.paginator_counter)
    if cntr:
        match = re.search(r"\(\s*\d+\s*/\s*(\d+)\s*\)", cntr.get_text())
        if match:
            max_pages = int(match.group(1))

    return records, max_pages


def enrich_from_detail_html(record: VehicleRecord, html: str) -> VehicleRecord:
    """
    Enrich an existing `VehicleRecord` using the vehicle's individual detail page HTML.
    Strictly ignores PII fields (`Reg. number`, `VIN-kood`, seller details).
    """
    soup = BeautifulSoup(html, "html.parser")

    for tr in soup.select(SELECTORS.detail_table_rows):
        label_el = tr.select_one(SELECTORS.detail_label)
        value_el = tr.select_one(SELECTORS.detail_value)
        if not label_el or not value_el:
            continue

        label = clean_whitespace(label_el.get_text()).lower().rstrip(":")
        value = clean_whitespace(value_el.get_text())

        if "reg. number" in label or "vin" in label or "tehasetähis" in label:
            continue

        if "esmane reg" in label:
            year, month = parse_registration_date(value)
            if year:
                record.year = year
            if month:
                record.month = month
        elif "mootor" in label and record.power_kw is None:
            record.power_kw, record.power_ps = parse_power_kw_and_ps(value)
        elif "kütus" in label and record.fuel_type is None:
            record.fuel_type = normalize_fuel_type(value)
        elif "läbisõidumõõdiku" in label and record.mileage_km is None:
            record.mileage_km = parse_mileage_km(value)
        elif "vedav sild" in label and record.drivetrain is None:
            record.drivetrain = normalize_drivetrain(value)
        elif "käigukast" in label and record.transmission is None:
            record.transmission = normalize_transmission(value)
        elif "keretüüp" in label and record.body_type is None:
            record.body_type = normalize_body_type(value)
        elif ("soodushind" in label or "hind" in label) and (
            "soodushind" in label or record.price_eur is None
        ):
            parsed_price = parse_price_eur(value)
            if parsed_price is not None:
                record.price_eur = parsed_price
                record.latest_price_eur = parsed_price

    page_text_lower = soup.get_text(" ", strip=True).lower()
    if "avariiline" in page_text_lower or "avarii" in page_text_lower:
        record.damaged = True

    return record


# ---------------------------------------------------------------------------
# Persistent State & Incremental Dataset Manager
# ---------------------------------------------------------------------------
class IncrementalStorage:
    """
    Manages cross-session deduplication, incremental checkpointing, and dataset persistence.
    Ensures running the script across multiple days only collects unseen listings and appends
    them cleanly to the target CSV or Parquet dataset.
    """

    def __init__(self, output_path: Path, state_path: Path) -> None:
        self.output_path = output_path
        self.state_path = state_path
        self.seen_ids: set[str] = set()
        self.last_completed_page: int = 0
        self.total_runs: int = 0
        self._load_existing_state()

    def _load_existing_state(self) -> None:
        """Load previously scraped `listing_id`s from state JSON and existing output file."""
        if self.state_path.exists():
            try:
                data = json.loads(self.state_path.read_text(encoding="utf-8"))
                self.seen_ids.update(str(x) for x in data.get("seen_ids", []))
                self.last_completed_page = int(data.get("last_completed_page", 0))
                self.total_runs = int(data.get("total_runs", 0))
            except Exception as exc:
                logger.warning("Could not parse state file %s: %s", self.state_path, exc)

        if self.output_path.exists():
            try:
                existing_df = self._read_dataset(self.output_path)
                if "listing_id" in existing_df.columns:
                    ids_from_file = (
                        existing_df["listing_id"].dropna().astype(str).unique().tolist()
                    )
                    self.seen_ids.update(ids_from_file)
                logger.info(
                    "Loaded %d existing records from %s (total unique seen IDs: %d).",
                    len(existing_df),
                    self.output_path,
                    len(self.seen_ids),
                )
            except Exception as exc:
                logger.warning("Could not read existing output file %s: %s", self.output_path, exc)

    @staticmethod
    def _read_dataset(path: Path) -> pd.DataFrame:
        suffix = path.suffix.lower()
        if suffix == ".parquet":
            return pd.read_parquet(path)
        return pd.read_csv(path, dtype={"listing_id": "string"})

    @classmethod
    def ensure_schema(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure all schema columns exist with their proper nullable pandas dtypes."""
        for col, dtype in SCHEMA_DTYPES.items():
            if col not in df.columns:
                if col == "is_sold":
                    df[col] = pd.Series([False] * len(df), dtype=dtype)
                elif col == "latest_price_eur" and "price_eur" in df.columns:
                    df[col] = df["price_eur"].astype(dtype)
                else:
                    df[col] = pd.Series([pd.NA] * len(df), dtype=dtype)
            else:
                df[col] = df[col].astype(dtype)
        # Backfill any missing latest_price_eur from initial price_eur
        if "latest_price_eur" in df.columns and "price_eur" in df.columns:
            df["latest_price_eur"] = df["latest_price_eur"].fillna(df["price_eur"])
        return df[list(SCHEMA_DTYPES.keys())]

    @classmethod
    def records_to_dataframe(cls, records: Iterable[VehicleRecord]) -> pd.DataFrame:
        rows = [asdict(r) for r in records]
        df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=list(SCHEMA_DTYPES.keys()))
        return cls.ensure_schema(df)

    def save_dataframe(self, df: pd.DataFrame) -> None:
        """Persist a standardized DataFrame to CSV or Parquet."""
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        normalized_df = self.ensure_schema(df)
        if self.output_path.suffix.lower() == ".parquet":
            normalized_df.to_parquet(self.output_path, index=False)
        else:
            normalized_df.to_csv(self.output_path, index=False, encoding="utf-8")

    def flush_checkpoint(
        self, new_records: list[VehicleRecord], current_page: int, run_finished: bool = False
    ) -> int:
        """
        Append `new_records` to the output CSV/Parquet file (deduplicating by `listing_id`)
        and persist the updated state file. Returns total row count in the dataset.
        """
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)

        new_df = self.records_to_dataframe(new_records)
        if self.output_path.exists() and self.output_path.stat().st_size > 0:
            existing_df = self.records_to_dataframe([])
            try:
                raw_existing = self._read_dataset(self.output_path)
                existing_df = self.ensure_schema(raw_existing)
            except Exception as exc:
                logger.warning("Error reading existing output during flush: %s", exc)

            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=["listing_id"], keep="last")
        else:
            combined_df = new_df.drop_duplicates(subset=["listing_id"], keep="last")

        self.save_dataframe(combined_df)

        self.seen_ids.update(r.listing_id for r in new_records)
        self.last_completed_page = current_page
        if run_finished:
            self.total_runs += 1

        state_payload = {
            "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "output_file": str(self.output_path),
            "total_seen_ids": len(self.seen_ids),
            "last_completed_page": self.last_completed_page,
            "total_runs": self.total_runs,
            "seen_ids": sorted(self.seen_ids),
        }
        tmp_state = self.state_path.with_suffix(".tmp")
        tmp_state.write_text(json.dumps(state_payload, indent=2), encoding="utf-8")
        tmp_state.replace(self.state_path)

        return len(combined_df)


# ---------------------------------------------------------------------------
# Polite HTTP Scraper Client
# ---------------------------------------------------------------------------
class TransientHTTPError(Exception):
    """Raised on HTTP 429, 502, 503, 504 to trigger exponential backoff."""


class Auto24Client:
    """Polite HTTP client impersonating Chrome TLS fingerprint with jittered rate limiting."""

    def __init__(self, min_delay: float = 2.0, max_delay: float = 4.0) -> None:
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.session = cffi_requests.Session(impersonate="chrome")
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "et-EE,et;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://www.auto24.ee/",
        }
        self._request_count = 0

    def polite_sleep(self) -> None:
        """Sleep for a randomized duration between `min_delay` and `max_delay` seconds."""
        if self._request_count > 0 and self.max_delay > 0:
            delay = random.uniform(self.min_delay, self.max_delay)
            logger.debug("Polite rate limit sleep: %.2fs", delay)
            time.sleep(delay)

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_random_exponential(multiplier=2.0, min=3.0, max=45.0),
        retry=retry_if_exception_type((TransientHTTPError, cffi_requests.RequestsError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def get_html(self, url: str, params: dict[str, Any] | None = None) -> str:
        """Fetch HTML with polite delay and automatic exponential backoff on 429/5xx."""
        self.polite_sleep()
        self._request_count += 1
        response = self.session.get(url, params=params, headers=self.headers, timeout=20)
        if response.status_code in {429, 500, 502, 503, 504}:
            raise TransientHTTPError(f"HTTP {response.status_code} for {response.url}")
        response.raise_for_status()
        return response.text

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_random_exponential(multiplier=2.0, min=3.0, max=45.0),
        retry=retry_if_exception_type((TransientHTTPError, cffi_requests.RequestsError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def get_status_and_html(self, url: str) -> tuple[int, str]:
        """
        Fetch HTTP status code and HTML body without raising on HTTP 404 or 410 (Gone).
        Used by `--check-sold` to detect sold/removed listings.
        """
        self.polite_sleep()
        self._request_count += 1
        response = self.session.get(url, headers=self.headers, timeout=20)
        if response.status_code in {429, 500, 502, 503, 504}:
            raise TransientHTTPError(f"HTTP {response.status_code} for {response.url}")
        return response.status_code, response.text


# ---------------------------------------------------------------------------
# Two-Phase Sold-Status Verification Orchestrator (`--check-sold`)
# ---------------------------------------------------------------------------
def reconcile_active_from_sweep(
    df: pd.DataFrame,
    live_prices: dict[str, float | None],
    checked_at_iso: str,
) -> tuple[int, int, list[int]]:
    """
    Pure helper for Phase 1 of `--check-sold`:
    - Marks any active row in `df` whose `listing_id` is in `live_prices` as still active
      (`is_sold = False`, `sold_checked_at = checked_at_iso`) and updates `latest_price_eur`.
    - Returns `(confirmed_active_count, price_changed_count, missing_candidate_indices)`.
    """
    active_mask = df["is_sold"].isna() | (df["is_sold"] == False)  # noqa: E712
    active_indices = df.index[active_mask].tolist()

    confirmed_active = 0
    price_changed = 0
    missing_indices: list[int] = []

    for idx in active_indices:
        lid = str(df.at[idx, "listing_id"])
        if lid in live_prices:
            df.at[idx, "is_sold"] = False
            df.at[idx, "sold_checked_at"] = checked_at_iso
            new_price = live_prices[lid]
            if new_price is not None:
                old_latest = df.at[idx, "latest_price_eur"]
                if pd.notna(old_latest) and float(old_latest) != float(new_price):
                    price_changed += 1
                df.at[idx, "latest_price_eur"] = new_price
            confirmed_active += 1
        else:
            missing_indices.append(idx)

    return confirmed_active, price_changed, missing_indices


def run_check_sold(
    output_path: Path,
    state_path: Path,
    category: str = "passenger",
    sweep_pages: int = 60,
    start_sweep_page: int = 1,
    limit: int | None = 200,
    checkpoint_every: int = 50,
    min_delay: float = 2.0,
    max_delay: float = 4.0,
) -> tuple[int, int, int]:
    """
    Two-Phase Sold-Status & Price-Drop Verification:
    - Phase 1 (Search Sweep): Scans `sweep_pages` of search results (50 listings/page).
      Every dataset `listing_id` seen on those search pages is immediately confirmed
      active (`is_sold = False`) and has its `latest_price_eur` updated at zero detail-page cost.
    - Phase 2 (Missing-ID Detail Check): Only the remaining active rows from `output_path`
      that were NOT seen in Phase 1 (up to `limit` detail checks) are requested individually
      via `/soidukid/<id>` to confirm whether they return `HTTP 410 Gone` ("Sold").
    Returns `(confirmed_active_in_sweep, detail_checked_count, newly_sold_count)`.
    """
    if not output_path.exists():
        logger.error("Dataset file %s does not exist. Scrape listings first.", output_path)
        return 0, 0, 0

    storage = IncrementalStorage(output_path=output_path, state_path=state_path)
    df = storage.ensure_schema(storage._read_dataset(output_path))
    client = Auto24Client(min_delay=min_delay, max_delay=max_delay)
    category_code = CATEGORY_CODES.get(category.lower(), 100)

    # -----------------------------------------------------------------------
    # Phase 1: Bulk Search-Page Sweep (50 cars per HTTP request)
    # -----------------------------------------------------------------------
    live_prices: dict[str, float | None] = {}
    if sweep_pages > 0:
        logger.info(
            "Phase 1 (--check-sold): Sweeping %d search pages (from page %d) to bulk-confirm active cars...",
            sweep_pages,
            start_sweep_page,
        )
        search_url = urljoin(BASE_URL, SEARCH_PATH)
        for p_idx in range(start_sweep_page, start_sweep_page + sweep_pages):
            offset = (p_idx - 1) * ITEMS_PER_PAGE
            params = {"bn": 2, "a": category_code, "ak": offset}
            html = client.get_html(search_url, params=params)
            records, max_pages = parse_search_page(html)
            if not records:
                break
            for rec in records:
                live_prices[rec.listing_id] = rec.price_eur
            if max_pages is not None and p_idx >= max_pages:
                break

    sweep_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    confirmed_active, price_changed, missing_indices = reconcile_active_from_sweep(
        df, live_prices, checked_at_iso=sweep_iso
    )
    storage.save_dataframe(df)
    logger.info(
        "Phase 1 complete | Search IDs seen: %d | Dataset rows confirmed active (0 detail requests): %d | "
        "Price changes detected: %d | Remaining missing candidates: %d",
        len(live_prices),
        confirmed_active,
        price_changed,
        len(missing_indices),
    )

    # -----------------------------------------------------------------------
    # Phase 2: Targeted Detail-Page Check Only on Missing Candidates
    # -----------------------------------------------------------------------
    candidates_to_verify = (
        missing_indices[:limit] if (limit is not None and limit >= 0) else missing_indices
    )
    logger.info(
        "Phase 2 (--check-sold): Verifying %d missing candidates via detail pages (capped by --limit %s)...",
        len(candidates_to_verify),
        limit,
    )

    detail_checked = 0
    newly_sold = 0

    try:
        for idx in candidates_to_verify:
            listing_id = str(df.at[idx, "listing_id"])
            detail_url = urljoin(BASE_URL, f"/soidukid/{listing_id}")

            status_code, html = client.get_status_and_html(detail_url)
            now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            sold = detect_sold_status(status_code, html)

            df.at[idx, "sold_checked_at"] = now_iso
            df.at[idx, "is_sold"] = sold
            if sold:
                if pd.isna(df.at[idx, "sold_at"]) or not str(df.at[idx, "sold_at"]).strip():
                    df.at[idx, "sold_at"] = now_iso
                newly_sold += 1

            detail_checked += 1

            if detail_checked % checkpoint_every == 0:
                storage.save_dataframe(df)
                logger.info(
                    "Phase 2 checkpoint saved | Detail-checked: %d/%d | Newly detected sold: %d",
                    detail_checked,
                    len(candidates_to_verify),
                    newly_sold,
                )

    except KeyboardInterrupt:
        logger.warning("Interrupted by user (Ctrl+C). Saving --check-sold progress...")
    finally:
        storage.save_dataframe(df)
        logger.info(
            "Finished --check-sold | Confirmed active via sweep: %d | Detail-checked: %d | "
            "Newly sold: %d | Total sold in dataset: %d/%d",
            confirmed_active,
            detail_checked,
            newly_sold,
            int((df["is_sold"] == True).sum()),  # noqa: E712
            len(df),
        )

    return confirmed_active, detail_checked, newly_sold


# ---------------------------------------------------------------------------
# Main Scraping Orchestrator
# ---------------------------------------------------------------------------
def run_scraper(
    category: str,
    limit: int,
    output_path: Path,
    state_path: Path,
    start_page: int = 1,
    resume_page: bool = False,
    fetch_details: bool = False,
    checkpoint_every: int = 50,
    min_delay: float = 2.0,
    max_delay: float = 4.0,
) -> int:
    """
    Collect up to `limit` NEW (previously unseen) vehicle listings from Auto24.ee.
    Returns the number of new listings collected in this session.
    """
    storage = IncrementalStorage(output_path=output_path, state_path=state_path)
    client = Auto24Client(min_delay=min_delay, max_delay=max_delay)

    category_code = CATEGORY_CODES.get(category.lower(), 100)
    page = (
        max(1, storage.last_completed_page + 1)
        if (resume_page and storage.last_completed_page > 0)
        else max(1, start_page)
    )

    logger.info(
        "Starting Auto24 scrape | Category: %s (a=%d) | Target NEW listings: %d | "
        "Already known IDs: %d | Starting page: %d",
        category,
        category_code,
        limit,
        len(storage.seen_ids),
        page,
    )

    session_collected = 0
    buffer: list[VehicleRecord] = []
    max_pages: int | None = None

    try:
        while session_collected < limit:
            offset = (page - 1) * ITEMS_PER_PAGE
            params = {"bn": 2, "a": category_code, "ak": offset}
            search_url = urljoin(BASE_URL, SEARCH_PATH)

            logger.info("Fetching search page %d (offset ak=%d)...", page, offset)
            html = client.get_html(search_url, params=params)
            page_records, detected_max_pages = parse_search_page(html)

            if detected_max_pages is not None:
                max_pages = detected_max_pages

            if not page_records:
                logger.info("No listings found on page %d. Stopping pagination.", page)
                break

            new_on_page = 0
            skipped_on_page = 0

            for record in page_records:
                if record.listing_id in storage.seen_ids:
                    skipped_on_page += 1
                    continue

                if fetch_details:
                    detail_url = urljoin(BASE_URL, f"/soidukid/{record.listing_id}")
                    try:
                        detail_html = client.get_html(detail_url)
                        record = enrich_from_detail_html(record, detail_html)
                    except Exception as exc:
                        logger.warning(
                            "Could not enrich detail page for ID %s: %s",
                            record.listing_id,
                            exc,
                        )

                storage.seen_ids.add(record.listing_id)
                buffer.append(record)
                session_collected += 1
                new_on_page += 1

                if len(buffer) >= checkpoint_every:
                    total_in_file = storage.flush_checkpoint(buffer, current_page=page)
                    logger.info(
                        "Checkpoint saved: +%d new records flushed | Session progress: %d/%d | "
                        "Total dataset size: %d",
                        len(buffer),
                        session_collected,
                        limit,
                        total_in_file,
                    )
                    buffer.clear()

                if session_collected >= limit:
                    break

            logger.info(
                "Page %d summary: %d new collected, %d already-seen skipped (Session: %d/%d)",
                page,
                new_on_page,
                skipped_on_page,
                session_collected,
                limit,
            )

            if max_pages is not None and page >= max_pages:
                logger.info("Reached final search page (%d/%d).", page, max_pages)
                break

            page += 1

    except KeyboardInterrupt:
        logger.warning("Interrupted by user (Ctrl+C). Flushing remaining buffer before exit...")
    finally:
        if buffer or session_collected > 0:
            total_in_file = storage.flush_checkpoint(
                buffer, current_page=page, run_finished=True
            )
            buffer.clear()
            logger.info(
                "Run finished. Collected %d new listings this session. Total dataset size: %d rows (%s).",
                session_collected,
                total_in_file,
                output_path,
            )

    return session_collected


# ---------------------------------------------------------------------------
# CLI Entrypoint
# ---------------------------------------------------------------------------
def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Polite academic web scraper for Auto24.ee with multi-day incremental "
            "collection, two-phase sold-status tracking, and eBay Kleinanzeigen schema normalization."
        )
    )
    parser.add_argument(
        "--category",
        type=str,
        default="passenger",
        choices=list(CATEGORY_CODES.keys()),
        help="Vehicle category to scrape (default: passenger).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help=(
            "Number of NEW (previously unseen) listings to collect in this session, "
            "or max missing candidates to verify via detail pages when --check-sold is used (default: 1000)."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/auto24_vehicles.csv"),
        help="Output dataset path (.csv or .parquet). Existing files are appended to, not overwritten.",
    )
    parser.add_argument(
        "--state-file",
        type=Path,
        default=Path("data/.auto24_state.json"),
        help="Path to persistent JSON state file tracking seen listing IDs and page bookmarks.",
    )
    parser.add_argument(
        "--start-page",
        type=int,
        default=1,
        help="Search result page number to start from (default: 1).",
    )
    parser.add_argument(
        "--resume-page",
        action="store_true",
        help="Start directly from the page after the last completed page recorded in --state-file.",
    )
    parser.add_argument(
        "--fetch-details",
        action="store_true",
        help=(
            "Fetch each unseen vehicle's individual detail page for registration month "
            "and damage status (slower: 1 extra request per vehicle)."
        ),
    )
    parser.add_argument(
        "--check-sold",
        action="store_true",
        help=(
            "Two-phase sold check: first sweeps search pages (50 cars/page) to confirm still-active "
            "listings and price drops, then checks detail pages only for missing IDs."
        ),
    )
    parser.add_argument(
        "--sweep-pages",
        type=int,
        default=60,
        help="Number of search pages (50 cars/page) to sweep in Phase 1 of --check-sold (default: 60).",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=50,
        help="Flush newly collected or checked records to disk every N listings (default: 50).",
    )
    parser.add_argument(
        "--min-delay",
        type=float,
        default=2.0,
        help="Minimum polite delay in seconds between HTTP requests (default: 2.0).",
    )
    parser.add_argument(
        "--max-delay",
        type=float,
        default=4.0,
        help="Maximum polite delay in seconds between HTTP requests (default: 4.0).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.output.suffix.lower() not in {".csv", ".parquet"}:
        parser.error("--output must end with '.csv' or '.parquet'")

    if args.min_delay < 0 or args.max_delay < args.min_delay:
        parser.error("--max-delay must be >= --min-delay and non-negative")

    if args.check_sold:
        run_check_sold(
            output_path=args.output,
            state_path=args.state_file,
            category=args.category,
            sweep_pages=args.sweep_pages,
            start_sweep_page=args.start_page,
            limit=args.limit,
            checkpoint_every=args.checkpoint_every,
            min_delay=args.min_delay,
            max_delay=args.max_delay,
        )
        return 0

    run_scraper(
        category=args.category,
        limit=args.limit,
        output_path=args.output,
        state_path=args.state_file,
        start_page=args.start_page,
        resume_page=args.resume_page,
        fetch_details=args.fetch_details,
        checkpoint_every=args.checkpoint_every,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
