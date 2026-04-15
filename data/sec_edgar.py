"""
SEC EDGAR 13F Filing Data Fetcher
Pulls real 13F-HR filings directly from the SEC EDGAR API.
No API key required — just a User-Agent header per SEC policy.

Data is cached locally to avoid repeated API calls.
"""

import os
import json
import time
import hashlib
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
import pandas as pd

logger = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────

SEC_BASE_URL = "https://data.sec.gov"
SEC_EDGAR_URL = "https://www.sec.gov"
EFTS_URL = "https://efts.sec.gov/LATEST"

USER_AGENT = "HedgeFlow Research App contact@hedgeflow.app"

CACHE_DIR = Path(__file__).parent.parent / ".cache" / "sec_edgar"
CACHE_TTL_HOURS = 12  # Cache data for 12 hours

# SEC rate limit: 10 requests/second — we use 5 to be safe
REQUEST_DELAY = 0.22

# 13F XML namespace
NS_13F = "http://www.sec.gov/edgar/document/thirteenf/informationtable"

# ─── Known Hedge Fund CIKs ───────────────────────────────────────────────────
# These are the Central Index Keys (CIK) used by the SEC to identify filers.

TRACKED_FUNDS = {
    "Berkshire Hathaway": {
        "cik": "0001067983",
        "manager": "Warren Buffett",
        "strategy": "Value Investing",
        "location": "Omaha, NE",
        "founded": 1965,
    },
    "Bridgewater Associates": {
        "cik": "0001350694",
        "manager": "Ray Dalio",
        "strategy": "Global Macro",
        "location": "Westport, CT",
        "founded": 1975,
    },
    "Citadel Advisors": {
        "cik": "0001423053",
        "manager": "Ken Griffin",
        "strategy": "Multi-Strategy",
        "location": "Miami, FL",
        "founded": 1990,
    },
    "Renaissance Technologies": {
        "cik": "0001037389",
        "manager": "Jim Simons",
        "strategy": "Quantitative",
        "location": "East Setauket, NY",
        "founded": 1982,
    },
    "Two Sigma Investments": {
        "cik": "0001179392",
        "manager": "David Siegel",
        "strategy": "Quantitative",
        "location": "New York, NY",
        "founded": 2001,
    },
    "D.E. Shaw": {
        "cik": "0001009207",
        "manager": "David Shaw",
        "strategy": "Quantitative",
        "location": "New York, NY",
        "founded": 1988,
    },
    "Millennium Management": {
        "cik": "0001273087",
        "manager": "Israel Englander",
        "strategy": "Multi-Strategy",
        "location": "New York, NY",
        "founded": 1989,
    },
    "AQR Capital Management": {
        "cik": "0001167557",
        "manager": "Cliff Asness",
        "strategy": "Quantitative",
        "location": "Greenwich, CT",
        "founded": 1998,
    },
    "Tiger Global Management": {
        "cik": "0001167483",
        "manager": "Chase Coleman",
        "strategy": "Growth Equity",
        "location": "New York, NY",
        "founded": 2001,
    },
    "Point72 Asset Management": {
        "cik": "0001603466",
        "manager": "Steve Cohen",
        "strategy": "Multi-Strategy",
        "location": "Stamford, CT",
        "founded": 2014,
    },
    "Pershing Square Capital": {
        "cik": "0001336528",
        "manager": "Bill Ackman",
        "strategy": "Activist",
        "location": "New York, NY",
        "founded": 2004,
    },
    "Soros Fund Management": {
        "cik": "0001029160",
        "manager": "George Soros",
        "strategy": "Global Macro",
        "location": "New York, NY",
        "founded": 1970,
    },
    "Appaloosa Management": {
        "cik": "0001656456",
        "manager": "David Tepper",
        "strategy": "Distressed Debt",
        "location": "Short Hills, NJ",
        "founded": 1993,
    },
    "Viking Global Investors": {
        "cik": "0001103804",
        "manager": "Andreas Halvorsen",
        "strategy": "Long/Short Equity",
        "location": "Greenwich, CT",
        "founded": 1999,
    },
    "Coatue Management": {
        "cik": "0001535392",
        "manager": "Philippe Laffont",
        "strategy": "Technology L/S",
        "location": "New York, NY",
        "founded": 1999,
    },
    "Lone Pine Capital": {
        "cik": "0001061768",
        "manager": "Stephen Mandel Jr.",
        "strategy": "Long/Short Equity",
        "location": "Greenwich, CT",
        "founded": 1997,
    },
    "Third Point LLC": {
        "cik": "0001040273",
        "manager": "Dan Loeb",
        "strategy": "Event Driven",
        "location": "New York, NY",
        "founded": 1995,
    },
    "Elliott Investment Management": {
        "cik": "0001048445",
        "manager": "Paul Singer",
        "strategy": "Activist",
        "location": "West Palm Beach, FL",
        "founded": 1977,
    },
    "Baupost Group": {
        "cik": "0001061219",
        "manager": "Seth Klarman",
        "strategy": "Value Investing",
        "location": "Boston, MA",
        "founded": 1982,
    },
    "Greenlight Capital": {
        "cik": "0001079114",
        "manager": "David Einhorn",
        "strategy": "Long/Short Equity",
        "location": "New York, NY",
        "founded": 1996,
    },
    "Icahn Capital": {
        "cik": "0000921669",
        "manager": "Carl Icahn",
        "strategy": "Activist",
        "location": "New York, NY",
        "founded": 1987,
    },
    "Duquesne Family Office": {
        "cik": "0001536411",
        "manager": "Stanley Druckenmiller",
        "strategy": "Global Macro",
        "location": "New York, NY",
        "founded": 2010,
    },
    "Dragoneer Investment Group": {
        "cik": "0001571174",
        "manager": "Marc Stad",
        "strategy": "Growth Equity",
        "location": "San Francisco, CA",
        "founded": 2012,
    },
    "Marshall Wace": {
        "cik": "0001544301",
        "manager": "Paul Marshall",
        "strategy": "Long/Short Equity",
        "location": "London, UK",
        "founded": 1997,
    },
    "Whale Rock Capital": {
        "cik": "0001513153",
        "manager": "Alex Sacerdote",
        "strategy": "Technology L/S",
        "location": "Boston, MA",
        "founded": 2006,
    },
}


# ─── HTTP Session ─────────────────────────────────────────────────────────────

def _get_session() -> requests.Session:
    """Create a requests session with proper SEC headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
        "Accept": "application/json, application/xml, text/xml, */*",
    })
    return session


_session: Optional[requests.Session] = None


def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = _get_session()
    return _session


def _rate_limited_get(url: str, accept: str = None) -> requests.Response:
    """Make a rate-limited GET request to the SEC."""
    session = get_session()
    if accept:
        headers = {"Accept": accept}
    else:
        headers = {}
    time.sleep(REQUEST_DELAY)
    resp = session.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp


# ─── Caching ──────────────────────────────────────────────────────────────────

def _cache_key(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()


def _get_cached(url: str) -> Optional[str]:
    """Get cached response for a URL."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / f"{_cache_key(url)}.json"
    if cache_file.exists():
        data = json.loads(cache_file.read_text(encoding="utf-8"))
        cached_at = datetime.fromisoformat(data["cached_at"])
        if datetime.now() - cached_at < timedelta(hours=CACHE_TTL_HOURS):
            return data["content"]
    return None


def _set_cached(url: str, content: str):
    """Cache response for a URL."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / f"{_cache_key(url)}.json"
    cache_file.write_text(json.dumps({
        "url": url,
        "cached_at": datetime.now().isoformat(),
        "content": content,
    }), encoding="utf-8")


def clear_cache():
    """Clear all cached data."""
    import shutil
    if CACHE_DIR.exists():
        shutil.rmtree(CACHE_DIR)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _fetch_with_cache(url: str, accept: str = None) -> str:
    """Fetch URL with caching."""
    cached = _get_cached(url)
    if cached is not None:
        return cached
    resp = _rate_limited_get(url, accept=accept)
    content = resp.text
    _set_cached(url, content)
    return content


# ─── SEC EDGAR API ────────────────────────────────────────────────────────────

def get_company_filings(cik: str) -> dict:
    """Get all filings for a company from the SEC EDGAR submissions API."""
    padded_cik = cik.lstrip("0").zfill(10)
    url = f"{SEC_BASE_URL}/submissions/CIK{padded_cik}.json"
    content = _fetch_with_cache(url, accept="application/json")
    return json.loads(content)


def find_13f_filings(cik: str, count: int = 4) -> list[dict]:
    """
    Find the most recent 13F-HR filings for a company.
    Returns filing metadata including accession numbers.
    """
    try:
        data = get_company_filings(cik)
    except Exception as e:
        logger.warning(f"Failed to fetch filings for CIK {cik}: {e}")
        return []

    company_name = data.get("name", "Unknown")
    recent = data.get("filings", {}).get("recent", {})

    if not recent:
        return []

    forms = recent.get("form", [])
    accession_numbers = recent.get("accessionNumber", [])
    filing_dates = recent.get("filingDate", [])
    report_dates = recent.get("reportDate", [])
    primary_docs = recent.get("primaryDocument", [])

    filings_13f = []
    for i, form in enumerate(forms):
        if form in ("13F-HR", "13F-HR/A") and i < len(accession_numbers):
            filings_13f.append({
                "company_name": company_name,
                "cik": cik,
                "form": form,
                "accession_number": accession_numbers[i],
                "filing_date": filing_dates[i] if i < len(filing_dates) else None,
                "report_date": report_dates[i] if i < len(report_dates) else None,
                "primary_document": primary_docs[i] if i < len(primary_docs) else None,
            })
            if len(filings_13f) >= count:
                break

    return filings_13f


def get_filing_index(cik: str, accession_number: str) -> dict:
    """Get the filing index (list of documents) for a specific filing."""
    padded_cik = cik.lstrip("0").zfill(10)
    acc_no_dashes = accession_number.replace("-", "")
    url = f"{SEC_BASE_URL}/Archives/edgar/data/{padded_cik}/{acc_no_dashes}/index.json"
    content = _fetch_with_cache(url, accept="application/json")
    return json.loads(content)


def find_infotable_url(cik: str, accession_number: str) -> Optional[str]:
    """Find the URL of the 13F information table XML within a filing."""
    try:
        index = get_filing_index(cik, accession_number)
    except Exception as e:
        logger.warning(f"Failed to get filing index for {accession_number}: {e}")
        return None

    padded_cik = cik.lstrip("0").zfill(10)
    acc_no_dashes = accession_number.replace("-", "")
    base_url = f"{SEC_BASE_URL}/Archives/edgar/data/{padded_cik}/{acc_no_dashes}"

    items = index.get("directory", {}).get("item", [])
    for item in items:
        name = item.get("name", "").lower()
        # Look for the infotable XML document
        if ("infotable" in name or "information_table" in name or
            "13f_combinedinformationtable" in name or
            "informationtable" in name) and name.endswith(".xml"):
            return f"{base_url}/{item['name']}"

    # Fallback: look for any XML that isn't the primary document
    for item in items:
        name = item.get("name", "").lower()
        if name.endswith(".xml") and "primary" not in name and "R" not in item.get("name", ""):
            return f"{base_url}/{item['name']}"

    return None


def parse_13f_xml(xml_content: str) -> pd.DataFrame:
    """
    Parse a 13F information table XML into a DataFrame.
    Handles multiple namespace patterns used in SEC filings.
    """
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        logger.error(f"XML parse error: {e}")
        return pd.DataFrame()

    # Try different namespace patterns
    namespaces = [
        {"ns": "http://www.sec.gov/edgar/document/thirteenf/informationtable"},
        {"ns": "http://www.sec.gov/edgar/13Fform"},
        {},
    ]

    holdings = []

    for ns in namespaces:
        prefix = f"{{{ns['ns']}}}" if ns else ""

        # Try to find info table entries
        entries = root.findall(f".//{prefix}infoTable")
        if not entries:
            entries = root.findall(f".//{prefix}InfoTable")
        if not entries:
            # Try with namespace mapping
            if ns:
                entries = root.findall(".//ns:infoTable", ns)
            if not entries and ns:
                entries = root.findall(".//ns:InfoTable", ns)

        if entries:
            for entry in entries:
                def _find(tag):
                    """Find text for a tag, trying multiple namespace strategies."""
                    # Try with prefix
                    el = entry.find(f"{prefix}{tag}")
                    if el is not None and el.text:
                        return el.text.strip()
                    # Try with namespace dict
                    if ns:
                        el = entry.find(f"ns:{tag}", ns)
                        if el is not None and el.text:
                            return el.text.strip()
                    # Try without namespace
                    el = entry.find(tag)
                    if el is not None and el.text:
                        return el.text.strip()
                    return None

                def _find_nested(parent_tag, child_tag):
                    """Find nested element text."""
                    parent = entry.find(f"{prefix}{parent_tag}")
                    if parent is None and ns:
                        parent = entry.find(f"ns:{parent_tag}", ns)
                    if parent is None:
                        parent = entry.find(parent_tag)
                    if parent is not None:
                        child = parent.find(f"{prefix}{child_tag}")
                        if child is None and ns:
                            child = parent.find(f"ns:{child_tag}", ns)
                        if child is None:
                            child = parent.find(child_tag)
                        if child is not None and child.text:
                            return child.text.strip()
                    return None

                name = _find("nameOfIssuer")
                cusip = _find("cusip")
                title = _find("titleOfClass")
                value = _find("value")  # in thousands
                shares = _find_nested("shrsOrPrnAmt", "sshPrnamt")
                share_type = _find_nested("shrsOrPrnAmt", "sshPrnamtType")
                discretion = _find("investmentDiscretion")
                voting_sole = _find_nested("votingAuthority", "Sole")
                voting_shared = _find_nested("votingAuthority", "Shared")
                voting_none = _find_nested("votingAuthority", "None")

                if name and value:
                    try:
                        value_thousands = int(value)
                        share_count = int(shares) if shares else 0
                    except (ValueError, TypeError):
                        value_thousands = 0
                        share_count = 0

                    holdings.append({
                        "name_of_issuer": name,
                        "title_of_class": title or "",
                        "cusip": cusip or "",
                        "value": value_thousands * 1000,  # Convert from thousands
                        "value_thousands": value_thousands,
                        "shares": share_count,
                        "share_type": share_type or "SH",
                        "investment_discretion": discretion or "",
                        "voting_sole": int(voting_sole) if voting_sole else 0,
                        "voting_shared": int(voting_shared) if voting_shared else 0,
                        "voting_none": int(voting_none) if voting_none else 0,
                    })

            if holdings:
                break

    if not holdings:
        # Last resort: try parsing without any namespace
        for entry in root.iter():
            if entry.tag.lower().endswith("infotable"):
                # Try to extract children directly
                row = {}
                for child in entry:
                    tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                    if child.text and child.text.strip():
                        row[tag] = child.text.strip()
                    for subchild in child:
                        subtag = subchild.tag.split("}")[-1] if "}" in subchild.tag else subchild.tag
                        if subchild.text and subchild.text.strip():
                            row[subtag] = subchild.text.strip()

                if "nameOfIssuer" in row:
                    try:
                        val = int(row.get("value", 0))
                        shr = int(row.get("sshPrnamt", 0))
                    except (ValueError, TypeError):
                        val = 0
                        shr = 0

                    holdings.append({
                        "name_of_issuer": row.get("nameOfIssuer", ""),
                        "title_of_class": row.get("titleOfClass", ""),
                        "cusip": row.get("cusip", ""),
                        "value": val * 1000,
                        "value_thousands": val,
                        "shares": shr,
                        "share_type": row.get("sshPrnamtType", "SH"),
                        "investment_discretion": row.get("investmentDiscretion", ""),
                        "voting_sole": int(row.get("Sole", 0)),
                        "voting_shared": int(row.get("Shared", 0)),
                        "voting_none": int(row.get("None", 0)),
                    })

    df = pd.DataFrame(holdings)
    if not df.empty:
        # Aggregate duplicate CUSIPs (some filings split across managers)
        df = df.groupby(["name_of_issuer", "cusip", "title_of_class"], as_index=False).agg({
            "value": "sum",
            "value_thousands": "sum",
            "shares": "sum",
            "share_type": "first",
            "investment_discretion": "first",
            "voting_sole": "sum",
            "voting_shared": "sum",
            "voting_none": "sum",
        })
        df = df.sort_values("value", ascending=False).reset_index(drop=True)

    return df


def fetch_13f_holdings(cik: str, filing_index: int = 0) -> tuple[pd.DataFrame, dict]:
    """
    Fetch and parse 13F holdings for a given CIK.
    filing_index: 0 = most recent, 1 = previous quarter, etc.
    Returns (holdings_df, filing_metadata).
    """
    filings = find_13f_filings(cik, count=filing_index + 2)
    if not filings or filing_index >= len(filings):
        return pd.DataFrame(), {}

    filing = filings[filing_index]
    accession = filing["accession_number"]

    infotable_url = find_infotable_url(cik, accession)
    if not infotable_url:
        logger.warning(f"Could not find infotable for {accession}")
        return pd.DataFrame(), filing

    try:
        xml_content = _fetch_with_cache(infotable_url, accept="application/xml")
    except Exception as e:
        logger.warning(f"Failed to fetch infotable XML: {e}")
        return pd.DataFrame(), filing

    df = parse_13f_xml(xml_content)
    return df, filing


# ─── High-Level Data Functions ────────────────────────────────────────────────

def get_fund_holdings_real(fund_name: str, progress_callback=None) -> tuple[pd.DataFrame, dict]:
    """
    Get current holdings for a tracked fund.
    Returns (holdings_df, metadata_dict).
    """
    fund_info = TRACKED_FUNDS.get(fund_name)
    if not fund_info:
        return pd.DataFrame(), {}

    cik = fund_info["cik"]
    current_df, current_filing = fetch_13f_holdings(cik, 0)
    prev_df, prev_filing = fetch_13f_holdings(cik, 1)

    if current_df.empty:
        return pd.DataFrame(), {}

    # Calculate total portfolio value
    total_value = current_df["value"].sum()

    # Add portfolio percentage
    current_df["portfolio_pct"] = round(current_df["value"] / total_value * 100, 2) if total_value > 0 else 0

    # Calculate changes from previous quarter
    if not prev_df.empty:
        prev_by_cusip = prev_df.set_index("cusip")["shares"].to_dict()
        prev_value_by_cusip = prev_df.set_index("cusip")["value"].to_dict()

        current_df["prev_shares"] = current_df["cusip"].map(prev_by_cusip).fillna(0).astype(int)
        current_df["prev_value"] = current_df["cusip"].map(prev_value_by_cusip).fillna(0).astype(int)
        current_df["change_shares"] = current_df["shares"] - current_df["prev_shares"]
        current_df["change_value"] = current_df["value"] - current_df["prev_value"]
        current_df["change_pct"] = round(
            current_df.apply(
                lambda r: ((r["shares"] - r["prev_shares"]) / r["prev_shares"] * 100)
                if r["prev_shares"] > 0 else (100.0 if r["shares"] > 0 else 0),
                axis=1
            ), 1
        )
        current_df["action"] = current_df.apply(
            lambda r: "New" if r["prev_shares"] == 0
            else "Buy" if r["change_shares"] > 0
            else "Sell" if r["change_shares"] < 0
            else "Hold",
            axis=1
        )
    else:
        current_df["prev_shares"] = 0
        current_df["prev_value"] = 0
        current_df["change_shares"] = 0
        current_df["change_value"] = 0
        current_df["change_pct"] = 0.0
        current_df["action"] = "N/A"

    metadata = {
        **fund_info,
        "fund_name": fund_name,
        "total_value": total_value,
        "num_holdings": len(current_df),
        "filing_date": current_filing.get("filing_date"),
        "report_date": current_filing.get("report_date"),
        "prev_report_date": prev_filing.get("report_date") if prev_filing else None,
    }

    return current_df, metadata


def get_all_funds_latest(progress_callback=None) -> tuple[dict, dict]:
    """
    Fetch the latest holdings for ALL tracked funds.
    Returns ({fund_name: holdings_df}, {fund_name: metadata}).
    """
    all_holdings = {}
    all_metadata = {}
    fund_names = list(TRACKED_FUNDS.keys())

    for i, fund_name in enumerate(fund_names):
        if progress_callback:
            progress_callback(i / len(fund_names), f"Loading {fund_name}...")

        try:
            df, meta = get_fund_holdings_real(fund_name)
            if not df.empty:
                all_holdings[fund_name] = df
                all_metadata[fund_name] = meta
        except Exception as e:
            logger.warning(f"Failed to load {fund_name}: {e}")

    if progress_callback:
        progress_callback(1.0, "Complete!")

    return all_holdings, all_metadata


def compute_largest_trades(all_holdings: dict, all_metadata: dict, trade_type: str = "buy") -> pd.DataFrame:
    """
    Compute the largest buys or sells across all funds.
    """
    trades = []
    for fund_name, df in all_holdings.items():
        meta = all_metadata.get(fund_name, {})
        for _, row in df.iterrows():
            change = row.get("change_shares", 0)
            change_value = row.get("change_value", 0)

            if trade_type == "buy" and change > 0:
                trades.append({
                    "fund": fund_name,
                    "manager": meta.get("manager", ""),
                    "name_of_issuer": row["name_of_issuer"],
                    "cusip": row["cusip"],
                    "title_of_class": row.get("title_of_class", ""),
                    "shares_changed": change,
                    "value_changed": abs(change_value),
                    "total_shares": row["shares"],
                    "total_value": row["value"],
                    "portfolio_pct": row.get("portfolio_pct", 0),
                    "change_pct": row.get("change_pct", 0),
                    "action": row.get("action", "Buy"),
                    "filing_date": meta.get("filing_date", ""),
                    "report_date": meta.get("report_date", ""),
                })
            elif trade_type == "sell" and change < 0:
                trades.append({
                    "fund": fund_name,
                    "manager": meta.get("manager", ""),
                    "name_of_issuer": row["name_of_issuer"],
                    "cusip": row["cusip"],
                    "title_of_class": row.get("title_of_class", ""),
                    "shares_changed": abs(change),
                    "value_changed": abs(change_value),
                    "total_shares": row["shares"],
                    "total_value": row["value"],
                    "portfolio_pct": row.get("portfolio_pct", 0),
                    "change_pct": row.get("change_pct", 0),
                    "action": row.get("action", "Sell"),
                    "filing_date": meta.get("filing_date", ""),
                    "report_date": meta.get("report_date", ""),
                })

    result = pd.DataFrame(trades)
    if not result.empty:
        result = result.sort_values("value_changed", ascending=False).reset_index(drop=True)
    return result


def compute_hot_stocks(all_holdings: dict, all_metadata: dict) -> pd.DataFrame:
    """
    Compute 'hot stocks' — stocks with the most hedge fund activity.
    """
    stock_activity = {}

    for fund_name, df in all_holdings.items():
        for _, row in df.iterrows():
            cusip = row["cusip"]
            name = row["name_of_issuer"]
            change = row.get("change_shares", 0)

            if cusip not in stock_activity:
                stock_activity[cusip] = {
                    "cusip": cusip,
                    "name_of_issuer": name,
                    "title_of_class": row.get("title_of_class", ""),
                    "funds_buying": 0,
                    "funds_selling": 0,
                    "funds_holding": 0,
                    "total_value": 0,
                    "total_shares": 0,
                    "shares_bought": 0,
                    "shares_sold": 0,
                    "buy_value": 0,
                    "sell_value": 0,
                    "holders": [],
                }

            stock_activity[cusip]["funds_holding"] += 1
            stock_activity[cusip]["total_value"] += row["value"]
            stock_activity[cusip]["total_shares"] += row["shares"]
            stock_activity[cusip]["holders"].append(fund_name)

            if change > 0:
                stock_activity[cusip]["funds_buying"] += 1
                stock_activity[cusip]["shares_bought"] += change
                stock_activity[cusip]["buy_value"] += abs(row.get("change_value", 0))
            elif change < 0:
                stock_activity[cusip]["funds_selling"] += 1
                stock_activity[cusip]["shares_sold"] += abs(change)
                stock_activity[cusip]["sell_value"] += abs(row.get("change_value", 0))

    for cusip, data in stock_activity.items():
        data["total_funds"] = data["funds_buying"] + data["funds_selling"]
        data["net_shares"] = data["shares_bought"] - data["shares_sold"]
        fb = data["funds_buying"]
        fs = data["funds_selling"]
        data["sentiment"] = "Bullish" if fb > fs else "Bearish" if fs > fb else "Neutral"
        data["holders_str"] = ", ".join(data["holders"][:5])
        del data["holders"]

    result = pd.DataFrame(list(stock_activity.values()))
    if not result.empty:
        result = result.sort_values("total_funds", ascending=False).reset_index(drop=True)
    return result


def get_fund_list_with_aum(all_metadata: dict) -> pd.DataFrame:
    """Get fund list with total AUM from filings."""
    funds = []
    for fund_name, meta in all_metadata.items():
        funds.append({
            "name": fund_name,
            "manager": meta.get("manager", ""),
            "strategy": meta.get("strategy", ""),
            "location": meta.get("location", ""),
            "founded": meta.get("founded", ""),
            "aum": meta.get("total_value", 0),
            "num_holdings": meta.get("num_holdings", 0),
            "filing_date": meta.get("filing_date", ""),
            "report_date": meta.get("report_date", ""),
        })
    df = pd.DataFrame(funds)
    if not df.empty:
        df = df.sort_values("aum", ascending=False).reset_index(drop=True)
    return df


# ─── Search & Discovery ──────────────────────────────────────────────────────

def search_13f_filers(query: str, max_results: int = 20) -> list[dict]:
    """
    Search SEC EDGAR for companies that have filed 13F-HR forms.
    Uses the EDGAR full-text search (EFTS) API.
    Returns a list of dicts with company_name, cik, filing_date, etc.
    """
    if not query or len(query.strip()) < 2:
        return []

    clean_query = query.strip().replace('"', '')
    url = (
        f"{EFTS_URL}/search-index"
        f"?q=%22{requests.utils.quote(clean_query)}%22"
        f"&forms=13F-HR"
        f"&dateRange=custom&startdt=2023-01-01"
    )

    try:
        content = _fetch_with_cache(url, accept="application/json")
        data = json.loads(content)
    except Exception as e:
        logger.warning(f"EFTS search failed: {e}")
        return []

    hits = data.get("hits", {}).get("hits", [])
    seen_ciks = set()
    results = []

    for hit in hits:
        source = hit.get("_source", {})
        entity_name = source.get("entity_name", "") or source.get("display_names", [""])[0]
        cik = source.get("entity_id", "")
        filing_date = source.get("file_date", "")
        form = source.get("form_type", "")

        if not entity_name or not cik:
            continue

        # Normalize CIK
        cik_str = str(cik).zfill(10)

        if cik_str in seen_ciks:
            continue
        seen_ciks.add(cik_str)

        # Check if it's already a tracked fund
        is_tracked = any(f["cik"] == cik_str for f in TRACKED_FUNDS.values())

        results.append({
            "entity_name": entity_name,
            "cik": cik_str,
            "filing_date": filing_date,
            "form_type": form,
            "is_tracked": is_tracked,
        })

        if len(results) >= max_results:
            break

    return results


def search_company_by_name(query: str, max_results: int = 15) -> list[dict]:
    """
    Search for companies using the SEC EDGAR company search endpoint.
    This searches company names specifically (better for auto-suggest).
    Returns list of dicts with entity_name, cik.
    """
    if not query or len(query.strip()) < 2:
        return []

    clean_query = query.strip()

    # Use the EDGAR company search via the submissions endpoint
    # We search the EFTS for company names filing 13F
    url = (
        f"{EFTS_URL}/search-index"
        f"?q=%22{requests.utils.quote(clean_query)}%22"
        f"&forms=13F-HR,13F-HR/A"
        f"&dateRange=custom&startdt=2022-01-01"
    )

    try:
        content = _fetch_with_cache(url, accept="application/json")
        data = json.loads(content)
    except Exception as e:
        logger.warning(f"Company search failed: {e}")
        return []

    hits = data.get("hits", {}).get("hits", [])
    seen = set()
    results = []

    for hit in hits:
        source = hit.get("_source", {})
        names = source.get("display_names", [])
        entity_name = source.get("entity_name", "") or (names[0] if names else "")
        cik = source.get("entity_id", "")

        if not entity_name or not cik:
            continue

        cik_str = str(cik).zfill(10)
        if cik_str in seen:
            continue
        seen.add(cik_str)

        is_tracked = any(f["cik"] == cik_str for f in TRACKED_FUNDS.values())
        tracked_name = None
        for fname, fdata in TRACKED_FUNDS.items():
            if fdata["cik"] == cik_str:
                tracked_name = fname
                break

        results.append({
            "entity_name": entity_name,
            "cik": cik_str,
            "is_tracked": is_tracked,
            "tracked_name": tracked_name,
        })

        if len(results) >= max_results:
            break

    return results


def explore_fund_by_cik(cik: str, fund_display_name: str = None) -> tuple[pd.DataFrame, dict]:
    """
    Explore any fund by its CIK — not limited to pre-tracked funds.
    Returns (holdings_df, metadata_dict).
    """
    cik_padded = str(cik).lstrip("0").zfill(10)

    # First, get the company info
    try:
        company_data = get_company_filings(cik_padded)
        company_name = company_data.get("name", fund_display_name or "Unknown Fund")
    except Exception:
        company_name = fund_display_name or "Unknown Fund"

    # Fetch current and previous holdings
    current_df, current_filing = fetch_13f_holdings(cik_padded, 0)
    prev_df, prev_filing = fetch_13f_holdings(cik_padded, 1)

    if current_df.empty:
        return pd.DataFrame(), {"fund_name": company_name, "cik": cik_padded}

    total_value = current_df["value"].sum()
    current_df["portfolio_pct"] = round(current_df["value"] / total_value * 100, 2) if total_value > 0 else 0

    # Calculate changes
    if not prev_df.empty:
        prev_by_cusip = prev_df.set_index("cusip")["shares"].to_dict()
        prev_value_by_cusip = prev_df.set_index("cusip")["value"].to_dict()

        current_df["prev_shares"] = current_df["cusip"].map(prev_by_cusip).fillna(0).astype(int)
        current_df["prev_value"] = current_df["cusip"].map(prev_value_by_cusip).fillna(0).astype(int)
        current_df["change_shares"] = current_df["shares"] - current_df["prev_shares"]
        current_df["change_value"] = current_df["value"] - current_df["prev_value"]
        current_df["change_pct"] = round(
            current_df.apply(
                lambda r: ((r["shares"] - r["prev_shares"]) / r["prev_shares"] * 100)
                if r["prev_shares"] > 0 else (100.0 if r["shares"] > 0 else 0),
                axis=1
            ), 1
        )
        current_df["action"] = current_df.apply(
            lambda r: "New" if r["prev_shares"] == 0
            else "Buy" if r["change_shares"] > 0
            else "Sell" if r["change_shares"] < 0
            else "Hold",
            axis=1
        )
    else:
        current_df["prev_shares"] = 0
        current_df["prev_value"] = 0
        current_df["change_shares"] = 0
        current_df["change_value"] = 0
        current_df["change_pct"] = 0.0
        current_df["action"] = "N/A"

    metadata = {
        "fund_name": company_name,
        "cik": cik_padded,
        "manager": "",
        "strategy": "",
        "location": "",
        "founded": "",
        "total_value": total_value,
        "num_holdings": len(current_df),
        "filing_date": current_filing.get("filing_date"),
        "report_date": current_filing.get("report_date"),
        "prev_report_date": prev_filing.get("report_date") if prev_filing else None,
    }

    # If it's a tracked fund, fill in metadata
    for fname, fdata in TRACKED_FUNDS.items():
        if fdata["cik"] == cik_padded:
            metadata["manager"] = fdata.get("manager", "")
            metadata["strategy"] = fdata.get("strategy", "")
            metadata["location"] = fdata.get("location", "")
            metadata["founded"] = fdata.get("founded", "")
            metadata["fund_name"] = fname
            break

    return current_df, metadata


# ─── Insider Trading (Form 4) ────────────────────────────────────────────────

def fetch_recent_insider_trades(limit: int = 50) -> pd.DataFrame:
    """
    Fetch the most recent Insider Trading (Form 4) filings from SEC EDGAR.
    """
    # Use EFTS to find recent Form 4 filings
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    url = (
        f"{EFTS_URL}/search-index"
        f"?forms=4"
        f"&dateRange=custom&startdt={start_date}"
    )

    try:
        content = _fetch_with_cache(url, accept="application/json")
        data = json.loads(content)
    except Exception as e:
        logger.warning(f"EFTS Form 4 search failed: {e}")
        return pd.DataFrame()

    hits = data.get("hits", {}).get("hits", [])
    trades = []

    for hit in hits:
        if len(trades) >= limit:
            break

        source = hit.get("_source", {})
        entity_name = source.get("entity_name", "")
        cik = source.get("entity_id", "")
        filing_date = source.get("file_date", "")
        accession = hit.get("_id", "").split(":")[0]  # Accession number

        if not accession:
            continue

        # Fetch the filing document to get the XML
        try:
            # For Form 4, the primary document is often the XML itself OR there is a doc4.xml
            # We use the same 'find_xml' logic but specifically for Form 4
            xml_url = find_form4_xml_url(cik, accession)
            if not xml_url:
                continue

            xml_content = _fetch_with_cache(xml_url, accept="application/xml")
            parsed_trades = parse_form4_xml(xml_content, filing_date)
            trades.extend(parsed_trades)
        except Exception as e:
            logger.debug(f"Failed to parse Form 4 {accession}: {e}")
            continue

    df = pd.DataFrame(trades)
    if not df.empty:
        # Sort by transaction date
        df = df.sort_values("transaction_date", ascending=False).reset_index(drop=True)
    return df


def find_form4_xml_url(cik: str, accession_number: str) -> Optional[str]:
    """Find the XML document URL for a Form 4 filing."""
    try:
        index = get_filing_index(cik, accession_number)
    except Exception:
        return None

    padded_cik = str(cik).zfill(10)
    acc_no_dashes = accession_number.replace("-", "")
    base_url = f"{SEC_BASE_URL}/Archives/edgar/data/{padded_cik}/{acc_no_dashes}"

    items = index.get("directory", {}).get("item", [])
    # Form 4 XML is usually named 'doc4.xml' or something similar
    for item in items:
        name = item.get("name", "").lower()
        if (name == "doc4.xml" or name.endswith(".xml")) and "primary" not in name and "R" not in item.get("name", ""):
            return f"{base_url}/{item['name']}"

    return None


def parse_form4_xml(xml_content: str, filing_date: str) -> list[dict]:
    """
    Parse SEC Form 4 XML content.
    Extracts owner, relationship, ticker, company, and transaction details.
    """
    try:
        root = ET.fromstring(xml_content)
    except Exception:
        return []

    # Basic info
    issuer = root.find(".//issuer")
    issuer_name = issuer.findtext("issuerName") if issuer is not None else ""
    issuer_ticker = issuer.findtext("issuerTicker") if issuer is not None else ""

    owner = root.find(".//reportingOwner")
    owner_name = owner.find(".//reportingOwnerName").text if owner is not None else ""

    # Relationship
    is_director = owner.findtext(".//isDirector") == "1" if owner is not None else False
    is_officer = owner.findtext(".//isOfficer") == "1" if owner is not None else False
    is_ten_percent = owner.findtext(".//isTenPercentOwner") == "1" if owner is not None else False
    officer_title = owner.findtext(".//officerTitle") if owner is not None else ""

    relationship = "Insider"
    if is_director: relationship = "Director"
    if is_officer: relationship = f"Officer ({officer_title})" if officer_title else "Officer"
    if is_ten_percent: relationship = "10% Owner"

    transactions = []

    # Non-derivative transactions (most common: direct stock buys/sells)
    for trans in root.findall(".//nonDerivativeTransaction"):
        try:
            coding = trans.find(".//transactionCoding")
            code = coding.findtext("transactionCode") if coding is not None else ""

            # Only interest in P (Purchase) or S (Sale)
            if code not in ("P", "S"):
                continue

            amounts = trans.find(".//transactionAmounts")
            if amounts is None:
                continue

            shares = float(amounts.findtext("transactionShares") or 0)
            price = float(amounts.findtext("transactionPricePerShare") or 0)
            acq_disp = amounts.findtext("transactionAcquiredDisposedCode")

            trans_date_el = trans.find(".//transactionDate")
            trans_date = trans_date_el.findtext("value") if trans_date_el is not None else filing_date

            value = shares * price

            transactions.append({
                "issuer_name": issuer_name,
                "ticker": issuer_ticker,
                "owner_name": owner_name,
                "relationship": relationship,
                "transaction_type": "Buy" if code == "P" else "Sell",
                "shares": shares,
                "price": price,
                "value": value,
                "transaction_date": trans_date,
                "filing_date": filing_date,
                "code": code,
            })
        except Exception:
            continue

    return transactions
