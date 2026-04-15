"""
Realistic mock data for hedge fund 13F filings, insider trades, and stock analytics.
All data is synthetic but modeled after real-world patterns from SEC 13F filings.
"""

import pandas as pd
import random
import datetime

random.seed(42)

# ─── Hedge Fund Definitions ───────────────────────────────────────────────────

HEDGE_FUNDS = [
    {"name": "Berkshire Hathaway", "manager": "Warren Buffett", "aum": 352_400_000_000, "founded": 1965, "strategy": "Value Investing", "location": "Omaha, NE", "ann_return_3y": 18.7, "ann_return_5y": 15.2, "sharpe": 1.42},
    {"name": "Bridgewater Associates", "manager": "Ray Dalio", "aum": 124_000_000_000, "founded": 1975, "strategy": "Global Macro", "location": "Westport, CT", "ann_return_3y": 12.3, "ann_return_5y": 10.8, "sharpe": 1.15},
    {"name": "Citadel LLC", "manager": "Ken Griffin", "aum": 62_300_000_000, "founded": 1990, "strategy": "Multi-Strategy", "location": "Chicago, IL", "ann_return_3y": 24.1, "ann_return_5y": 21.5, "sharpe": 1.87},
    {"name": "Renaissance Technologies", "manager": "Jim Simons", "aum": 55_000_000_000, "founded": 1982, "strategy": "Quantitative", "location": "East Setauket, NY", "ann_return_3y": 31.2, "ann_return_5y": 28.9, "sharpe": 2.34},
    {"name": "Two Sigma Investments", "manager": "David Siegel", "aum": 67_000_000_000, "founded": 2001, "strategy": "Quantitative", "location": "New York, NY", "ann_return_3y": 19.8, "ann_return_5y": 17.1, "sharpe": 1.55},
    {"name": "D.E. Shaw & Co.", "manager": "David Shaw", "aum": 60_000_000_000, "founded": 1988, "strategy": "Quantitative", "location": "New York, NY", "ann_return_3y": 22.4, "ann_return_5y": 19.3, "sharpe": 1.72},
    {"name": "Millennium Management", "manager": "Israel Englander", "aum": 59_400_000_000, "founded": 1989, "strategy": "Multi-Strategy", "location": "New York, NY", "ann_return_3y": 16.5, "ann_return_5y": 14.8, "sharpe": 1.38},
    {"name": "Elliott Management", "manager": "Paul Singer", "aum": 55_200_000_000, "founded": 1977, "strategy": "Activist", "location": "New York, NY", "ann_return_3y": 14.2, "ann_return_5y": 12.7, "sharpe": 1.21},
    {"name": "AQR Capital Management", "manager": "Cliff Asness", "aum": 98_000_000_000, "founded": 1998, "strategy": "Quantitative", "location": "Greenwich, CT", "ann_return_3y": 11.5, "ann_return_5y": 10.2, "sharpe": 1.05},
    {"name": "Tiger Global Management", "manager": "Chase Coleman", "aum": 35_000_000_000, "founded": 2001, "strategy": "Growth Equity", "location": "New York, NY", "ann_return_3y": 28.6, "ann_return_5y": 22.1, "sharpe": 1.65},
    {"name": "Point72 Asset Management", "manager": "Steve Cohen", "aum": 34_800_000_000, "founded": 2014, "strategy": "Multi-Strategy", "location": "Stamford, CT", "ann_return_3y": 17.3, "ann_return_5y": 15.9, "sharpe": 1.45},
    {"name": "Baupost Group", "manager": "Seth Klarman", "aum": 27_000_000_000, "founded": 1982, "strategy": "Value Investing", "location": "Boston, MA", "ann_return_3y": 13.8, "ann_return_5y": 12.1, "sharpe": 1.18},
    {"name": "Pershing Square Capital", "manager": "Bill Ackman", "aum": 18_500_000_000, "founded": 2004, "strategy": "Activist", "location": "New York, NY", "ann_return_3y": 26.3, "ann_return_5y": 18.7, "sharpe": 1.52},
    {"name": "Third Point LLC", "manager": "Dan Loeb", "aum": 16_400_000_000, "founded": 1995, "strategy": "Event Driven", "location": "New York, NY", "ann_return_3y": 15.9, "ann_return_5y": 13.4, "sharpe": 1.28},
    {"name": "Appaloosa Management", "manager": "David Tepper", "aum": 13_000_000_000, "founded": 1993, "strategy": "Distressed Debt", "location": "Short Hills, NJ", "ann_return_3y": 20.7, "ann_return_5y": 17.8, "sharpe": 1.48},
    {"name": "Viking Global Investors", "manager": "Andreas Halvorsen", "aum": 38_700_000_000, "founded": 1999, "strategy": "Long/Short Equity", "location": "Greenwich, CT", "ann_return_3y": 21.1, "ann_return_5y": 18.4, "sharpe": 1.62},
    {"name": "Coatue Management", "manager": "Philippe Laffont", "aum": 22_000_000_000, "founded": 1999, "strategy": "Technology L/S", "location": "New York, NY", "ann_return_3y": 25.8, "ann_return_5y": 20.3, "sharpe": 1.58},
    {"name": "Lone Pine Capital", "manager": "Stephen Mandel Jr.", "aum": 28_500_000_000, "founded": 1997, "strategy": "Long/Short Equity", "location": "Greenwich, CT", "ann_return_3y": 19.4, "ann_return_5y": 16.7, "sharpe": 1.44},
    {"name": "Marshall Wace", "manager": "Paul Marshall", "aum": 64_000_000_000, "founded": 1997, "strategy": "Long/Short Equity", "location": "London, UK", "ann_return_3y": 16.8, "ann_return_5y": 14.5, "sharpe": 1.35},
    {"name": "Soros Fund Management", "manager": "George Soros", "aum": 25_000_000_000, "founded": 1970, "strategy": "Global Macro", "location": "New York, NY", "ann_return_3y": 14.9, "ann_return_5y": 13.1, "sharpe": 1.22},
]

# ─── Stock Universe ────────────────────────────────────────────────────────────

STOCKS = [
    {"ticker": "AAPL", "name": "Apple Inc.", "sector": "Technology", "price": 227.48, "market_cap": 3_480_000_000_000},
    {"ticker": "MSFT", "name": "Microsoft Corp.", "sector": "Technology", "price": 456.32, "market_cap": 3_390_000_000_000},
    {"ticker": "NVDA", "name": "NVIDIA Corp.", "sector": "Technology", "price": 138.25, "market_cap": 3_390_000_000_000},
    {"ticker": "AMZN", "name": "Amazon.com Inc.", "sector": "Consumer Cyclical", "price": 214.87, "market_cap": 2_270_000_000_000},
    {"ticker": "GOOGL", "name": "Alphabet Inc.", "sector": "Technology", "price": 177.93, "market_cap": 2_180_000_000_000},
    {"ticker": "META", "name": "Meta Platforms Inc.", "sector": "Technology", "price": 612.77, "market_cap": 1_560_000_000_000},
    {"ticker": "BRK.B", "name": "Berkshire Hathaway", "sector": "Financial Services", "price": 524.18, "market_cap": 1_130_000_000_000},
    {"ticker": "TSLA", "name": "Tesla Inc.", "sector": "Consumer Cyclical", "price": 248.42, "market_cap": 795_000_000_000},
    {"ticker": "JPM", "name": "JPMorgan Chase", "sector": "Financial Services", "price": 243.67, "market_cap": 698_000_000_000},
    {"ticker": "V", "name": "Visa Inc.", "sector": "Financial Services", "price": 318.52, "market_cap": 624_000_000_000},
    {"ticker": "UNH", "name": "UnitedHealth Group", "sector": "Healthcare", "price": 512.34, "market_cap": 470_000_000_000},
    {"ticker": "XOM", "name": "Exxon Mobil Corp.", "sector": "Energy", "price": 108.92, "market_cap": 460_000_000_000},
    {"ticker": "MA", "name": "Mastercard Inc.", "sector": "Financial Services", "price": 524.81, "market_cap": 454_000_000_000},
    {"ticker": "JNJ", "name": "Johnson & Johnson", "sector": "Healthcare", "price": 156.73, "market_cap": 377_000_000_000},
    {"ticker": "PG", "name": "Procter & Gamble", "sector": "Consumer Defensive", "price": 171.24, "market_cap": 403_000_000_000},
    {"ticker": "HD", "name": "Home Depot Inc.", "sector": "Consumer Cyclical", "price": 401.56, "market_cap": 393_000_000_000},
    {"ticker": "AVGO", "name": "Broadcom Inc.", "sector": "Technology", "price": 186.43, "market_cap": 868_000_000_000},
    {"ticker": "CVX", "name": "Chevron Corp.", "sector": "Energy", "price": 152.38, "market_cap": 278_000_000_000},
    {"ticker": "MRK", "name": "Merck & Co.", "sector": "Healthcare", "price": 127.84, "market_cap": 323_000_000_000},
    {"ticker": "ABBV", "name": "AbbVie Inc.", "sector": "Healthcare", "price": 191.56, "market_cap": 338_000_000_000},
    {"ticker": "KO", "name": "Coca-Cola Co.", "sector": "Consumer Defensive", "price": 63.42, "market_cap": 274_000_000_000},
    {"ticker": "PEP", "name": "PepsiCo Inc.", "sector": "Consumer Defensive", "price": 145.87, "market_cap": 200_000_000_000},
    {"ticker": "BAC", "name": "Bank of America", "sector": "Financial Services", "price": 42.56, "market_cap": 335_000_000_000},
    {"ticker": "CRM", "name": "Salesforce Inc.", "sector": "Technology", "price": 324.18, "market_cap": 310_000_000_000},
    {"ticker": "LLY", "name": "Eli Lilly & Co.", "sector": "Healthcare", "price": 812.43, "market_cap": 772_000_000_000},
    {"ticker": "WMT", "name": "Walmart Inc.", "sector": "Consumer Defensive", "price": 93.45, "market_cap": 752_000_000_000},
    {"ticker": "NFLX", "name": "Netflix Inc.", "sector": "Communication", "price": 948.72, "market_cap": 408_000_000_000},
    {"ticker": "AMD", "name": "AMD Inc.", "sector": "Technology", "price": 118.34, "market_cap": 191_000_000_000},
    {"ticker": "ORCL", "name": "Oracle Corp.", "sector": "Technology", "price": 178.42, "market_cap": 492_000_000_000},
    {"ticker": "DIS", "name": "Walt Disney Co.", "sector": "Communication", "price": 112.78, "market_cap": 206_000_000_000},
]

STOCK_MAP = {s["ticker"]: s for s in STOCKS}


# ─── Generate Holdings per Fund ───────────────────────────────────────────────

def _generate_fund_holdings(fund):
    """Generate realistic 13F holdings for a fund."""
    num_holdings = random.randint(8, 20)
    selected = random.sample(STOCKS, num_holdings)
    holdings = []
    remaining_pct = 100.0

    for i, stock in enumerate(selected):
        if i == len(selected) - 1:
            pct = round(remaining_pct, 2)
        else:
            max_pct = min(remaining_pct - (len(selected) - i - 1) * 0.5, 35.0)
            pct = round(random.uniform(0.5, max(1.0, max_pct)), 2)
            remaining_pct -= pct

        value = int(fund["aum"] * pct / 100)
        shares = int(value / stock["price"])
        prev_shares = int(shares * random.uniform(0.7, 1.3))
        change_pct = round((shares - prev_shares) / prev_shares * 100, 1) if prev_shares else 0

        action = "Buy" if shares > prev_shares else "Sell" if shares < prev_shares else "Hold"
        if prev_shares == 0:
            action = "New Position"

        holdings.append({
            "ticker": stock["ticker"],
            "name": stock["name"],
            "sector": stock["sector"],
            "shares": shares,
            "value": value,
            "portfolio_pct": pct,
            "prev_shares": prev_shares,
            "change_shares": shares - prev_shares,
            "change_pct": change_pct,
            "action": action,
            "price": stock["price"],
        })

    holdings.sort(key=lambda x: x["value"], reverse=True)
    return holdings


def get_fund_holdings(fund_name):
    fund = next((f for f in HEDGE_FUNDS if f["name"] == fund_name), None)
    if fund is None:
        return pd.DataFrame()
    holdings = _generate_fund_holdings(fund)
    return pd.DataFrame(holdings)


# ─── Largest Buys / Sells ─────────────────────────────────────────────────────

def _generate_large_trades(trade_type="buy"):
    trades = []
    for fund in HEDGE_FUNDS:
        holdings = _generate_fund_holdings(fund)
        for h in holdings:
            if trade_type == "buy" and h["change_shares"] > 0:
                trades.append({
                    "fund": fund["name"],
                    "manager": fund["manager"],
                    "ticker": h["ticker"],
                    "stock": h["name"],
                    "sector": h["sector"],
                    "shares_changed": abs(h["change_shares"]),
                    "value_changed": abs(int(h["change_shares"] * h["price"])),
                    "total_shares": h["shares"],
                    "total_value": h["value"],
                    "portfolio_pct": h["portfolio_pct"],
                    "filing_date": (datetime.date(2026, 2, 14) - datetime.timedelta(days=random.randint(0, 30))).isoformat(),
                    "report_date": "2025-12-31",
                })
            elif trade_type == "sell" and h["change_shares"] < 0:
                trades.append({
                    "fund": fund["name"],
                    "manager": fund["manager"],
                    "ticker": h["ticker"],
                    "stock": h["name"],
                    "sector": h["sector"],
                    "shares_changed": abs(h["change_shares"]),
                    "value_changed": abs(int(h["change_shares"] * h["price"])),
                    "total_shares": h["shares"],
                    "total_value": h["value"],
                    "portfolio_pct": h["portfolio_pct"],
                    "filing_date": (datetime.date(2026, 2, 14) - datetime.timedelta(days=random.randint(0, 30))).isoformat(),
                    "report_date": "2025-12-31",
                })

    trades.sort(key=lambda x: x["value_changed"], reverse=True)
    return trades[:50]


def get_largest_buys():
    return pd.DataFrame(_generate_large_trades("buy"))


def get_largest_sells():
    return pd.DataFrame(_generate_large_trades("sell"))


# ─── Insider Trading Data ─────────────────────────────────────────────────────

INSIDERS = [
    {"name": "Tim Cook", "title": "CEO", "company": "AAPL", "company_name": "Apple Inc."},
    {"name": "Satya Nadella", "title": "CEO", "company": "MSFT", "company_name": "Microsoft Corp."},
    {"name": "Jensen Huang", "title": "CEO", "company": "NVDA", "company_name": "NVIDIA Corp."},
    {"name": "Andy Jassy", "title": "CEO", "company": "AMZN", "company_name": "Amazon.com Inc."},
    {"name": "Sundar Pichai", "title": "CEO", "company": "GOOGL", "company_name": "Alphabet Inc."},
    {"name": "Mark Zuckerberg", "title": "CEO", "company": "META", "company_name": "Meta Platforms Inc."},
    {"name": "Elon Musk", "title": "CEO", "company": "TSLA", "company_name": "Tesla Inc."},
    {"name": "Jamie Dimon", "title": "CEO", "company": "JPM", "company_name": "JPMorgan Chase"},
    {"name": "Ryan McInerney", "title": "CEO", "company": "V", "company_name": "Visa Inc."},
    {"name": "David Ricks", "title": "CEO", "company": "LLY", "company_name": "Eli Lilly & Co."},
    {"name": "Lisa Su", "title": "CEO", "company": "AMD", "company_name": "AMD Inc."},
    {"name": "Bob Iger", "title": "CEO", "company": "DIS", "company_name": "Walt Disney Co."},
    {"name": "Doug McMillon", "title": "CEO", "company": "WMT", "company_name": "Walmart Inc."},
    {"name": "Ted Sarandos", "title": "Co-CEO", "company": "NFLX", "company_name": "Netflix Inc."},
    {"name": "Marc Benioff", "title": "CEO", "company": "CRM", "company_name": "Salesforce Inc."},
    {"name": "Luca Maestri", "title": "CFO", "company": "AAPL", "company_name": "Apple Inc."},
    {"name": "Amy Hood", "title": "CFO", "company": "MSFT", "company_name": "Microsoft Corp."},
    {"name": "Colette Kress", "title": "CFO", "company": "NVDA", "company_name": "NVIDIA Corp."},
    {"name": "Ruth Porat", "title": "CFO", "company": "GOOGL", "company_name": "Alphabet Inc."},
    {"name": "Susan Li", "title": "CFO", "company": "META", "company_name": "Meta Platforms Inc."},
]


def _generate_insider_trades(trade_type="buy"):
    trades = []
    for insider in INSIDERS:
        stock = STOCK_MAP.get(insider["company"])
        if not stock:
            continue
        num_trades = random.randint(0, 3)
        for _ in range(num_trades):
            shares = random.randint(1000, 500000)
            value = int(shares * stock["price"])
            if trade_type == "sell":
                value = int(value * random.uniform(1.0, 1.5))

            trade_date = datetime.date(2026, 4, 1) - datetime.timedelta(days=random.randint(0, 90))
            trades.append({
                "insider": insider["name"],
                "title": insider["title"],
                "ticker": insider["company"],
                "company": insider["company_name"],
                "trade_type": trade_type.capitalize(),
                "shares": shares,
                "price_per_share": round(stock["price"] * random.uniform(0.95, 1.05), 2),
                "value": value,
                "date": trade_date.isoformat(),
                "filing_date": (trade_date + datetime.timedelta(days=random.randint(1, 3))).isoformat(),
                "sector": stock["sector"],
            })

    trades.sort(key=lambda x: x["value"], reverse=True)
    return trades[:40]


def get_insider_buys():
    return pd.DataFrame(_generate_insider_trades("buy"))


def get_insider_sells():
    return pd.DataFrame(_generate_insider_trades("sell"))


# ─── Top / Popular / Largest Funds ─────────────────────────────────────────────

def get_top_funds():
    """Top funds by 3-year annualized return."""
    df = pd.DataFrame(HEDGE_FUNDS)
    df = df.sort_values("ann_return_3y", ascending=False).reset_index(drop=True)
    df.index += 1
    df.index.name = "Rank"
    return df


def get_largest_funds():
    """Largest funds by AUM."""
    df = pd.DataFrame(HEDGE_FUNDS)
    df = df.sort_values("aum", ascending=False).reset_index(drop=True)
    df.index += 1
    df.index.name = "Rank"
    return df


def get_popular_funds():
    """Simulated 'popular' funds by search frequency."""
    df = pd.DataFrame(HEDGE_FUNDS)
    df["searches"] = [random.randint(5000, 50000) for _ in range(len(df))]
    df = df.sort_values("searches", ascending=False).reset_index(drop=True)
    df.index += 1
    df.index.name = "Rank"
    return df


# ─── Hot Stocks / Consensus ────────────────────────────────────────────────────

def get_hot_stocks():
    """Stocks most traded by hedge funds."""
    results = []
    for stock in STOCKS:
        funds_buying = random.randint(1, 12)
        funds_selling = random.randint(0, 8)
        total_bought = random.randint(100_000, 10_000_000)
        total_sold = random.randint(50_000, 5_000_000)
        results.append({
            "ticker": stock["ticker"],
            "name": stock["name"],
            "sector": stock["sector"],
            "price": stock["price"],
            "market_cap": stock["market_cap"],
            "funds_buying": funds_buying,
            "funds_selling": funds_selling,
            "total_funds": funds_buying + funds_selling,
            "shares_bought": total_bought,
            "shares_sold": total_sold,
            "net_shares": total_bought - total_sold,
            "buy_value": int(total_bought * stock["price"]),
            "sentiment": "Bullish" if funds_buying > funds_selling else "Bearish" if funds_selling > funds_buying else "Neutral",
        })

    results.sort(key=lambda x: x["total_funds"], reverse=True)
    return pd.DataFrame(results)


def get_consensus_picks(direction="buy"):
    """Stocks where most funds agree on direction."""
    hot = get_hot_stocks()
    if direction == "buy":
        return hot[hot["sentiment"] == "Bullish"].sort_values("funds_buying", ascending=False)
    else:
        return hot[hot["sentiment"] == "Bearish"].sort_values("funds_selling", ascending=False)


# ─── Performance History ──────────────────────────────────────────────────────

def get_performance_history(fund_name):
    """Generate quarterly performance history for a fund."""
    fund = next((f for f in HEDGE_FUNDS if f["name"] == fund_name), None)
    if not fund:
        return pd.DataFrame()

    quarters = []
    base_return = fund["ann_return_3y"] / 4
    cumulative = 100.0

    for year in range(2020, 2027):
        for q in range(1, 5):
            if year == 2026 and q > 1:
                break
            qtr_return = round(base_return + random.uniform(-8, 8), 2)
            cumulative *= (1 + qtr_return / 100)
            sp500_return = round(random.uniform(-5, 8), 2)
            quarters.append({
                "quarter": f"Q{q} {year}",
                "year": year,
                "q": q,
                "fund_return": qtr_return,
                "sp500_return": sp500_return,
                "fund_cumulative": round(cumulative, 2),
                "alpha": round(qtr_return - sp500_return, 2),
            })

    return pd.DataFrame(quarters)
