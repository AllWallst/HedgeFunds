"""
HedgeFlow — Hedge Fund & Insider Trading Intelligence Platform
Real SEC EDGAR 13F data — no mock data.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from data.sec_edgar import (
    TRACKED_FUNDS,
    get_fund_holdings_real,
    get_all_funds_latest,
    compute_largest_trades,
    compute_hot_stocks,
    get_fund_list_with_aum,
    clear_cache,
    search_13f_filers,
    search_company_by_name,
    explore_fund_by_cik,
    fetch_recent_insider_trades,
)

# ─── Page Config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="HedgeFlow — Real 13F Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Inject Custom CSS ───────────────────────────────────────────────────────

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap');

    :root {
        --bg-primary: #0a0e17;
        --bg-secondary: #111827;
        --bg-card: #1a1f2e;
        --bg-card-hover: #232a3b;
        --border-color: #2a3142;
        --text-primary: #e5e7eb;
        --text-secondary: #9ca3af;
        --text-muted: #6b7280;
        --accent-green: #10b981;
        --accent-red: #ef4444;
        --accent-blue: #3b82f6;
        --accent-purple: #8b5cf6;
        --accent-gold: #f59e0b;
        --shadow-card: 0 4px 24px rgba(0,0,0,0.25);
    }

    .stApp { font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important; }
    .main .block-container { padding: 1rem 2rem 2rem 2rem; max-width: 1400px; }

    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0d1117 0%, #161b22 100%);
        border-right: 1px solid var(--border-color);
    }

    div[data-testid="stMetric"] {
        background: var(--bg-card);
        border: 1px solid var(--border-color);
        border-radius: 12px;
        padding: 1.2rem 1.4rem;
        box-shadow: var(--shadow-card);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    div[data-testid="stMetric"]:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 32px rgba(0,0,0,0.35);
    }
    div[data-testid="stMetric"] > div > div > div {
        font-size: 0.8rem; font-weight: 600; text-transform: uppercase;
        letter-spacing: 0.06em; color: var(--text-muted) !important;
    }

    div[data-testid="stDataFrame"] {
        border: 1px solid var(--border-color);
        border-radius: 12px;
        overflow: hidden;
    }

    h1, h2, h3, h4 { font-family: 'Inter', sans-serif !important; font-weight: 700 !important; }

    .hero-title {
        font-size: 2.2rem; font-weight: 900;
        background: linear-gradient(135deg, #3b82f6 0%, #8b5cf6 50%, #ec4899 100%);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        background-clip: text; margin-bottom: 0.2rem; line-height: 1.1;
    }
    .hero-subtitle { font-size: 1rem; color: #9ca3af; font-weight: 400; margin-bottom: 1.5rem; }
    .section-header {
        font-size: 1.3rem; font-weight: 700; color: #e5e7eb;
        margin: 1.5rem 0 0.8rem 0; display: flex; align-items: center; gap: 0.5rem;
    }
    .badge {
        display: inline-block; padding: 0.2rem 0.65rem; border-radius: 20px;
        font-size: 0.7rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em;
    }
    .badge-buy { background: rgba(16,185,129,0.15); color: #34d399; border: 1px solid rgba(16,185,129,0.3); }
    .badge-sell { background: rgba(239,68,68,0.15); color: #f87171; border: 1px solid rgba(239,68,68,0.3); }
    .badge-hot { background: rgba(245,158,11,0.15); color: #fbbf24; border: 1px solid rgba(245,158,11,0.3); }
    .badge-new { background: rgba(139,92,246,0.15); color: #a78bfa; border: 1px solid rgba(139,92,246,0.3); }
    .badge-live { background: rgba(16,185,129,0.15); color: #34d399; border: 1px solid rgba(16,185,129,0.3);
                  animation: pulse 2s ease-in-out infinite; }
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.6; } }

    .stat-card {
        background: linear-gradient(135deg, var(--bg-card) 0%, var(--bg-card-hover) 100%);
        border: 1px solid var(--border-color); border-radius: 16px; padding: 1.5rem;
        text-align: center; box-shadow: var(--shadow-card); transition: all 0.3s ease;
    }
    .stat-card:hover { border-color: rgba(59,130,246,0.4); transform: translateY(-3px); }
    .stat-value {
        font-size: 2rem; font-weight: 800;
        background: linear-gradient(135deg, #3b82f6, #8b5cf6);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text;
    }
    .stat-label {
        font-size: 0.78rem; color: #6b7280; font-weight: 600;
        text-transform: uppercase; letter-spacing: 0.06em; margin-top: 0.3rem;
    }
    .fund-card {
        background: var(--bg-card); border: 1px solid var(--border-color);
        border-radius: 14px; padding: 1.2rem 1.5rem; margin-bottom: 0.6rem;
        transition: all 0.25s ease; cursor: pointer;
    }
    .fund-card:hover {
        border-color: rgba(59,130,246,0.5); background: var(--bg-card-hover);
        box-shadow: 0 0 20px rgba(59,130,246,0.08);
    }
    .fund-name { font-size: 1.05rem; font-weight: 700; color: #e5e7eb; }
    .fund-manager { font-size: 0.82rem; color: #9ca3af; font-weight: 400; }
    .fund-aum { font-size: 1.1rem; font-weight: 700; color: #3b82f6; }
    .positive { color: #10b981 !important; }
    .negative { color: #ef4444 !important; }
    .divider {
        height: 1px;
        background: linear-gradient(90deg, transparent 0%, var(--border-color) 50%, transparent 100%);
        margin: 1.5rem 0;
    }
    .footer {
        text-align: center; padding: 2rem 0 1rem 0; color: #4b5563;
        font-size: 0.78rem; border-top: 1px solid var(--border-color); margin-top: 3rem;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 0; background: var(--bg-card); border-radius: 12px; padding: 4px;
        border: 1px solid var(--border-color);
    }
    .stTabs [data-baseweb="tab"] { border-radius: 8px; padding: 0.5rem 1.2rem; font-weight: 600; font-size: 0.85rem; }
    .stTabs [aria-selected="true"] { background: rgba(59,130,246,0.15) !important; color: #3b82f6 !important; }
    a { color: #3b82f6 !important; text-decoration: none !important; }
    a:hover { color: #60a5fa !important; }
</style>
""", unsafe_allow_html=True)


# ─── Helper Functions ─────────────────────────────────────────────────────────

def fmt_money(val, decimals=1):
    if val >= 1_000_000_000_000:
        return f"${val / 1_000_000_000_000:,.{decimals}f}T"
    elif val >= 1_000_000_000:
        return f"${val / 1_000_000_000:,.{decimals}f}B"
    elif val >= 1_000_000:
        return f"${val / 1_000_000:,.{decimals}f}M"
    elif val >= 1_000:
        return f"${val / 1_000:,.{decimals}f}K"
    return f"${val:,.0f}"


def fmt_number(val):
    if val >= 1_000_000:
        return f"{val / 1_000_000:,.1f}M"
    elif val >= 1_000:
        return f"{val / 1_000:,.1f}K"
    return f"{val:,}"


def render_logo():
    st.markdown("""
        <div style="display:flex;align-items:center;gap:0.8rem;margin-bottom:0.5rem;">
            <div style="width:42px;height:42px;background:linear-gradient(135deg,#3b82f6,#8b5cf6);
                        border-radius:12px;display:flex;align-items:center;justify-content:center;
                        font-size:1.4rem;box-shadow:0 4px 12px rgba(59,130,246,0.3);">📊</div>
            <div>
                <div class="hero-title">HedgeFlow</div>
            </div>
        </div>
        <div class="hero-subtitle">Real SEC EDGAR 13F filings — track hedge fund portfolios with actual data</div>
    """, unsafe_allow_html=True)


def plotly_dark_layout(fig, title="", height=450):
    """Apply consistent dark theme to plotly figures."""
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter", color="#9ca3af"),
        title=dict(text=title, font=dict(size=16, color="#e5e7eb")),
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor="rgba(42,49,66,0.5)", zeroline=False),
        margin=dict(t=50, b=40, l=50, r=20),
        height=height,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


# ─── Data Loading with Caching ───────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def load_all_funds():
    """Load all fund data. Cached for 1 hour in Streamlit."""
    holdings, metadata = get_all_funds_latest()
    return holdings, metadata


@st.cache_data(ttl=3600, show_spinner=False)
def load_single_fund(fund_name):
    """Load a single fund's holdings."""
    return get_fund_holdings_real(fund_name)


# ─── Sidebar Navigation ──────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("""
        <div style="text-align:center;padding:1rem 0 1.5rem 0;">
            <div style="width:48px;height:48px;background:linear-gradient(135deg,#3b82f6,#8b5cf6);
                        border-radius:14px;display:inline-flex;align-items:center;justify-content:center;
                        font-size:1.6rem;box-shadow:0 4px 16px rgba(59,130,246,0.35);margin-bottom:0.5rem;">📊</div>
            <div style="font-size:1.4rem;font-weight:800;
                        background:linear-gradient(135deg,#3b82f6,#8b5cf6,#ec4899);
                        -webkit-background-clip:text;-webkit-text-fill-color:transparent;
                        background-clip:text;">HedgeFlow</div>
            <div style="font-size:0.72rem;color:#6b7280;font-weight:500;letter-spacing:0.1em;text-transform:uppercase;">
                Real 13F Data</div>
        </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    page = st.radio(
        "Navigate",
        [
            "🏠 Dashboard",
            "🔍 Fund Search",
            "👥 Insider Trades",
            "📈 Largest Buys",
            "📉 Largest Sells",
            "🏆 Top Funds by AUM",
            "📊 Fund Explorer",
            "🔥 Hot Stocks",
            "✅ Consensus Buys",
            "❌ Consensus Sells",
        ],
        label_visibility="collapsed",
    )

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    st.markdown(f"""
        <div style="background:rgba(59,130,246,0.08);border:1px solid rgba(59,130,246,0.2);
                    border-radius:12px;padding:1rem;margin-top:0.5rem;">
            <div style="font-size:0.72rem;color:#6b7280;font-weight:600;text-transform:uppercase;
                        letter-spacing:0.06em;margin-bottom:0.4rem;">📋 Data Source</div>
            <div style="font-size:0.9rem;font-weight:700;color:#e5e7eb;">SEC EDGAR</div>
            <div style="font-size:0.78rem;color:#9ca3af;margin-top:0.2rem;">Real 13F-HR Filings</div>
            <div style="font-size:0.78rem;color:#9ca3af;">{len(TRACKED_FUNDS)} Funds Tracked</div>
        </div>
    """, unsafe_allow_html=True)

    st.markdown("""
        <div style="margin-top:1rem;padding:0.8rem;background:rgba(16,185,129,0.08);
                    border:1px solid rgba(16,185,129,0.2);border-radius:10px;">
            <div style="font-size:0.72rem;color:#6b7280;font-weight:600;text-transform:uppercase;
                        letter-spacing:0.06em;">✅ Live SEC Data</div>
            <div style="font-size:0.78rem;color:#10b981;font-weight:600;margin-top:0.3rem;">
                Fetched directly from EDGAR</div>
            <div style="font-size:0.72rem;color:#6b7280;margin-top:0.2rem;">
                Cached locally for performance</div>
        </div>
    """, unsafe_allow_html=True)

    if st.button("🔄 Refresh Data", use_container_width=True):
        clear_cache()
        st.cache_data.clear()
        st.rerun()


# ─── PAGE: Dashboard ─────────────────────────────────────────────────────────

if page == "🏠 Dashboard":
    render_logo()

    st.markdown("""
        <div style="background:rgba(59,130,246,0.06);border:1px solid rgba(59,130,246,0.15);
                    border-radius:12px;padding:0.8rem 1.2rem;margin-bottom:1rem;
                    display:flex;align-items:center;gap:0.5rem;">
            <span class="badge badge-live">LIVE</span>
            <span style="color:#9ca3af;font-size:0.85rem;">
                All data sourced from real SEC EDGAR 13F-HR filings. First load fetches from SEC (may take ~60s).
            </span>
        </div>
    """, unsafe_allow_html=True)

    with st.spinner("🔄 Fetching 13F filings from SEC EDGAR... (first load may take ~60 seconds)"):
        all_holdings, all_metadata = load_all_funds()

    if not all_holdings:
        st.error("⚠️ Could not fetch data from SEC EDGAR. Check your internet connection and try again.")
        st.stop()

    # Overview metrics
    total_aum = sum(m.get("total_value", 0) for m in all_metadata.values())
    total_holdings = sum(m.get("num_holdings", 0) for m in all_metadata.values())
    funds_loaded = len(all_holdings)
    largest_fund = max(all_metadata.values(), key=lambda x: x.get("total_value", 0))

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total 13F Value", fmt_money(total_aum), f"{funds_loaded} Funds")
    with col2:
        st.metric("Total Holdings", f"{total_holdings:,}", "Across all funds")
    with col3:
        st.metric("Largest Fund", largest_fund.get("fund_name", "N/A").split()[0],
                  fmt_money(largest_fund.get("total_value", 0)))
    with col4:
        # Get most recent filing date
        dates = [m.get("report_date", "") for m in all_metadata.values() if m.get("report_date")]
        latest = max(dates) if dates else "N/A"
        st.metric("Latest Report Date", latest)

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    # Two columns: largest buys and sells
    left_col, right_col = st.columns([1, 1])

    with left_col:
        st.markdown('<div class="section-header">📈 Largest Buys <span class="badge badge-buy">REAL DATA</span></div>',
                   unsafe_allow_html=True)
        buys = compute_largest_trades(all_holdings, all_metadata, "buy").head(12)
        if not buys.empty:
            for _, row in buys.iterrows():
                st.markdown(f"""
                    <div class="fund-card">
                        <div style="display:flex;justify-content:space-between;align-items:center;">
                            <div style="flex:1;min-width:0;">
                                <div style="font-weight:700;color:#e5e7eb;font-size:0.95rem;
                                            white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">
                                    {row['name_of_issuer']}</div>
                                <div style="color:#6b7280;font-size:0.75rem;">{row['fund']}</div>
                            </div>
                            <div style="text-align:right;flex-shrink:0;margin-left:0.5rem;">
                                <div style="font-weight:700;color:#10b981;font-size:0.95rem;">
                                    +{fmt_money(row['value_changed'])}</div>
                                <div style="color:#6b7280;font-size:0.72rem;">
                                    +{fmt_number(row['shares_changed'])} shares</div>
                            </div>
                        </div>
                    </div>
                """, unsafe_allow_html=True)
        else:
            st.info("No buy data available yet.")

    with right_col:
        st.markdown('<div class="section-header">📉 Largest Sells <span class="badge badge-sell">REAL DATA</span></div>',
                   unsafe_allow_html=True)
        sells = compute_largest_trades(all_holdings, all_metadata, "sell").head(12)
        if not sells.empty:
            for _, row in sells.iterrows():
                st.markdown(f"""
                    <div class="fund-card">
                        <div style="display:flex;justify-content:space-between;align-items:center;">
                            <div style="flex:1;min-width:0;">
                                <div style="font-weight:700;color:#e5e7eb;font-size:0.95rem;
                                            white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">
                                    {row['name_of_issuer']}</div>
                                <div style="color:#6b7280;font-size:0.75rem;">{row['fund']}</div>
                            </div>
                            <div style="text-align:right;flex-shrink:0;margin-left:0.5rem;">
                                <div style="font-weight:700;color:#ef4444;font-size:0.95rem;">
                                    -{fmt_money(row['value_changed'])}</div>
                                <div style="color:#6b7280;font-size:0.72rem;">
                                    -{fmt_number(row['shares_changed'])} shares</div>
                            </div>
                        </div>
                    </div>
                """, unsafe_allow_html=True)
        else:
            st.info("No sell data available yet.")

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    # Fund rankings
    st.markdown('<div class="section-header">🏦 Tracked Funds by 13F Portfolio Value</div>',
               unsafe_allow_html=True)

    fund_list = get_fund_list_with_aum(all_metadata)
    if not fund_list.empty:
        cols = st.columns(4)
        for i, (_, fund) in enumerate(fund_list.head(8).iterrows()):
            with cols[i % 4]:
                st.markdown(f"""
                    <div class="stat-card" style="margin-bottom:0.8rem;">
                        <div style="font-size:0.72rem;color:#6b7280;font-weight:600;text-transform:uppercase;
                                    letter-spacing:0.06em;">#{i+1} by AUM</div>
                        <div style="font-size:0.95rem;font-weight:700;color:#e5e7eb;margin:0.4rem 0 0.1rem 0;
                                    white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">
                            {fund['name']}</div>
                        <div style="font-size:0.78rem;color:#9ca3af;">{fund['manager']}</div>
                        <div style="margin-top:0.5rem;">
                            <span class="stat-value" style="font-size:1.2rem;">{fmt_money(fund['aum'])}</span>
                        </div>
                        <div style="font-size:0.72rem;color:#6b7280;margin-top:0.3rem;">
                            {fund['num_holdings']} holdings · {fund['strategy']}</div>
                    </div>
                """, unsafe_allow_html=True)


# ─── PAGE: Largest Buys ──────────────────────────────────────────────────────

elif page == "📈 Largest Buys":
    render_logo()
    st.markdown("""
        <div class="section-header">📈 Largest Hedge Fund Buys
            <span class="badge badge-buy">REAL 13F DATA</span>
        </div>
        <p style="color:#9ca3af;font-size:0.88rem;margin-bottom:1rem;">
            Largest stock purchases by hedge funds — comparing current vs. previous quarter 13F filings from SEC EDGAR.
        </p>
    """, unsafe_allow_html=True)

    with st.spinner("🔄 Fetching from SEC EDGAR..."):
        all_holdings, all_metadata = load_all_funds()

    if not all_holdings:
        st.error("⚠️ Could not load data.")
        st.stop()

    buys = compute_largest_trades(all_holdings, all_metadata, "buy")

    if buys.empty:
        st.info("No buy data available.")
        st.stop()

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Buy Volume", fmt_money(buys["value_changed"].sum()))
    with col2:
        st.metric("Unique Stocks", buys["name_of_issuer"].nunique())
    with col3:
        st.metric("Funds Active", buys["fund"].nunique())

    # Top buys by value treemap
    top_buys = buys.head(40)
    if len(top_buys) > 0:
        fig = px.treemap(
            top_buys, path=["fund", "name_of_issuer"], values="value_changed",
            color="value_changed",
            color_continuous_scale=["#1a1f2e", "#10b981", "#34d399"],
            title="Buy Volume by Fund → Stock",
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter", color="#e5e7eb"),
            title_font_size=16,
            margin=dict(t=40, l=10, r=10, b=10),
            height=500,
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    # Table
    display_df = buys[["name_of_issuer", "fund", "manager", "shares_changed", "value_changed",
                       "total_value", "portfolio_pct", "filing_date"]].copy()
    display_df.columns = ["Stock", "Fund", "Manager", "Shares Bought", "Value Added",
                          "Total Position", "Port. %", "Filing Date"]
    display_df["Value Added"] = display_df["Value Added"].apply(fmt_money)
    display_df["Total Position"] = display_df["Total Position"].apply(fmt_money)
    display_df["Port. %"] = display_df["Port. %"].apply(lambda x: f"{x:.1f}%")
    display_df["Shares Bought"] = display_df["Shares Bought"].apply(lambda x: f"{x:,}")
    display_df.index = range(1, len(display_df) + 1)

    st.dataframe(display_df, use_container_width=True, height=600)


# ─── PAGE: Largest Sells ─────────────────────────────────────────────────────

elif page == "📉 Largest Sells":
    render_logo()
    st.markdown("""
        <div class="section-header">📉 Largest Hedge Fund Sells
            <span class="badge badge-sell">REAL 13F DATA</span>
        </div>
        <p style="color:#9ca3af;font-size:0.88rem;margin-bottom:1rem;">
            Largest stock sales by hedge funds between the two most recent 13F filings.
        </p>
    """, unsafe_allow_html=True)

    with st.spinner("🔄 Fetching from SEC EDGAR..."):
        all_holdings, all_metadata = load_all_funds()

    if not all_holdings:
        st.error("⚠️ Could not load data.")
        st.stop()

    sells = compute_largest_trades(all_holdings, all_metadata, "sell")

    if sells.empty:
        st.info("No sell data available.")
        st.stop()

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Sell Volume", fmt_money(sells["value_changed"].sum()))
    with col2:
        st.metric("Unique Stocks", sells["name_of_issuer"].nunique())
    with col3:
        st.metric("Funds Active", sells["fund"].nunique())

    top_sells = sells.head(40)
    if len(top_sells) > 0:
        fig = px.treemap(
            top_sells, path=["fund", "name_of_issuer"], values="value_changed",
            color="value_changed",
            color_continuous_scale=["#1a1f2e", "#ef4444", "#f87171"],
            title="Sell Volume by Fund → Stock",
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter", color="#e5e7eb"),
            title_font_size=16,
            margin=dict(t=40, l=10, r=10, b=10),
            height=500,
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    display_df = sells[["name_of_issuer", "fund", "manager", "shares_changed", "value_changed",
                        "total_value", "portfolio_pct", "filing_date"]].copy()
    display_df.columns = ["Stock", "Fund", "Manager", "Shares Sold", "Value Reduced",
                          "Remaining Position", "Port. %", "Filing Date"]
    display_df["Value Reduced"] = display_df["Value Reduced"].apply(fmt_money)
    display_df["Remaining Position"] = display_df["Remaining Position"].apply(fmt_money)
    display_df["Port. %"] = display_df["Port. %"].apply(lambda x: f"{x:.1f}%")
    display_df["Shares Sold"] = display_df["Shares Sold"].apply(lambda x: f"{x:,}")
    display_df.index = range(1, len(display_df) + 1)

    st.dataframe(display_df, use_container_width=True, height=600)


# ─── PAGE: Top Funds by AUM ─────────────────────────────────────────────────

elif page == "🏆 Top Funds by AUM":
    render_logo()
    st.markdown("""
        <div class="section-header">🏆 Hedge Funds Ranked by 13F Portfolio Value
            <span class="badge badge-new">REAL DATA</span>
        </div>
        <p style="color:#9ca3af;font-size:0.88rem;margin-bottom:1rem;">
            All tracked hedge funds ranked by their total 13F filing portfolio value (from SEC EDGAR).
        </p>
    """, unsafe_allow_html=True)

    with st.spinner("🔄 Fetching from SEC EDGAR..."):
        all_holdings, all_metadata = load_all_funds()

    if not all_metadata:
        st.error("⚠️ Could not load data.")
        st.stop()

    fund_list = get_fund_list_with_aum(all_metadata)

    if not fund_list.empty:
        # Bar chart
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=fund_list["name"],
            y=fund_list["aum"],
            marker=dict(
                color=fund_list["aum"],
                colorscale=[[0, "#1e3a5f"], [0.5, "#3b82f6"], [1, "#8b5cf6"]],
                line=dict(width=0),
                cornerradius=6,
            ),
            text=[fmt_money(v) for v in fund_list["aum"]],
            textposition="outside",
            textfont=dict(color="#e5e7eb", size=10, family="Inter"),
        ))
        fig.update_layout(
            title="13F Portfolio Value",
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter", color="#9ca3af"),
            title_font=dict(size=16, color="#e5e7eb"),
            xaxis=dict(tickangle=-45, showgrid=False),
            yaxis=dict(showgrid=True, gridcolor="rgba(42,49,66,0.5)", zeroline=False),
            margin=dict(t=50, b=140, l=60, r=20),
            height=500,
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

        # Fund cards
        for idx, (_, fund) in enumerate(fund_list.iterrows()):
            st.markdown(f"""
                <div class="fund-card">
                    <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:0.5rem;">
                        <div style="min-width:200px;">
                            <div style="display:flex;align-items:center;gap:0.5rem;">
                                <span style="color:#6b7280;font-size:0.82rem;font-weight:600;">#{idx+1}</span>
                                <span class="fund-name">{fund['name']}</span>
                            </div>
                            <div class="fund-manager">{fund['manager']} · {fund['strategy']} · {fund['location']}</div>
                        </div>
                        <div style="display:flex;gap:2rem;align-items:center;flex-wrap:wrap;">
                            <div style="text-align:center;">
                                <div style="font-size:0.68rem;color:#6b7280;text-transform:uppercase;letter-spacing:0.06em;">
                                    13F Value</div>
                                <div class="fund-aum">{fmt_money(fund['aum'])}</div>
                            </div>
                            <div style="text-align:center;">
                                <div style="font-size:0.68rem;color:#6b7280;text-transform:uppercase;letter-spacing:0.06em;">
                                    Holdings</div>
                                <div style="font-size:1.1rem;font-weight:700;color:#8b5cf6;">
                                    {fund['num_holdings']}</div>
                            </div>
                            <div style="text-align:center;">
                                <div style="font-size:0.68rem;color:#6b7280;text-transform:uppercase;letter-spacing:0.06em;">
                                    Report Date</div>
                                <div style="font-size:0.9rem;font-weight:600;color:#9ca3af;">
                                    {fund['report_date'] or 'N/A'}</div>
                            </div>
                        </div>
                    </div>
                </div>
            """, unsafe_allow_html=True)


# ─── PAGE: Fund Explorer ─────────────────────────────────────────────────────

elif page == "📊 Fund Explorer":
    render_logo()
    st.markdown('<div class="section-header">📊 Fund Explorer — Real 13F Holdings</div>',
               unsafe_allow_html=True)

    fund_names = sorted(TRACKED_FUNDS.keys())
    selected_fund = st.selectbox("Select a Hedge Fund", fund_names, index=fund_names.index("Berkshire Hathaway") if "Berkshire Hathaway" in fund_names else 0)

    fund_info = TRACKED_FUNDS[selected_fund]

    with st.spinner(f"🔄 Fetching {selected_fund} 13F filing from SEC EDGAR..."):
        holdings, metadata = load_single_fund(selected_fund)

    if holdings.empty:
        st.warning(f"⚠️ Could not load 13F data for {selected_fund}. The fund may not have recent filings or the CIK may need verification.")
        st.stop()

    # Fund info header
    total_val = metadata.get("total_value", 0)
    num_hold = metadata.get("num_holdings", 0)
    filing_date = metadata.get("filing_date", "N/A")
    report_date = metadata.get("report_date", "N/A")

    st.markdown(f"""
        <div style="background:linear-gradient(135deg,var(--bg-card) 0%,rgba(59,130,246,0.08) 100%);
                    border:1px solid var(--border-color);border-radius:16px;padding:1.5rem 2rem;margin:1rem 0;">
            <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:1rem;">
                <div>
                    <div style="font-size:1.6rem;font-weight:800;color:#e5e7eb;">{selected_fund}</div>
                    <div style="color:#9ca3af;font-size:0.9rem;">
                        Managed by <span style="color:#3b82f6;font-weight:600;">{fund_info['manager']}</span>
                        · {fund_info['strategy']} · {fund_info['location']}
                    </div>
                    <div style="color:#6b7280;font-size:0.82rem;margin-top:0.3rem;">
                        Filed: {filing_date} · Report: {report_date} · Founded {fund_info['founded']}
                    </div>
                </div>
                <div style="display:flex;gap:1.5rem;">
                    <div class="stat-card" style="padding:0.8rem 1.2rem;">
                        <div class="stat-label">13F Value</div>
                        <div class="stat-value" style="font-size:1.4rem;">{fmt_money(total_val)}</div>
                    </div>
                    <div class="stat-card" style="padding:0.8rem 1.2rem;">
                        <div class="stat-label">Holdings</div>
                        <div style="font-size:1.4rem;font-weight:800;color:#8b5cf6;">{num_hold}</div>
                    </div>
                </div>
            </div>
        </div>
    """, unsafe_allow_html=True)

    tab_holdings, tab_changes = st.tabs(["💼 All Holdings", "📊 Changes from Prior Quarter"])

    with tab_holdings:
        # Portfolio heatmap
        top_holdings = holdings.head(50)  # Top 50 for treemap
        if len(top_holdings) > 0:
            fig = px.treemap(
                top_holdings,
                path=["name_of_issuer"],
                values="value",
                color="change_pct",
                color_continuous_scale=["#ef4444", "#1a1f2e", "#10b981"],
                color_continuous_midpoint=0,
                title=f"{selected_fund} — Portfolio Heatmap (size = value, color = Q/Q change %)",
            )
            fig.update_traces(
                texttemplate="<b>%{label}</b><br>%{value:$,.0s}",
                textfont=dict(size=12),
            )
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="Inter", color="#e5e7eb"),
                title_font_size=14,
                margin=dict(t=50, l=10, r=10, b=10),
                height=500,
                coloraxis_colorbar=dict(title="Change %", ticksuffix="%"),
            )
            st.plotly_chart(fig, use_container_width=True)

        # Full holdings table
        display_h = holdings[["name_of_issuer", "cusip", "title_of_class", "shares", "value",
                              "portfolio_pct"]].copy()
        display_h.columns = ["Issuer", "CUSIP", "Class", "Shares", "Value ($)", "Port. %"]
        display_h["Value ($)"] = display_h["Value ($)"].apply(fmt_money)
        display_h["Port. %"] = display_h["Port. %"].apply(lambda x: f"{x:.2f}%")
        display_h["Shares"] = display_h["Shares"].apply(lambda x: f"{x:,}")
        display_h.index = range(1, len(display_h) + 1)

        st.dataframe(display_h, use_container_width=True, height=600)

    with tab_changes:
        # Show buys and sells
        buys = holdings[holdings["change_shares"] > 0].copy()
        sells = holdings[holdings["change_shares"] < 0].copy()
        new_positions = holdings[holdings["action"] == "New"].copy()

        col_stats = st.columns(4)
        with col_stats[0]:
            st.metric("Positions Increased", len(buys))
        with col_stats[1]:
            st.metric("Positions Decreased", len(sells))
        with col_stats[2]:
            st.metric("New Positions", len(new_positions))
        with col_stats[3]:
            st.metric("Total Holdings", len(holdings))

        if not buys.empty or not sells.empty:
            # Changes bar chart - top buys by value change
            changes_df = holdings[holdings["change_value"] != 0].copy()
            changes_df = changes_df.nlargest(20, "change_value", keep="first")

            if not changes_df.empty:
                fig = go.Figure()
                colors = ["#10b981" if v > 0 else "#ef4444" for v in changes_df["change_value"]]
                fig.add_trace(go.Bar(
                    x=changes_df["name_of_issuer"],
                    y=changes_df["change_value"],
                    marker_color=colors,
                    text=[fmt_money(abs(v)) for v in changes_df["change_value"]],
                    textposition="outside",
                    textfont=dict(color="#e5e7eb", size=9),
                ))
                plotly_dark_layout(fig, "Largest Position Changes (Value $)", 400)
                fig.update_layout(xaxis=dict(tickangle=-45), margin=dict(b=120))
                st.plotly_chart(fig, use_container_width=True)

        # Changes table
        changes_display = holdings[["name_of_issuer", "shares", "prev_shares", "change_shares",
                                    "change_pct", "value", "change_value", "action"]].copy()
        changes_display.columns = ["Issuer", "Current Shares", "Prev Shares", "Change",
                                   "Change %", "Current Value", "Value Change", "Action"]
        changes_display["Current Value"] = changes_display["Current Value"].apply(fmt_money)
        changes_display["Value Change"] = changes_display["Value Change"].apply(lambda x: f"+{fmt_money(x)}" if x > 0 else f"-{fmt_money(abs(x))}" if x < 0 else "$0")
        changes_display["Change %"] = changes_display["Change %"].apply(lambda x: f"{x:+.1f}%")
        changes_display["Current Shares"] = changes_display["Current Shares"].apply(lambda x: f"{x:,}")
        changes_display["Prev Shares"] = changes_display["Prev Shares"].apply(lambda x: f"{x:,}")
        changes_display["Change"] = changes_display["Change"].apply(lambda x: f"{x:+,}")
        changes_display.index = range(1, len(changes_display) + 1)

        st.dataframe(changes_display, use_container_width=True, height=600)


# ─── PAGE: Hot Stocks ────────────────────────────────────────────────────────

elif page == "🔥 Hot Stocks":
    render_logo()
    st.markdown("""
        <div class="section-header">🔥 Hot Stocks
            <span class="badge badge-hot">TRENDING ACROSS FUNDS</span>
        </div>
        <p style="color:#9ca3af;font-size:0.88rem;margin-bottom:1rem;">
            Stocks with the most hedge fund activity — held, bought, or sold by multiple tracked funds.
        </p>
    """, unsafe_allow_html=True)

    with st.spinner("🔄 Fetching from SEC EDGAR..."):
        all_holdings, all_metadata = load_all_funds()

    if not all_holdings:
        st.error("⚠️ Could not load data.")
        st.stop()

    hot = compute_hot_stocks(all_holdings, all_metadata)

    if hot.empty:
        st.info("No data available.")
        st.stop()

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Unique Securities", len(hot))
    with col2:
        bullish = len(hot[hot["sentiment"] == "Bullish"])
        st.metric("Bullish Consensus", bullish)
    with col3:
        bearish = len(hot[hot["sentiment"] == "Bearish"])
        st.metric("Bearish Consensus", bearish)

    # Bubble chart
    top_hot = hot.head(30)
    if not top_hot.empty:
        fig = px.scatter(
            top_hot, x="funds_buying", y="funds_selling",
            size="total_value", color="sentiment",
            color_discrete_map={"Bullish": "#10b981", "Bearish": "#ef4444", "Neutral": "#6b7280"},
            hover_data=["name_of_issuer", "funds_holding"],
            text="name_of_issuer",
            title="Fund Buy vs Sell Activity",
            labels={"funds_buying": "Funds Buying", "funds_selling": "Funds Selling"},
        )
        fig.update_traces(textposition="top center", textfont_size=8)
        plotly_dark_layout(fig, "Fund Buy vs Sell Activity", 500)
        fig.update_layout(
            xaxis=dict(showgrid=True, gridcolor="rgba(42,49,66,0.5)"),
        )
        st.plotly_chart(fig, use_container_width=True)

    # Table
    display_hot = hot[["name_of_issuer", "cusip", "funds_holding", "funds_buying",
                       "funds_selling", "total_value", "net_shares", "sentiment", "holders_str"]].head(50).copy()
    display_hot.columns = ["Issuer", "CUSIP", "Funds Holding", "Buying", "Selling",
                           "Total Value", "Net Shares", "Sentiment", "Top Holders"]
    display_hot["Total Value"] = display_hot["Total Value"].apply(fmt_money)
    display_hot["Net Shares"] = display_hot["Net Shares"].apply(lambda x: f"{x:+,}")
    display_hot.index = range(1, len(display_hot) + 1)

    st.dataframe(display_hot, use_container_width=True, height=600)


# ─── PAGE: Consensus Buys ───────────────────────────────────────────────────

elif page == "✅ Consensus Buys":
    render_logo()
    st.markdown("""
        <div class="section-header">✅ Consensus Buys
            <span class="badge badge-buy">BULLISH</span>
        </div>
        <p style="color:#9ca3af;font-size:0.88rem;margin-bottom:1rem;">
            Stocks where the majority of tracked hedge funds are increasing their positions.
        </p>
    """, unsafe_allow_html=True)

    with st.spinner("🔄 Fetching from SEC EDGAR..."):
        all_holdings, all_metadata = load_all_funds()

    if not all_holdings:
        st.error("⚠️ Could not load data.")
        st.stop()

    hot = compute_hot_stocks(all_holdings, all_metadata)
    consensus = hot[hot["sentiment"] == "Bullish"].sort_values("funds_buying", ascending=False)

    if consensus.empty:
        st.info("No consensus buy picks this period.")
        st.stop()

    fig = px.bar(
        consensus.head(20), x="name_of_issuer", y="funds_buying",
        color="funds_buying",
        color_continuous_scale=["#1a3a2e", "#10b981", "#34d399"],
        text="funds_buying",
        title="Number of Funds Buying",
    )
    fig.update_traces(textposition="outside", textfont=dict(color="#e5e7eb"))
    plotly_dark_layout(fig, "Number of Funds Buying", 400)
    fig.update_layout(xaxis=dict(tickangle=-45), margin=dict(b=120), coloraxis_showscale=False, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    display_c = consensus[["name_of_issuer", "cusip", "funds_buying", "funds_selling",
                           "total_value", "net_shares", "holders_str"]].head(30).copy()
    display_c.columns = ["Issuer", "CUSIP", "Funds Buying", "Funds Selling",
                         "Total Value", "Net Shares", "Top Holders"]
    display_c["Total Value"] = display_c["Total Value"].apply(fmt_money)
    display_c["Net Shares"] = display_c["Net Shares"].apply(lambda x: f"{x:+,}")
    display_c.index = range(1, len(display_c) + 1)

    st.dataframe(display_c, use_container_width=True, height=500)


# ─── PAGE: Consensus Sells ──────────────────────────────────────────────────

elif page == "❌ Consensus Sells":
    render_logo()
    st.markdown("""
        <div class="section-header">❌ Consensus Sells
            <span class="badge badge-sell">BEARISH</span>
        </div>
        <p style="color:#9ca3af;font-size:0.88rem;margin-bottom:1rem;">
            Stocks where the majority of tracked hedge funds are reducing their positions.
        </p>
    """, unsafe_allow_html=True)

    with st.spinner("🔄 Fetching from SEC EDGAR..."):
        all_holdings, all_metadata = load_all_funds()

    if not all_holdings:
        st.error("⚠️ Could not load data.")
        st.stop()

    hot = compute_hot_stocks(all_holdings, all_metadata)
    consensus_sell = hot[hot["sentiment"] == "Bearish"].sort_values("funds_selling", ascending=False)

    if consensus_sell.empty:
        st.info("No consensus sell picks this period.")
        st.stop()

    fig = px.bar(
        consensus_sell.head(20), x="name_of_issuer", y="funds_selling",
        color="funds_selling",
        color_continuous_scale=["#3a1a1a", "#ef4444", "#f87171"],
        text="funds_selling",
        title="Number of Funds Selling",
    )
    fig.update_traces(textposition="outside", textfont=dict(color="#e5e7eb"))
    plotly_dark_layout(fig, "Number of Funds Selling", 400)
    fig.update_layout(xaxis=dict(tickangle=-45), margin=dict(b=120), coloraxis_showscale=False, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    display_cs = consensus_sell[["name_of_issuer", "cusip", "funds_buying", "funds_selling",
                                "total_value", "net_shares", "holders_str"]].head(30).copy()
    display_cs.columns = ["Issuer", "CUSIP", "Funds Buying", "Funds Selling",
                          "Total Value", "Net Shares", "Top Holders"]
    display_cs["Total Value"] = display_cs["Total Value"].apply(fmt_money)
    display_cs["Net Shares"] = display_cs["Net Shares"].apply(lambda x: f"{x:+,}")
    display_cs.index = range(1, len(display_cs) + 1)

    st.dataframe(display_cs, use_container_width=True, height=500)


# ─── PAGE: Insider Trades ───────────────────────────────────────────────────

elif page == "👥 Insider Trades":
    render_logo()
    st.markdown("""
        <div class="section-header">👥 Insider Trading Intelligence
            <span class="badge badge-live">REAL-TIME SEC FORM 4</span>
        </div>
        <p style="color:#9ca3af;font-size:0.88rem;margin-bottom:1.5rem;">
            Real-time tracking of corporate insiders (CEOs, CFOs, Directors) buying and selling their own company stock.
            Sourced directly from SEC Form 4 filings.
        </p>
    """, unsafe_allow_html=True)

    with st.spinner("🔄 Fetching latest Form 4 filings from SEC EDGAR..."):
        # We cache this for 30 mins in Streamlit to keep it fresh
        @st.cache_data(ttl=1800)
        def get_insider_data():
            try:
                data = fetch_recent_insider_trades(limit=100)
                return data
            except Exception as e:
                st.error(f"Error fetching insider data: {e}")
                return pd.DataFrame()

        insider_df = get_insider_data()

    if insider_df.empty:
        st.warning("⚠️ No recent insider trades found. This could be due to SEC maintenance or a temporary API issue.")
        st.stop()

    # Metrics & Sentiment
    buys_df = insider_df[insider_df["transaction_type"] == "Buy"]
    sells_df = insider_df[insider_df["transaction_type"] == "Sell"]

    total_buy_val = buys_df["value"].sum()
    total_sell_val = sells_df["value"].sum()
    sentiment_ratio = total_buy_val / (total_sell_val + 1) # Avoid div by zero

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Total Buys (7d)", fmt_money(total_buy_val), f"{len(buys_df)} trades")
    with m2:
        st.metric("Total Sells (7d)", fmt_money(total_sell_val), f"{len(sells_df)} trades")
    with m3:
        sentiment_label = "Bullish" if sentiment_ratio > 1.2 else "Bearish" if sentiment_ratio < 0.8 else "Neutral"
        sentiment_color = "#10b981" if sentiment_label == "Bullish" else "#ef4444" if sentiment_label == "Bearish" else "#9ca3af"
        st.markdown(f"""
            <div style="background:var(--bg-card);border:1px solid var(--border-color);border-radius:12px;padding:1.15rem;text-align:center;">
                <div style="font-size:0.75rem;color:var(--text-muted);font-weight:600;text-transform:uppercase;margin-bottom:0.2rem;">Market Sentiment</div>
                <div style="font-size:1.4rem;font-weight:800;color:{sentiment_color};">{sentiment_label}</div>
            </div>
        """, unsafe_allow_html=True)
    with m4:
        st.metric("Top Ticker", insider_df["ticker"].mode()[0] if not insider_df.empty else "N/A")

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    # Tabs for Visuals vs Table
    tab_feed, tab_stats = st.tabs(["🗞️ Live Feed", "📊 Aggregate Stats"])

    with tab_feed:
        # Filter controls
        f1, f2 = st.columns([1, 2])
        with f1:
            trade_filter = st.radio("Show", ["All", "Buys Only", "Sells Only"], horizontal=True)

        filtered_df = insider_df
        if trade_filter == "Buys Only": filtered_df = buys_df
        if trade_filter == "Sells Only": filtered_df = sells_df

        # Display as cards
        for _, row in filtered_df.head(40).iterrows():
            is_buy = row["transaction_type"] == "Buy"
            color = "#10b981" if is_buy else "#ef4444"
            bg_glow = "rgba(16,185,129,0.05)" if is_buy else "rgba(239,68,68,0.05)"

            st.markdown(f"""
                <div style="background:{bg_glow};border:1px solid var(--border-color);border-left:4px solid {color};
                            border-radius:12px;padding:1.2rem;margin-bottom:0.8rem;">
                    <div style="display:flex;justify-content:space-between;align-items:start;">
                        <div>
                            <div style="display:flex;align-items:center;gap:0.6rem;">
                                <span style="font-size:1.1rem;font-weight:800;color:#e5e7eb;">{row['ticker']}</span>
                                <span class="badge {'badge-buy' if is_buy else 'badge-sell'}">{row['transaction_type']}</span>
                                <span style="font-size:0.78rem;color:#6b7280;">{row['issuer_name']}</span>
                            </div>
                            <div style="margin-top:0.4rem;font-size:0.95rem;font-weight:600;color:#3b82f6;">
                                {row['owner_name']}
                            </div>
                            <div style="font-size:0.75rem;color:#9ca3af;margin-top:0.1rem;">
                                {row['relationship']}
                            </div>
                        </div>
                        <div style="text-align:right;">
                            <div style="font-size:1.1rem;font-weight:800;color:{color};">
                                {fmt_money(row['value'])}
                            </div>
                            <div style="font-size:0.72rem;color:#6b7280;">
                                {fmt_number(int(row['shares']))} shares @ ${row['price']:.2f}
                            </div>
                            <div style="font-size:0.68rem;color:#4b5563;margin-top:0.3rem;">
                                Transacted: {row['transaction_date']}
                            </div>
                        </div>
                    </div>
                </div>
            """, unsafe_allow_html=True)

    with tab_stats:
        # Ticker breakdown
        ticker_val = insider_df.groupby("ticker")["value"].sum().nlargest(15).reset_index()

        fig = px.bar(
            ticker_val, x="ticker", y="value",
            color="value",
            color_continuous_scale="Blues",
            title="Highest Transaction Volume by Ticker (7d)",
            labels={"value": "Total Value ($)", "ticker": "Stock Ticker"},
        )
        plotly_dark_layout(fig, height=400)
        st.plotly_chart(fig, use_container_width=True)

        # Full table
        st.markdown("### All Insider Transactions")
        st_df = insider_df[["ticker", "owner_name", "relationship", "transaction_type", "shares", "price", "value", "transaction_date"]].copy()
        st_df.columns = ["Ticker", "Insider Name", "Role", "Type", "Shares", "Price", "Total Value", "Date"]
        st_df["Total Value"] = st_df["Total Value"].apply(fmt_money)
        st_df["Price"] = st_df["Price"].apply(lambda x: f"${x:.2f}")
        st_df["Shares"] = st_df["Shares"].apply(lambda x: f"{x:,.0f}")
        st.dataframe(st_df, use_container_width=True, height=500)


# ─── PAGE: Fund Search ───────────────────────────────────────────────────────

elif page == "🔍 Fund Search":
    render_logo()
    st.markdown("""
        <div class="section-header">🔍 Hedge Fund Search
            <span class="badge badge-new">SEARCH ANY 13F FILER</span>
        </div>
        <p style="color:#9ca3af;font-size:0.88rem;margin-bottom:1rem;">
            Search for any hedge fund, investment manager, or institution that files 13F-HR reports with the SEC.
            Type a name to see auto-suggestions, then explore their full portfolio.
        </p>
    """, unsafe_allow_html=True)

    # ── Search input ──
    search_col, btn_col = st.columns([4, 1])
    with search_col:
        search_query = st.text_input(
            "Search for a hedge fund or institution",
            placeholder="e.g. Vanguard, BlackRock, Goldman Sachs, Ark Invest...",
            key="fund_search_input",
        )
    with btn_col:
        st.markdown("<div style='height:1.65rem'></div>", unsafe_allow_html=True)
        search_clicked = st.button("🔍 Search", use_container_width=True, type="primary")

    # ── Auto-suggest: show results as user types ──
    if search_query and len(search_query.strip()) >= 2:
        with st.spinner("Searching SEC EDGAR..."):
            results = search_13f_filers(search_query, max_results=15)

        if results:
            st.markdown(f"""
                <div style="margin:0.5rem 0 0.3rem 0;font-size:0.82rem;color:#6b7280;font-weight:600;">
                    Found {len(results)} 13F filer(s) matching \"{search_query}\"</div>
            """, unsafe_allow_html=True)

            # Show results as selectable options
            options = []
            option_map = {}
            for r in results:
                tracked_label = " ⭐ TRACKED" if r["is_tracked"] else ""
                label = f"{r['entity_name']}{tracked_label}  (CIK: {r['cik']})"
                options.append(label)
                option_map[label] = r

            selected_result = st.selectbox(
                "Select a fund to explore",
                options=["— Select a fund —"] + options,
                key="fund_search_select",
            )

            if selected_result != "— Select a fund —" and selected_result in option_map:
                selected_data = option_map[selected_result]
                cik = selected_data["cik"]
                entity_name = selected_data["entity_name"]

                st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

                with st.spinner(f"🔄 Loading 13F holdings for {entity_name}..."):
                    holdings, metadata = explore_fund_by_cik(cik, entity_name)

                if holdings.empty:
                    st.warning(f"⚠️ No 13F holdings data found for {entity_name} (CIK: {cik}).")
                else:
                    fund_name = metadata.get("fund_name", entity_name)
                    total_val = metadata.get("total_value", 0)
                    num_hold = metadata.get("num_holdings", 0)
                    filing_date = metadata.get("filing_date", "N/A")
                    report_date = metadata.get("report_date", "N/A")
                    manager = metadata.get("manager", "")
                    strategy = metadata.get("strategy", "")
                    location = metadata.get("location", "")

                    info_parts = []
                    if manager:
                        info_parts.append(f'Managed by <span style="color:#3b82f6;font-weight:600;">{manager}</span>')
                    if strategy:
                        info_parts.append(strategy)
                    if location:
                        info_parts.append(location)
                    info_str = " · ".join(info_parts) if info_parts else f"CIK: {cik}"

                    st.markdown(f"""
                        <div style="background:linear-gradient(135deg,var(--bg-card) 0%,rgba(59,130,246,0.08) 100%);
                                    border:1px solid var(--border-color);border-radius:16px;padding:1.5rem 2rem;margin:1rem 0;">
                            <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:1rem;">
                                <div>
                                    <div style="font-size:1.5rem;font-weight:800;color:#e5e7eb;">{fund_name}</div>
                                    <div style="color:#9ca3af;font-size:0.88rem;">{info_str}</div>
                                    <div style="color:#6b7280;font-size:0.8rem;margin-top:0.3rem;">
                                        Filed: {filing_date} · Report: {report_date} · CIK: {cik}</div>
                                </div>
                                <div style="display:flex;gap:1.5rem;">
                                    <div class="stat-card" style="padding:0.8rem 1.2rem;">
                                        <div class="stat-label">13F Value</div>
                                        <div class="stat-value" style="font-size:1.4rem;">{fmt_money(total_val)}</div>
                                    </div>
                                    <div class="stat-card" style="padding:0.8rem 1.2rem;">
                                        <div class="stat-label">Holdings</div>
                                        <div style="font-size:1.4rem;font-weight:800;color:#8b5cf6;">{num_hold}</div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    """, unsafe_allow_html=True)

                    tab_h, tab_c = st.tabs(["💼 All Holdings", "📊 Quarter-over-Quarter Changes"])

                    with tab_h:
                        # Portfolio treemap
                        top_h = holdings.head(50)
                        if len(top_h) > 0:
                            fig = px.treemap(
                                top_h, path=["name_of_issuer"], values="value",
                                color="change_pct",
                                color_continuous_scale=["#ef4444", "#1a1f2e", "#10b981"],
                                color_continuous_midpoint=0,
                                title=f"{fund_name} — Portfolio Heatmap",
                            )
                            fig.update_traces(
                                texttemplate="<b>%{label}</b><br>%{value:$,.0s}",
                                textfont=dict(size=11),
                            )
                            fig.update_layout(
                                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                                font=dict(family="Inter", color="#e5e7eb"),
                                title_font_size=14,
                                margin=dict(t=50, l=10, r=10, b=10),
                                height=480,
                                coloraxis_colorbar=dict(title="Change %", ticksuffix="%"),
                            )
                            st.plotly_chart(fig, use_container_width=True)

                        # Holdings table
                        disp = holdings[["name_of_issuer", "cusip", "title_of_class",
                                         "shares", "value", "portfolio_pct"]].copy()
                        disp.columns = ["Issuer", "CUSIP", "Class", "Shares", "Value ($)", "Port. %"]
                        disp["Value ($)"] = disp["Value ($)"].apply(fmt_money)
                        disp["Port. %"] = disp["Port. %"].apply(lambda x: f"{x:.2f}%")
                        disp["Shares"] = disp["Shares"].apply(lambda x: f"{x:,}")
                        disp.index = range(1, len(disp) + 1)
                        st.dataframe(disp, use_container_width=True, height=600)

                    with tab_c:
                        buys_s = holdings[holdings["change_shares"] > 0]
                        sells_s = holdings[holdings["change_shares"] < 0]
                        new_s = holdings[holdings["action"] == "New"]

                        cs1, cs2, cs3, cs4 = st.columns(4)
                        with cs1:
                            st.metric("Increased", len(buys_s))
                        with cs2:
                            st.metric("Decreased", len(sells_s))
                        with cs3:
                            st.metric("New", len(new_s))
                        with cs4:
                            st.metric("Total", len(holdings))

                        # Changes chart
                        chg = holdings[holdings["change_value"] != 0].copy()
                        chg = chg.nlargest(20, "change_value", keep="first")
                        if not chg.empty:
                            fig2 = go.Figure()
                            colors = ["#10b981" if v > 0 else "#ef4444" for v in chg["change_value"]]
                            fig2.add_trace(go.Bar(
                                x=chg["name_of_issuer"], y=chg["change_value"],
                                marker_color=colors,
                                text=[fmt_money(abs(v)) for v in chg["change_value"]],
                                textposition="outside",
                                textfont=dict(color="#e5e7eb", size=9),
                            ))
                            plotly_dark_layout(fig2, "Largest Position Changes ($)", 400)
                            fig2.update_layout(xaxis=dict(tickangle=-45), margin=dict(b=120))
                            st.plotly_chart(fig2, use_container_width=True)

                        # Changes table
                        chg_disp = holdings[["name_of_issuer", "shares", "prev_shares",
                                             "change_shares", "change_pct", "value",
                                             "change_value", "action"]].copy()
                        chg_disp.columns = ["Issuer", "Current Shares", "Prev Shares",
                                            "Change", "Change %", "Value", "Value Chg", "Action"]
                        chg_disp["Value"] = chg_disp["Value"].apply(fmt_money)
                        chg_disp["Value Chg"] = chg_disp["Value Chg"].apply(
                            lambda x: f"+{fmt_money(x)}" if x > 0 else f"-{fmt_money(abs(x))}" if x < 0 else "$0")
                        chg_disp["Change %"] = chg_disp["Change %"].apply(lambda x: f"{x:+.1f}%")
                        chg_disp["Current Shares"] = chg_disp["Current Shares"].apply(lambda x: f"{x:,}")
                        chg_disp["Prev Shares"] = chg_disp["Prev Shares"].apply(lambda x: f"{x:,}")
                        chg_disp["Change"] = chg_disp["Change"].apply(lambda x: f"{x:+,}")
                        chg_disp.index = range(1, len(chg_disp) + 1)
                        st.dataframe(chg_disp, use_container_width=True, height=600)

        else:
            st.info(f'No 13F filers found matching "{search_query}". Try a different name (e.g. "Vanguard", "BlackRock", "Fidelity").')

    elif not search_query:
        # Show popular suggestions when no search query
        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
        st.markdown("""<div class="section-header">💡 Popular Searches</div>""", unsafe_allow_html=True)

        suggestions = [
            ("BlackRock", "World's largest asset manager"),
            ("Vanguard", "Index fund pioneer"),
            ("Fidelity", "Major mutual fund company"),
            ("Goldman Sachs", "Global investment bank"),
            ("Morgan Stanley", "Financial services giant"),
            ("ARK Invest", "Cathie Wood's innovation funds"),
            ("Paulson & Co", "Macro hedge fund"),
            ("Hillhouse Capital", "Asia-focused fund"),
        ]

        cols = st.columns(4)
        for i, (name, desc) in enumerate(suggestions):
            with cols[i % 4]:
                st.markdown(f"""
                    <div class="fund-card" style="text-align:center;padding:1rem;">
                        <div style="font-size:1rem;font-weight:700;color:#e5e7eb;">{name}</div>
                        <div style="font-size:0.75rem;color:#6b7280;margin-top:0.3rem;">{desc}</div>
                        <div style="font-size:0.7rem;color:#3b82f6;margin-top:0.4rem;">Type name above to search →</div>
                    </div>
                """, unsafe_allow_html=True)

        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

        # Also show all tracked funds for quick access
        st.markdown("""<div class="section-header">⭐ Pre-Tracked Funds (Quick Access)</div>""", unsafe_allow_html=True)
        st.markdown("""<p style="color:#6b7280;font-size:0.82rem;margin-bottom:0.8rem;">
            These 25 funds are pre-indexed for faster loading. Search above to explore any other 13F filer.</p>""",
            unsafe_allow_html=True)

        tracked_cols = st.columns(3)
        for i, (fname, fdata) in enumerate(sorted(TRACKED_FUNDS.items())):
            with tracked_cols[i % 3]:
                st.markdown(f"""
                    <div class="fund-card" style="padding:0.8rem 1rem;">
                        <div style="display:flex;justify-content:space-between;align-items:center;">
                            <div>
                                <div style="font-weight:600;color:#e5e7eb;font-size:0.88rem;">{fname}</div>
                                <div style="color:#6b7280;font-size:0.72rem;">{fdata['manager']} · {fdata['strategy']}</div>
                            </div>
                            <span class="badge badge-live" style="font-size:0.6rem;">TRACKED</span>
                        </div>
                    </div>
                """, unsafe_allow_html=True)


# ─── Footer ──────────────────────────────────────────────────────────────────

st.markdown("""
    <div class="footer">
        <div style="font-size:1rem;font-weight:700;margin-bottom:0.5rem;
                    background:linear-gradient(135deg,#3b82f6,#8b5cf6);
                    -webkit-background-clip:text;-webkit-text-fill-color:transparent;
                    background-clip:text;">📊 HedgeFlow</div>
        <div>Real SEC EDGAR 13F Filing Intelligence</div>
        <div style="margin-top:0.3rem;">
            Data sourced directly from SEC EDGAR · 13F-HR filings · Values in USD
        </div>
        <div style="margin-top:0.3rem;color:#374151;">
            13F values are reported in thousands by the SEC and converted to full dollars here.
            For educational and informational purposes only — not investment advice.
        </div>
        <div style="margin-top:0.5rem;font-size:0.7rem;color:#374151;">
            © 2026 HedgeFlow · All rights reserved
        </div>
    </div>
""", unsafe_allow_html=True)
