import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date, datetime

# =============================================================================
# CONFIG
# =============================================================================

BQ_PROJECT = "yotam-395120"
FTUE_TABLE = f"{BQ_PROJECT}.peerplay.ftue_dashboard"
RETENTION_TABLE = f"{BQ_PROJECT}.peerplay.hint_system_ab_test_results"

TEST_START_DATE = date(2026, 3, 16)

DAYS_SINCE_INSTALL_BUCKET_ORDER = ['0-3', '3-7', '7-14', '14-21', '21-30', '30-60']

# --- Design System ---
COLORS = {
    'before':       '#5B8DEF',   # calm blue
    'after':        '#2ECB71',   # confident green
    'negative':     '#E74C3C',   # clear red
    'neutral':      '#95A5A6',   # soft gray
    'bg_card':      '#F8F9FA',   # card bg
    'accent':       '#8E44AD',   # purple accent
    'test_line':    '#E74C3C',   # test-start line
    'text_muted':   '#7F8C8D',
}

VERSION_PALETTE = [
    '#5B8DEF', '#F39C12', '#2ECB71', '#E74C3C', '#8E44AD',
    '#1ABC9C', '#E67E22', '#3498DB', '#9B59B6', '#2ECC71',
    '#E91E63', '#00BCD4', '#FF9800', '#607D8B',
]

PLOTLY_TEMPLATE = dict(
    paper_bgcolor='white',
    plot_bgcolor='#FAFBFC',
    font=dict(family='Inter, -apple-system, sans-serif', color='#2C3E50', size=13),
    title=dict(text='', font=dict(size=18, color='#2C3E50'), x=0, xanchor='left'),
    xaxis=dict(gridcolor='#ECF0F1', zerolinecolor='#ECF0F1'),
    yaxis=dict(gridcolor='#ECF0F1', zerolinecolor='#ECF0F1'),
    legend=dict(
        orientation='h', yanchor='bottom', y=1.04, xanchor='right', x=1,
        bgcolor='rgba(255,255,255,0.9)', bordercolor='#ECF0F1', borderwidth=1,
        font=dict(size=12),
    ),
    hoverlabel=dict(bgcolor='white', font_size=12, bordercolor='#BDC3C7'),
    margin=dict(l=60, r=30, t=70, b=60),
)


# =============================================================================
# CUSTOM CSS
# =============================================================================

def inject_css():
    st.markdown("""
    <style>
    /* --- Global --- */
    .stApp { background-color: #FAFBFC; }
    section[data-testid="stSidebar"] { background-color: #F0F2F6; }

    /* --- Metric cards --- */
    div[data-testid="stMetric"] {
        background: white;
        border: 1px solid #E8ECF0;
        border-radius: 12px;
        padding: 16px 20px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    }
    div[data-testid="stMetric"] label {
        color: #7F8C8D !important;
        font-size: 0.82rem !important;
        font-weight: 500 !important;
        text-transform: uppercase;
        letter-spacing: 0.4px;
    }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        font-size: 1.6rem !important;
        font-weight: 700 !important;
        color: #2C3E50 !important;
    }

    /* --- Tabs --- */
    button[data-baseweb="tab"] {
        font-weight: 600 !important;
        font-size: 0.92rem !important;
        padding: 10px 20px !important;
    }
    button[data-baseweb="tab"][aria-selected="true"] {
        border-bottom: 3px solid #5B8DEF !important;
        color: #5B8DEF !important;
    }

    /* --- Section dividers --- */
    hr { border: none; border-top: 1px solid #E8ECF0; margin: 2rem 0 !important; }

    /* --- Info boxes --- */
    .legend-box {
        background: white;
        border: 1px solid #E8ECF0;
        border-radius: 10px;
        padding: 14px 20px;
        margin: 12px 0 20px 0;
        font-size: 0.88rem;
        line-height: 1.7;
        color: #5D6D7E;
    }
    .legend-box strong { color: #2C3E50; }

    .summary-box {
        background: linear-gradient(135deg, #F8F9FA 0%, #EBF5FB 100%);
        border-left: 4px solid #5B8DEF;
        border-radius: 0 10px 10px 0;
        padding: 18px 24px;
        margin: 16px 0 24px 0;
        font-size: 0.92rem;
        line-height: 1.8;
        color: #2C3E50;
    }
    .summary-box h4 { margin: 0 0 8px 0; color: #2C3E50; }
    .tag-up { background: #D5F5E3; color: #1E8449; padding: 2px 8px; border-radius: 4px; font-weight: 600; font-size: 0.82rem; }
    .tag-down { background: #FADBD8; color: #C0392B; padding: 2px 8px; border-radius: 4px; font-weight: 600; font-size: 0.82rem; }
    .tag-flat { background: #EAECEE; color: #7F8C8D; padding: 2px 8px; border-radius: 4px; font-weight: 600; font-size: 0.82rem; }
    </style>
    """, unsafe_allow_html=True)


# =============================================================================
# DATA LOADING
# =============================================================================

@st.cache_resource(ttl=600)
def get_bq_client():
    try:
        if st.secrets and "gcp_service_account" in st.secrets:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"],
                scopes=["https://www.googleapis.com/auth/bigquery"],
            )
            return bigquery.Client(project=BQ_PROJECT, credentials=credentials)
    except Exception:
        pass
    return bigquery.Client(project=BQ_PROJECT)


@st.cache_data(ttl=600)
def load_ftue_data():
    client = get_bq_client()
    query = f"""
    SELECT * FROM `{FTUE_TABLE}`
    WHERE install_date >= '2026-02-01'
      AND SAFE_CAST(install_version AS FLOAT64) >= 0.38
      AND platform != 'none'
    """
    df = client.query(query).to_dataframe()
    if 'install_date' in df.columns:
        df['install_date'] = pd.to_datetime(df['install_date']).dt.date
    return df


@st.cache_data(ttl=600)
def load_retention_data(_client, start_date, end_date):
    query = f"""
    WITH cohort AS (
        SELECT
            dp.distinct_id,
            dp.install_date,
            CAST(dp.first_app_version AS STRING) AS app_version,
            CASE WHEN dp.first_country = 'US' THEN 1 ELSE 0 END AS is_usa,
            dp.first_platform AS platform
        FROM `yotam-395120.peerplay.dim_player` dp
        WHERE dp.install_date >= '{start_date}'
          AND dp.first_country NOT IN ('UA', 'IL', 'AM')
          AND dp.distinct_id NOT IN (SELECT distinct_id FROM `yotam-395120.peerplay.potential_fraudsters`)
    ),
    days_array AS (
        SELECT day_num FROM UNNEST(GENERATE_ARRAY(0, 90)) AS day_num
    ),
    cohort_days AS (
        SELECT c.*, d.day_num AS days_since_install,
            DATE_ADD(c.install_date, INTERVAL d.day_num DAY) AS activity_date
        FROM cohort c
        CROSS JOIN days_array d
        WHERE DATE_ADD(c.install_date, INTERVAL d.day_num DAY) < CURRENT_DATE()
    )
    SELECT
        cd.install_date AS date,
        cd.app_version,
        cd.platform,
        cd.is_usa,
        cd.days_since_install,
        COUNT(DISTINCT cd.distinct_id) AS cohort_size,
        COUNT(DISTINCT apd.distinct_id) AS users_active
    FROM cohort_days cd
    LEFT JOIN `yotam-395120.peerplay.agg_player_daily` apd
        ON cd.distinct_id = apd.distinct_id AND cd.activity_date = apd.date
    GROUP BY ALL
    """
    df = _client.query(query).to_dataframe()
    df['date'] = pd.to_datetime(df['date']).dt.date
    return df


# =============================================================================
# HELPERS
# =============================================================================

def get_pct_columns(df):
    return sorted([c for c in df.columns if c.startswith('pct_')])

def get_ratio_columns(df):
    return sorted([c for c in df.columns if c.startswith('ratio_')])

def format_step_label(col):
    parts = col.split('_', 2)
    if len(parts) >= 3:
        return f"{parts[1]}: {parts[2]}"
    elif len(parts) == 2:
        return parts[1]
    return col

def version_sort_key(v):
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0

def weighted_retention(df):
    filtered = df[df['users_active'] > 0]
    cs = filtered['cohort_size'].sum()
    if cs == 0:
        return 0.0
    return filtered['users_active'].sum() / cs

def fmt_pct(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return "N/A"
    return f"{val:.2%}"

def fmt_number(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return "N/A"
    return f"{val:,.0f}"

def fmt_money(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return "N/A"
    return f"${val:.4f}"

def get_version_color_map(versions):
    return {v: VERSION_PALETTE[i % len(VERSION_PALETTE)]
            for i, v in enumerate(sorted(versions, key=version_sort_key))}

def calc_weighted_steps(subset, metrics_list):
    tu = subset['total_users'].sum() if 'total_users' in subset.columns else len(subset)
    vals = []
    for m in metrics_list:
        if 'total_users' in subset.columns and tu > 0:
            val = (subset[m] * subset['total_users']).sum() / tu
        else:
            val = subset[m].mean() if not subset.empty else 0
        vals.append(val)
    return vals, tu

def add_test_start_line(fig):
    """Add a consistent test-start marker to any time-series chart."""
    fig.add_shape(
        type="line",
        x0=str(TEST_START_DATE), x1=str(TEST_START_DATE),
        y0=0, y1=1, yref="paper",
        line=dict(dash="dash", color=COLORS['test_line'], width=2),
    )
    fig.add_annotation(
        x=str(TEST_START_DATE), y=1.06, yref="paper",
        text="TEST START", showarrow=False,
        font=dict(color=COLORS['test_line'], size=11, weight='bold'),
        bgcolor='rgba(231,76,60,0.08)', borderpad=4,
    )

def apply_chart_theme(fig, **overrides):
    """Apply consistent theme to all charts."""
    layout = {**PLOTLY_TEMPLATE}
    layout.update(overrides)
    fig.update_layout(**layout)

def delta_tag(val, fmt="+.1f", suffix="%"):
    """Return an HTML tag for a delta value."""
    if val is None:
        return '<span class="tag-flat">N/A</span>'
    if val > 0.001:
        return f'<span class="tag-up">{val:{fmt}}{suffix}</span>'
    elif val < -0.001:
        return f'<span class="tag-down">{val:{fmt}}{suffix}</span>'
    return f'<span class="tag-flat">{val:{fmt}}{suffix}</span>'


# =============================================================================
# MAIN
# =============================================================================

def main():
    st.set_page_config(
        page_title="Hint System A/B Test Dashboard",
        page_icon="🔬",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    inject_css()

    st.markdown("## Hint System A/B Test Dashboard")
    st.caption(f"Comparing FTUE funnel & retention before vs after test start ({TEST_START_DATE.strftime('%b %d, %Y')})")

    # Load data
    with st.spinner("Loading data from BigQuery..."):
        try:
            ftue_df = load_ftue_data()
        except Exception as e:
            st.error(f"Failed to load FTUE data: {e}")
            ftue_df = pd.DataFrame()
        try:
            ret_df = load_retention_data(get_bq_client(), '2026-03-01', str(date.today()))
        except Exception as e:
            st.error(f"Failed to load retention data: {e}")
            ret_df = pd.DataFrame()

    if ftue_df.empty and ret_df.empty:
        st.warning("No data available.")
        return

    # =========================================================================
    # SIDEBAR — helper to build "value (count)" labels
    # =========================================================================
    def opts_with_counts(df, col, sort_key=None):
        """Return list of 'value (N)' strings and a mapping back to raw values."""
        if df.empty or col not in df.columns:
            return [], {}, []
        vc = df.groupby(col)['total_users'].sum().reset_index() if 'total_users' in df.columns else df[col].value_counts().reset_index()
        if 'total_users' in df.columns:
            vc.columns = [col, 'count']
        else:
            vc.columns = [col, 'count']
        raw_vals = vc[col].tolist()
        if sort_key:
            raw_vals = sorted(raw_vals, key=sort_key)
        labels = []
        label_map = {}
        for v in raw_vals:
            c = int(vc[vc[col] == v]['count'].sum())
            lbl = f"{v} ({c:,})"
            labels.append(lbl)
            label_map[lbl] = v
        return labels, label_map, raw_vals

    with st.sidebar.form("filter_form"):
        submitted = st.form_submit_button("Run Dashboard", type="primary", use_container_width=True)
        st.markdown("---")
        st.markdown("### Install Date Range")

        all_dates = []
        if not ftue_df.empty and 'install_date' in ftue_df.columns:
            all_dates.extend(ftue_df['install_date'].dropna().tolist())
        if not ret_df.empty:
            all_dates.extend(ret_df['date'].dropna().tolist())
        min_date = min(all_dates) if all_dates else date(2026, 2, 1)
        max_date = max(all_dates) if all_dates else date.today()

        sd_col, ed_col = st.columns(2)
        with sd_col:
            start_date = st.date_input("Start Date", value=min_date, min_value=min_date, max_value=max_date, key="sd")
        with ed_col:
            end_date = st.date_input("End Date", value=max_date, min_value=min_date, max_value=max_date, key="ed")
        date_range = (start_date, end_date)

        st.markdown("---")
        st.markdown("### Additional Filters")

        # Install Week
        selected_weeks = None
        if not ftue_df.empty and 'install_week' in ftue_df.columns:
            week_opts = sorted(ftue_df['install_week'].dropna().unique().tolist())
            selected_weeks = st.multiselect("Install Week", week_opts, default=[])

        # Install Month
        selected_months = None
        if not ftue_df.empty and 'install_month' in ftue_df.columns:
            month_opts = sorted(ftue_df['install_month'].dropna().unique().tolist())
            selected_months = st.multiselect("Install Month", month_opts, default=[])

        # Version with user counts
        ver_labels, ver_map, ver_raw = opts_with_counts(ftue_df, 'install_version', sort_key=version_sort_key) if not ftue_df.empty else ([], {}, [])
        ret_versions = sorted(ret_df['app_version'].dropna().unique().tolist(), key=version_sort_key) if not ret_df.empty else []
        for rv in ret_versions:
            if str(rv) not in [str(v) for v in ver_raw]:
                lbl = f"{rv} (ret only)"
                ver_labels.append(lbl)
                ver_map[lbl] = rv
        # Default to 0.3811 only
        default_ver = [l for l in ver_labels if l.startswith('0.3811')]
        selected_ver_labels = st.multiselect("Version", ver_labels, default=default_ver if default_ver else ver_labels)
        selected_versions = [str(ver_map.get(l, l)) for l in selected_ver_labels]

        # Platform
        plat_opts = sorted(ftue_df['platform'].dropna().unique().tolist()) if not ftue_df.empty and 'platform' in ftue_df.columns else (sorted(ret_df['platform'].dropna().unique().tolist()) if not ret_df.empty else [])
        selected_platforms = st.multiselect("Platform", plat_opts, default=plat_opts) if plat_opts else []

        # Media Source
        selected_mediasource = None
        if not ftue_df.empty and 'mediasource' in ftue_df.columns:
            ms_labels, ms_map, _ = opts_with_counts(ftue_df, 'mediasource')
            sel_ms_labels = st.multiselect("Media Source", ms_labels, default=[])
            selected_mediasource = [ms_map[l] for l in sel_ms_labels] if sel_ms_labels else None

        # Media Type
        selected_media_type = None
        if not ftue_df.empty and 'media_type' in ftue_df.columns:
            mt_labels, mt_map, _ = opts_with_counts(ftue_df, 'media_type')
            sel_mt_labels = st.multiselect("Media Type", mt_labels, default=[])
            selected_media_type = [mt_map[l] for l in sel_mt_labels] if sel_mt_labels else None

        # Country with user counts
        selected_countries = None
        if not ftue_df.empty and 'country' in ftue_df.columns:
            c_labels, c_map, _ = opts_with_counts(ftue_df, 'country')
            sel_c_labels = st.multiselect("Country", c_labels, default=[])
            selected_countries = [c_map[l] for l in sel_c_labels] if sel_c_labels else None

        # Is Low Payers
        selected_low_payers = None
        if not ftue_df.empty and 'is_low_payers_country' in ftue_df.columns:
            lp_opts = sorted(ftue_df['is_low_payers_country'].dropna().unique().tolist())
            lp_display = {0: 'No', 1: 'Yes'}
            lp_labels = [lp_display.get(v, str(v)) for v in lp_opts]
            lp_map = dict(zip(lp_labels, lp_opts))
            sel_lp = st.selectbox("Is Low Payers", ["All"] + lp_labels, index=0)
            selected_low_payers = lp_map[sel_lp] if sel_lp != "All" else None

        period_options = ["All", "Before Test", "After Test"]
        selected_period = st.selectbox("Period (Before/After Test)", period_options, index=0)

        st.markdown("---")
        st.markdown("### Retention Filters")
        selected_usa_only = None
        if not ret_df.empty and 'is_usa' in ret_df.columns:
            usa_opt = st.selectbox("Country (Retention)", ["All", "US Only", "Non-US"], index=0, key="ret_usa")
            if usa_opt == "US Only":
                selected_usa_only = 1
            elif usa_opt == "Non-US":
                selected_usa_only = 0

        st.caption("Note: values in brackets show total users for each option.")

        # (Run button is at the top of the form)

    # =========================================================================
    # APPLY FILTERS
    # =========================================================================
    fdf = ftue_df.copy() if not ftue_df.empty else pd.DataFrame()
    if not fdf.empty:
        fdf['install_version_str'] = fdf['install_version'].astype(str)
        fdf = fdf[fdf['install_version_str'].isin(selected_versions)]
        if 'install_date' in fdf.columns and isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            fdf = fdf[(fdf['install_date'] >= date_range[0]) & (fdf['install_date'] <= date_range[1])]
        if selected_period == "Before Test":
            fdf = fdf[fdf['install_date'] < TEST_START_DATE]
        elif selected_period == "After Test":
            fdf = fdf[fdf['install_date'] >= TEST_START_DATE]
        if selected_platforms and 'platform' in fdf.columns:
            fdf = fdf[fdf['platform'].isin(selected_platforms)]
        if selected_countries is not None and 'country' in fdf.columns:
            fdf = fdf[fdf['country'].isin(selected_countries)]
        if selected_weeks and 'install_week' in fdf.columns:
            fdf = fdf[fdf['install_week'].isin(selected_weeks)]
        if selected_months and 'install_month' in fdf.columns:
            fdf = fdf[fdf['install_month'].isin(selected_months)]
        if selected_mediasource is not None and 'mediasource' in fdf.columns:
            fdf = fdf[fdf['mediasource'].isin(selected_mediasource)]
        if selected_media_type is not None and 'media_type' in fdf.columns:
            fdf = fdf[fdf['media_type'].isin(selected_media_type)]
        if selected_low_payers is not None and 'is_low_payers_country' in fdf.columns:
            fdf = fdf[fdf['is_low_payers_country'] == selected_low_payers]

    rdf = ret_df.copy() if not ret_df.empty else pd.DataFrame()
    if not rdf.empty:
        rdf = rdf[rdf['app_version'].isin(selected_versions)]
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            rdf = rdf[(rdf['date'] >= date_range[0]) & (rdf['date'] <= date_range[1])]
        if selected_period == "Before Test":
            rdf = rdf[rdf['date'] < TEST_START_DATE]
        elif selected_period == "After Test":
            rdf = rdf[rdf['date'] >= TEST_START_DATE]
        if selected_platforms:
            rdf = rdf[rdf['platform'].isin(selected_platforms)]
        if selected_usa_only is not None and 'is_usa' in rdf.columns:
            rdf = rdf[rdf['is_usa'] == selected_usa_only]

    versions_in_data = sorted(
        set(list(fdf['install_version_str'].unique()) if not fdf.empty else []) |
        set(list(rdf['app_version'].unique()) if not rdf.empty else []),
        key=version_sort_key
    )
    color_map = get_version_color_map(versions_in_data)

    # =========================================================================
    # FILTER COVERAGE PIE (shows what % of total users your filters cover)
    # =========================================================================
    if not ftue_df.empty and 'total_users' in ftue_df.columns:
        total_all = int(ftue_df['total_users'].sum())
        total_filtered = int(fdf['total_users'].sum()) if not fdf.empty and 'total_users' in fdf.columns else 0
        total_excluded = total_all - total_filtered
        pct_included = total_filtered / total_all * 100 if total_all > 0 else 0

        st.markdown(f'<div class="legend-box" style="text-align:center;">'
                    f'<b>Filter Coverage:</b> '
                    f'<span style="color:#2ECB71;font-size:1.3em;font-weight:700;">{total_filtered:,}</span> '
                    f'of {total_all:,} users selected '
                    f'(<span style="color:#2ECB71;font-weight:600;">{pct_included:.1f}%</span>)'
                    f'</div>', unsafe_allow_html=True)

        fc1, fc2 = st.columns([1, 3])
        with fc1:
            fig_fc = go.Figure(data=[go.Pie(
                labels=['Selected', 'Filtered Out'],
                values=[total_filtered, total_excluded],
                hole=0.55, textinfo='percent',
                textfont=dict(size=13),
                marker=dict(colors=[COLORS['after'], '#E8ECF0']),
                hovertemplate='%{label}: %{value:,} users (%{percent})<extra></extra>',
            )])
            fig_fc.update_layout(
                height=200, margin=dict(l=10, r=10, t=10, b=10),
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                showlegend=False,
                annotations=[dict(text=f"<b>{pct_included:.0f}%</b>", x=0.5, y=0.5,
                                  font_size=18, font_color=COLORS['after'], showarrow=False)],
            )
            st.plotly_chart(fig_fc, use_container_width=True)
        with fc2:
            # Breakdown of what's included
            if not fdf.empty:
                bc_items = []
                for ver in sorted(fdf['install_version_str'].unique(), key=version_sort_key):
                    vu = int(fdf[fdf['install_version_str'] == ver]['total_users'].sum())
                    bc_items.append(f"v{ver}: **{vu:,}**")
                st.markdown("**By version:** " + " | ".join(bc_items))
                if 'platform' in fdf.columns:
                    plat_items = []
                    for p in sorted(fdf['platform'].unique()):
                        pu = int(fdf[fdf['platform'] == p]['total_users'].sum())
                        plat_items.append(f"{p}: **{pu:,}**")
                    st.markdown("**By platform:** " + " | ".join(plat_items))
                date_min = fdf['install_date'].min() if 'install_date' in fdf.columns else 'N/A'
                date_max = fdf['install_date'].max() if 'install_date' in fdf.columns else 'N/A'
                st.markdown(f"**Date range:** {date_min} to {date_max}")

    # =========================================================================
    # TABS
    # =========================================================================
    tab_ba, tab_funnel, tab_steps_trend, tab_installs, tab_daily_steps, tab_retention, tab_daily_retention = st.tabs([
        "Before vs After",
        "Chart: Version",
        "FTUE Steps Trend",
        "Install Distribution",
        "Daily Step Tracking",
        "Retention by Version",
        "Daily Retention",
    ])

    # =========================================================================
    # TAB 0: BEFORE VS AFTER
    # =========================================================================
    with tab_ba:
        ba_version = st.selectbox(
            "Select version",
            sorted(versions_in_data, key=version_sort_key, reverse=True),
            index=0, key="ba_version",
        )

        has_ftue = not fdf.empty
        has_ret = not rdf.empty
        pct_cols_ba = get_pct_columns(fdf) if has_ftue else []

        vals_before, vals_after = [], []
        users_before, users_after = 0, 0
        ba_step_labels = []
        dropoff_before, dropoff_after = [], []
        has_after_ftue = False

        if has_ftue and pct_cols_ba:
            ba_step_labels = [format_step_label(m) for m in pct_cols_ba]
            vdf_ba = fdf[fdf['install_version_str'] == str(ba_version)]
            vdf_before = vdf_ba[vdf_ba['install_date'] < TEST_START_DATE]
            vdf_after = vdf_ba[vdf_ba['install_date'] >= TEST_START_DATE]
            has_after_ftue = not vdf_after.empty
            vals_before, users_before = calc_weighted_steps(vdf_before, pct_cols_ba)
            if has_after_ftue:
                vals_after, users_after = calc_weighted_steps(vdf_after, pct_cols_ba)
            for i in range(len(vals_before) - 1):
                do = (vals_before[i] - vals_before[i + 1]) / vals_before[i] if vals_before[i] > 0 else 0
                dropoff_before.append(do)
            if has_after_ftue:
                for i in range(len(vals_after) - 1):
                    do = (vals_after[i] - vals_after[i + 1]) / vals_after[i] if vals_after[i] > 0 else 0
                    dropoff_after.append(do)

        ret_data = {}
        has_after_ret = False
        if has_ret:
            vdf_ret = rdf[rdf['app_version'] == str(ba_version)]
            vdf_ret_before = vdf_ret[vdf_ret['date'] < TEST_START_DATE]
            vdf_ret_after = vdf_ret[vdf_ret['date'] >= TEST_START_DATE]
            has_after_ret = not vdf_ret_after.empty
            for rd in [1, 3, 7, 14]:
                rb = weighted_retention(vdf_ret_before[vdf_ret_before['days_since_install'] == rd])
                nb = vdf_ret_before[(vdf_ret_before['days_since_install'] == rd) & (vdf_ret_before['users_active'] > 0)]['cohort_size'].sum()
                ra = weighted_retention(vdf_ret_after[vdf_ret_after['days_since_install'] == rd])
                na = vdf_ret_after[(vdf_ret_after['days_since_install'] == rd) & (vdf_ret_after['users_active'] > 0)]['cohort_size'].sum()
                ret_data[rd] = {'before': rb, 'after': ra, 'nb': nb, 'na': na}

        # --- SUMMARY ---
        st.markdown("---")

        if has_after_ftue or has_after_ret:
            lifts = []
            if has_after_ftue and vals_before and vals_after:
                for i in range(len(vals_before)):
                    vb, va = vals_before[i], vals_after[i]
                    lift_pct = (va - vb) / vb * 100 if vb > 0 else 0
                    lifts.append((ba_step_labels[i], va - vb, lift_pct))
                improved = sorted([(s, d, p) for s, d, p in lifts if p > 0.5], key=lambda x: -x[2])
                declined = sorted([(s, d, p) for s, d, p in lifts if p < -0.5], key=lambda x: x[2])
                avg_lift = np.mean([p for _, _, p in lifts])
                steps_improved = len(improved)
                last_step_lift = lifts[-1][2] if lifts else 0

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Avg FTUE Lift", f"{avg_lift:+.1f}%")
                c2.metric("Steps Improved", f"{steps_improved}/{len(lifts)}")
                c3.metric("Steps Declined", f"{len(declined)}/{len(lifts)}")
                c4.metric("Last Step Lift", f"{last_step_lift:+.1f}%")

            if has_after_ret:
                rc1, rc2, rc3, rc4 = st.columns(4)
                for cw, rd in zip([rc1, rc2, rc3, rc4], [1, 3, 7, 14]):
                    ri = ret_data.get(rd, {})
                    if ri.get('nb', 0) > 0 and ri.get('na', 0) > 0:
                        d = ri['after'] - ri['before']
                        cw.metric(f"D{rd} Retention", f"{ri['after']:.1%}", f"{d:+.1%} vs before")
                    elif ri.get('nb', 0) > 0:
                        cw.metric(f"D{rd} Baseline", f"{ri['before']:.1%}", "Awaiting post-test")

            # Text summary
            lines = []
            if has_after_ftue and lifts:
                lines.append(f"<h4>v{ba_version} — Impact Summary</h4>")
                lines.append(f"<b>FTUE:</b> {steps_improved}/{len(lifts)} steps improved (avg lift: {avg_lift:+.1f}%)")
                if improved:
                    lines.append("<br><b>Top gains:</b> " + " &middot; ".join([f"{s} {delta_tag(p)}" for s, _, p in improved[:3]]))
                if declined:
                    lines.append("<br><b>Biggest drops:</b> " + " &middot; ".join([f"{s} {delta_tag(p)}" for s, _, p in declined[:3]]))
            if has_after_ret:
                ret_items = []
                for rd in [1, 3]:
                    ri = ret_data.get(rd, {})
                    if ri.get('nb', 0) > 0 and ri.get('na', 0) > 0:
                        d = ri['after'] - ri['before']
                        ret_items.append(f"D{rd}: {ri['before']:.1%} &rarr; {ri['after']:.1%} {delta_tag(d*100)}")
                if ret_items:
                    lines.append("<br><b>Retention:</b> " + " &middot; ".join(ret_items))
            if lines:
                st.markdown(f'<div class="summary-box">{"".join(lines)}</div>', unsafe_allow_html=True)

        else:
            # Pre-test baseline
            if vals_before:
                c1, c2, c3 = st.columns(3)
                c1.metric("Users (Baseline)", fmt_number(users_before))
                c2.metric("Last Step Conversion", f"{vals_before[-1]:.1%}" if vals_before else "N/A")
                if dropoff_before:
                    worst_do = max(dropoff_before)
                    worst_idx = dropoff_before.index(worst_do)
                    c3.metric("Worst Drop-off", f"{worst_do:.1%}", f"Step {worst_idx+1} -> {worst_idx+2}")
            if has_ret:
                rc1, rc2, rc3, rc4 = st.columns(4)
                for cw, rd in zip([rc1, rc2, rc3, rc4], [1, 3, 7, 14]):
                    ri = ret_data.get(rd, {})
                    if ri.get('nb', 0) > 0:
                        cw.metric(f"D{rd} Baseline", f"{ri['before']:.1%}")

            st.markdown(f'<div class="summary-box"><h4>Pre-test baseline for v{ba_version}</h4>'
                        f'Test begins <b>{TEST_START_DATE.strftime("%b %d, %Y")}</b>. '
                        'This section will auto-populate with lift metrics, drop-off deltas, '
                        'and a full impact summary once post-test data is available.</div>',
                        unsafe_allow_html=True)

        # --- FTUE FUNNEL CHART ---
        if has_ftue and pct_cols_ba and vals_before:
            st.markdown("---")
            st.markdown("### FTUE Funnel: Before vs After")
            st.markdown('<div class="legend-box">'
                        '<strong style="color:#5B8DEF">Blue line</strong> = Before test &nbsp;&nbsp;'
                        '<strong style="color:#2ECB71">Green line</strong> = After test &nbsp;&nbsp;'
                        '<strong>Annotations</strong> = % lift at key steps (green = improved, red = declined)'
                        '</div>', unsafe_allow_html=True)

            ba_metric_options = ["Conversion vs Step 1"]
            ratio_cols_ba = get_ratio_columns(fdf)
            if ratio_cols_ba:
                ba_metric_options.append("Conversion vs Previous Step")
            ba_metric_set = st.selectbox("Metric set", ba_metric_options, key="ba_metric_set")

            if ba_metric_set == "Conversion vs Previous Step" and ratio_cols_ba:
                active_labels = [format_step_label(m) for m in ratio_cols_ba]
                av_before, _ = calc_weighted_steps(vdf_before, ratio_cols_ba)
                av_after = []
                if has_after_ftue:
                    av_after, _ = calc_weighted_steps(vdf_after, ratio_cols_ba)
            else:
                active_labels = ba_step_labels
                av_before = vals_before
                av_after = vals_after

            fig_ba = go.Figure()
            fig_ba.add_trace(go.Scatter(
                x=active_labels, y=av_before,
                mode='lines+markers',
                name=f"Before ({fmt_number(users_before)} users)",
                line=dict(color=COLORS['before'], width=2.5),
                marker=dict(size=7, symbol='circle'),
                hovertemplate='<b>%{x}</b><br>Before: %{y:.4f}<extra></extra>',
            ))
            if has_after_ftue and av_after:
                fig_ba.add_trace(go.Scatter(
                    x=active_labels, y=av_after,
                    mode='lines+markers',
                    name=f"After ({fmt_number(users_after)} users)",
                    line=dict(color=COLORS['after'], width=2.5),
                    marker=dict(size=7, symbol='diamond'),
                    hovertemplate='<b>%{x}</b><br>After: %{y:.4f}<extra></extra>',
                ))
                for i in range(len(av_before)):
                    if (i % 5 == 0 or i == len(av_before) - 1) and av_before[i] > 0:
                        lift = (av_after[i] - av_before[i]) / av_before[i] * 100
                        color = COLORS['after'] if lift > 0 else COLORS['negative']
                        fig_ba.add_annotation(
                            x=active_labels[i], y=max(av_before[i], av_after[i]),
                            text=f"<b>{lift:+.1f}%</b>", showarrow=True,
                            arrowhead=0, arrowcolor=color, arrowwidth=1,
                            ay=-25, font=dict(size=11, color=color),
                            bgcolor='rgba(255,255,255,0.85)', borderpad=3,
                        )

            apply_chart_theme(fig_ba,
                title=dict(text=f"v{ba_version} — FTUE Funnel", font=dict(size=18)),
                xaxis_title="FTUE Steps", yaxis_title="Conversion Rate",
                height=620, hovermode='x unified',
                xaxis_tickangle=-45, xaxis=dict(tickfont=dict(size=9)),
                yaxis=dict(tickformat='.2f'),
                margin=dict(b=160, t=80),
            )
            st.plotly_chart(fig_ba, use_container_width=True)

            if not has_after_ftue:
                st.info("Showing pre-test baseline. Green 'After' line appears once post-test data flows in.")

        # --- DROP-OFF COMPARISON ---
        if has_ftue and len(dropoff_before) > 0:
            st.markdown("---")
            st.markdown("### Step-to-Step Drop-off")
            st.markdown('<div class="legend-box">'
                        'Each bar shows the <b>% of users lost</b> between consecutive steps. '
                        '<b>Lower bars = better</b> (fewer users drop off). '
                        '<strong style="color:#5B8DEF">Blue</strong> = Before &nbsp;'
                        '<strong style="color:#2ECB71">Green</strong> = After'
                        '</div>', unsafe_allow_html=True)

            transition_labels = [f"{ba_step_labels[i]} -> {ba_step_labels[i+1]}" for i in range(len(dropoff_before))]
            short_labels = [f"{i+1}->{i+2}" for i in range(len(dropoff_before))]

            fig_do = go.Figure()
            fig_do.add_trace(go.Bar(
                x=short_labels, y=dropoff_before, name="Before",
                marker_color=COLORS['before'], opacity=0.85,
                customdata=transition_labels,
                hovertemplate='<b>%{customdata}</b><br>Drop-off: %{y:.2%}<extra>Before</extra>',
            ))
            if has_after_ftue and dropoff_after:
                fig_do.add_trace(go.Bar(
                    x=short_labels, y=dropoff_after, name="After",
                    marker_color=COLORS['after'], opacity=0.85,
                    customdata=transition_labels,
                    hovertemplate='<b>%{customdata}</b><br>Drop-off: %{y:.2%}<extra>After</extra>',
                ))

            apply_chart_theme(fig_do,
                title=dict(text=f"v{ba_version} — Drop-off per Transition"),
                xaxis_title="Step Transition (hover for full name)",
                yaxis_title="Drop-off Rate", yaxis_tickformat='.1%',
                height=420, barmode='group',
            )
            st.plotly_chart(fig_do, use_container_width=True)

            # Drop-off change waterfall
            if has_after_ftue and dropoff_after and len(dropoff_after) == len(dropoff_before):
                st.markdown("#### Drop-off Change (After minus Before)")
                st.markdown('<div class="legend-box">'
                            '<strong style="color:#2ECB71">Green bars below zero</strong> = drop-off decreased = improvement<br>'
                            '<strong style="color:#E74C3C">Red bars above zero</strong> = drop-off increased = regression'
                            '</div>', unsafe_allow_html=True)

                do_changes = [dropoff_after[i] - dropoff_before[i] for i in range(len(dropoff_before))]
                do_colors = [COLORS['after'] if c < -0.001 else COLORS['negative'] if c > 0.001 else COLORS['neutral'] for c in do_changes]

                fig_doc = go.Figure()
                fig_doc.add_trace(go.Bar(
                    x=short_labels, y=do_changes,
                    marker_color=do_colors,
                    customdata=transition_labels,
                    hovertemplate='<b>%{customdata}</b><br>Change: %{y:+.2%}<extra></extra>',
                ))
                fig_doc.add_hline(y=0, line_color="#2C3E50", line_width=1.5)
                apply_chart_theme(fig_doc,
                    title=dict(text=f"v{ba_version} — Drop-off Delta"),
                    xaxis_title="Step Transition", yaxis_title="Change in Drop-off",
                    yaxis_tickformat='+.1%', height=380,
                )
                st.plotly_chart(fig_doc, use_container_width=True)

            # Detailed table
            st.markdown("#### Detailed Step Table")
            detail_rows = []
            for i in range(len(ba_step_labels)):
                row = {'Step': ba_step_labels[i], 'Before': f"{vals_before[i]:.4f}"}
                if has_after_ftue and i < len(vals_after):
                    row['After'] = f"{vals_after[i]:.4f}"
                    lift = vals_after[i] - vals_before[i]
                    lift_pct = (lift / vals_before[i] * 100) if vals_before[i] > 0 else 0
                    row['Lift'] = f"{lift:+.4f}"
                    row['Lift %'] = f"{lift_pct:+.1f}%"
                if i < len(dropoff_before):
                    row['Drop-off Before'] = f"{dropoff_before[i]:.2%}"
                    if has_after_ftue and i < len(dropoff_after):
                        row['Drop-off After'] = f"{dropoff_after[i]:.2%}"
                        row['DO Change'] = f"{dropoff_after[i] - dropoff_before[i]:+.2%}"
                detail_rows.append(row)
            st.dataframe(pd.DataFrame(detail_rows), use_container_width=True, hide_index=True, height=500)

        # --- RETENTION ---
        if has_ret:
            st.markdown("---")
            st.markdown("### Retention: Before vs After")
            st.markdown('<div class="legend-box">'
                        'Retention = % of install cohort returning on day N. '
                        'Calculation <b>excludes days with 0 active users</b> to avoid skew. '
                        '<strong style="color:#5B8DEF">Blue</strong> = Before test &nbsp;'
                        '<strong style="color:#2ECB71">Green</strong> = After test &nbsp;'
                        '<strong style="color:#E74C3C">Red dashed line</strong> = Test start date &nbsp;'
                        '<b>Dotted lines</b> = period averages'
                        '</div>', unsafe_allow_html=True)

            ret_comp = []
            for rd in [1, 3, 7, 14]:
                ri = ret_data.get(rd, {})
                row = {'Metric': f'D{rd} Retention', 'Before': fmt_pct(ri['before']) if ri.get('nb', 0) > 0 else 'N/A', 'Cohort Before': fmt_number(ri.get('nb', 0))}
                if has_after_ret and ri.get('na', 0) > 0:
                    d = ri['after'] - ri['before']
                    row.update({'After': fmt_pct(ri['after']), 'Cohort After': fmt_number(ri['na']),
                                'Delta (pp)': f"{d:+.2%}", 'Relative Lift': f"{(d / ri['before'] * 100) if ri['before'] > 0 else 0:+.1f}%"})
                else:
                    row.update({'After': '-', 'Cohort After': '-', 'Delta (pp)': '-', 'Relative Lift': '-'})
                ret_comp.append(row)
            st.dataframe(pd.DataFrame(ret_comp), use_container_width=True, hide_index=True)

            st.markdown(f"#### v{ba_version} — D1 & D3 Daily Retention")

            for ret_d in [1, 3]:
                daily_ret = []
                for dt, gdf in vdf_ret.groupby('date'):
                    ddf = gdf[(gdf['days_since_install'] == ret_d) & (gdf['users_active'] > 0)]
                    cs, ua = ddf['cohort_size'].sum(), ddf['users_active'].sum()
                    ret = ua / cs if cs > 0 else None
                    cohort_d0 = gdf[gdf['days_since_install'] == 0]['cohort_size'].sum()
                    daily_ret.append({'date': dt, 'retention': ret, 'cohort': cohort_d0, 'period': 'After' if dt >= TEST_START_DATE else 'Before'})

                dr_df = pd.DataFrame(daily_ret).dropna(subset=['retention'])
                if dr_df.empty:
                    continue
                dr_df['date'] = pd.to_datetime(dr_df['date'])
                dr_before = dr_df[dr_df['period'] == 'Before']
                dr_after = dr_df[dr_df['period'] == 'After']

                fig_dr = go.Figure()
                if not dr_before.empty:
                    fig_dr.add_trace(go.Scatter(
                        x=dr_before['date'], y=dr_before['retention'], mode='lines+markers',
                        name='Before', line=dict(color=COLORS['before'], width=2.5), marker=dict(size=6),
                        customdata=dr_before[['cohort']].values,
                        hovertemplate='%{x|%b %d}<br>D' + str(ret_d) + ': %{y:.2%}<br>Cohort: %{customdata[0]:,}<extra>Before</extra>',
                    ))
                if not dr_after.empty:
                    fig_dr.add_trace(go.Scatter(
                        x=dr_after['date'], y=dr_after['retention'], mode='lines+markers',
                        name='After', line=dict(color=COLORS['after'], width=2.5), marker=dict(size=6, symbol='diamond'),
                        customdata=dr_after[['cohort']].values,
                        hovertemplate='%{x|%b %d}<br>D' + str(ret_d) + ': %{y:.2%}<br>Cohort: %{customdata[0]:,}<extra>After</extra>',
                    ))
                if not dr_before.empty:
                    avg_b = dr_before['retention'].mean()
                    fig_dr.add_hline(y=avg_b, line_dash="dot", line_color=COLORS['before'], line_width=1.5,
                                     annotation_text=f"Avg Before: {avg_b:.1%}", annotation_position="top left",
                                     annotation_font=dict(size=11, color=COLORS['before']))
                if not dr_after.empty:
                    avg_a = dr_after['retention'].mean()
                    fig_dr.add_hline(y=avg_a, line_dash="dot", line_color=COLORS['after'], line_width=1.5,
                                     annotation_text=f"Avg After: {avg_a:.1%}", annotation_position="top right",
                                     annotation_font=dict(size=11, color=COLORS['after']))
                add_test_start_line(fig_dr)
                apply_chart_theme(fig_dr,
                    title=dict(text=f"v{ba_version} — D{ret_d} Retention"), xaxis_title="Install Date",
                    yaxis_title=f"D{ret_d} Retention", yaxis_tickformat='.1%', height=420,
                    hovermode='x unified',
                )
                st.plotly_chart(fig_dr, use_container_width=True)

            if not has_after_ret:
                st.info("No post-test retention data yet. D1 appears ~1 day after test start, D3 after ~3 days.")

    # =========================================================================
    # TAB 1: CHART: VERSION
    # =========================================================================
    with tab_funnel:
        st.markdown("### Chart: Version")

        if fdf.empty:
            st.warning("No FTUE data for selected filters.")
        else:
            pct_cols = get_pct_columns(fdf)

            # --- Auto insights ---
            if pct_cols:
                last_col = pct_cols[-1]
                ver_last = {}
                for ver in sorted(fdf['install_version_str'].unique(), key=version_sort_key):
                    vdf = fdf[fdf['install_version_str'] == ver]
                    tu = vdf['total_users'].sum() if 'total_users' in vdf.columns else len(vdf)
                    if tu > 0 and 'total_users' in vdf.columns:
                        val = (vdf[last_col] * vdf['total_users']).sum() / tu
                    else:
                        val = vdf[last_col].mean()
                    ver_last[ver] = val
                best_ver = max(ver_last, key=ver_last.get) if ver_last else "N/A"
                worst_ver = min(ver_last, key=ver_last.get) if ver_last else "N/A"
                last_label = format_step_label(last_col)
                st.markdown(f'<div class="summary-box"><h4>Quick Insights</h4>'
                            f'Comparing <b>{len(ver_last)}</b> versions across <b>{len(pct_cols)}</b> FTUE steps. '
                            f'Best end-to-end conversion (last step "{last_label}"): '
                            f'<b>v{best_ver}</b> ({ver_last.get(best_ver, 0):.1%}). '
                            f'Lowest: <b>v{worst_ver}</b> ({ver_last.get(worst_ver, 0):.1%}).</div>',
                            unsafe_allow_html=True)

            pct_cols = get_pct_columns(fdf)
            ratio_cols = get_ratio_columns(fdf)
            c1, c2 = st.columns(2)
            with c1:
                chart_type = st.radio("Chart type", ["Bar", "Line"], index=1, horizontal=True, key="funnel_chart_type")
            with c2:
                m_opts = []
                if pct_cols: m_opts.append("Conversion vs Step 1")
                if ratio_cols: m_opts.append("Conversion vs Previous Step")
                metric_set = st.selectbox("Metric set", m_opts, key="funnel_metric_set") if m_opts else None
            metrics = pct_cols if metric_set == "Conversion vs Step 1" else (ratio_cols if metric_set == "Conversion vs Previous Step" else pct_cols)
            if metrics:
                step_labels = [format_step_label(m) for m in metrics]
                fig = go.Figure()
                for ver in sorted(fdf['install_version_str'].unique(), key=version_sort_key):
                    vdf = fdf[fdf['install_version_str'] == ver]
                    tu = vdf['total_users'].sum() if 'total_users' in vdf.columns else len(vdf)
                    values = []
                    for m in metrics:
                        val = (vdf[m] * vdf['total_users']).sum() / tu if 'total_users' in vdf.columns and tu > 0 else vdf[m].mean()
                        values.append(val)
                    if chart_type == "Line":
                        fig.add_trace(go.Scatter(x=step_labels, y=values, mode='lines+markers', name=str(ver),
                            line=dict(color=color_map.get(str(ver), '#333'), width=2), marker=dict(size=6),
                            hovertemplate='<b>%{x}</b><br>v' + str(ver) + ': %{y:.4f}<extra></extra>'))
                    else:
                        fig.add_trace(go.Bar(x=step_labels, y=values, name=str(ver), marker_color=color_map.get(str(ver), '#333')))
                apply_chart_theme(fig, xaxis_title="FTUE Steps", yaxis_title="Value", height=620,
                    hovermode='x unified', xaxis_tickangle=-45, xaxis=dict(tickfont=dict(size=9)),
                    yaxis=dict(tickformat='.2f'), barmode='group' if chart_type == "Bar" else None, margin=dict(b=160, t=70))
                st.plotly_chart(fig, use_container_width=True)

                st.markdown("#### Version Summary")
                rows = []
                for ver in sorted(fdf['install_version_str'].unique(), key=version_sort_key):
                    vdf = fdf[fdf['install_version_str'] == ver]
                    tu = vdf['total_users'].sum() if 'total_users' in vdf.columns else len(vdf)
                    row = {'Version': ver, 'Users': fmt_number(tu)}
                    for m in metrics:
                        val = (vdf[m] * vdf['total_users']).sum() / tu if 'total_users' in vdf.columns and tu > 0 else vdf[m].mean()
                        row[format_step_label(m)] = fmt_pct(val)
                    rows.append(row)
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

                # Step Reference Table
                st.markdown("#### Step Reference")
                st.markdown('<div class="legend-box">What each FTUE step measures — the event name and conditions from the tracking query.</div>', unsafe_allow_html=True)
                step_reference = [
                    {"Step": "01: impression_privacy", "Event": "impression_privacy", "Description": "User sees the privacy screen"},
                    {"Step": "02: impression_scapes_ch1", "Event": "impression_scapes", "Description": "User sees scapes screen (chapter 1)"},
                    {"Step": "03: board_tasks_new_task", "Event": "board_tasks_new_task", "Description": "New board task appears"},
                    {"Step": "04: impression_dialog_1001199", "Event": "impression_dialog", "Description": "Dialog 1001199 shown (intro dialog)"},
                    {"Step": "05: click_dialog_exit_1001199", "Event": "click_dialog_exit", "Description": "User closes dialog 1001199"},
                    {"Step": "06: ftue_flow1_step0", "Event": "impression_ftue_flow1_step0", "Description": "FTUE Flow 1 begins — first tutorial prompt"},
                    {"Step": "07: ftue_flow1_step1", "Event": "impression_ftue_flow1_step1", "Description": "FTUE Flow 1 step 1 — tutorial continues"},
                    {"Step": "08: click_board_button_scapes", "Event": "click_board_button_scapes", "Description": "User taps the scapes button on the board"},
                    {"Step": "09: impression_board", "Event": "impression_board", "Description": "User sees the main board screen"},
                    {"Step": "10: ftue_flow1_step2", "Event": "impression_ftue_flow1_step2", "Description": "FTUE Flow 1 step 2 — guided to merge"},
                    {"Step": "11: generation_before_merge", "Event": "generation", "Description": "User generates an item BEFORE their first merge"},
                    {"Step": "12: ftue_flow1_step3", "Event": "impression_ftue_flow1_step3", "Description": "FTUE Flow 1 step 3 — merge tutorial"},
                    {"Step": "13: first_merge", "Event": "merge", "Description": "User performs their first ever merge"},
                    {"Step": "14: ftue_flow1_step4", "Event": "impression_ftue_flow1_step4", "Description": "FTUE Flow 1 step 4 — post-merge guidance"},
                    {"Step": "15: board_tasks_task_ready", "Event": "board_tasks_task_ready", "Description": "A board task becomes ready to complete"},
                    {"Step": "16: ftue_flow1_step5", "Event": "impression_ftue_flow1_step5", "Description": "FTUE Flow 1 step 5 — task completion guide"},
                    {"Step": "17: click_board_tasks_go", "Event": "click_board_tasks_go", "Description": "User taps 'Go' on a board task"},
                    {"Step": "18: rewards_board_task", "Event": "rewards_board_task", "Description": "User receives reward for completing board task"},
                    {"Step": "19: ftue_flow1_step6", "Event": "impression_ftue_flow1_step6", "Description": "FTUE Flow 1 step 6 — generation tutorial"},
                    {"Step": "20: generation_after_merge", "Event": "generation", "Description": "User generates an item AFTER their first merge"},
                    {"Step": "21: ftue_flow1_step7", "Event": "impression_ftue_flow1_step7", "Description": "FTUE Flow 1 step 7 — end of Flow 1"},
                    {"Step": "22: impression_how_to_play", "Event": "impression_how_to_play", "Description": "User sees the 'How to Play' screen"},
                    {"Step": "23: click_scapes_button_board", "Event": "click_scapes_button_board", "Description": "User taps scapes button from the board"},
                    {"Step": "24: ftue_flow2_step0", "Event": "impression_ftue_flow2_step0", "Description": "FTUE Flow 2 begins — scapes tutorial"},
                    {"Step": "25: impression_dialog_10013", "Event": "impression_dialog", "Description": "Dialog 10013 shown (scapes task intro)"},
                    {"Step": "26: click_scapes_tasks_go_button", "Event": "click_scapes_tasks_go_button", "Description": "User taps 'Go' on a scapes task"},
                    {"Step": "27: scapes_tasks_cash_deducted", "Event": "scapes_tasks_cash_deducted", "Description": "Cash deducted for scapes task"},
                    {"Step": "28: rewards_scape_task", "Event": "rewards_scape_task", "Description": "User receives reward for scapes task"},
                    {"Step": "29: click_dialog_exit_10013", "Event": "click_dialog_exit", "Description": "User closes dialog 10013"},
                    {"Step": "30: ftue_flow2_step1", "Event": "impression_ftue_flow2_step1", "Description": "FTUE Flow 2 step 1 — ship tutorial"},
                    {"Step": "31: impression_dialog_10015", "Event": "impression_dialog", "Description": "Dialog 10015 shown (ship intro)"},
                    {"Step": "32: ftue_flow2_step2", "Event": "impression_ftue_flow2_step2", "Description": "FTUE Flow 2 step 2"},
                    {"Step": "33: ship_animation", "Event": "impression_ship_animation_started", "Description": "Ship animation plays — chapter completion"},
                    {"Step": "34: ftue_flow2_step5", "Event": "impression_ftue_flow2_step5", "Description": "FTUE Flow 2 step 5 — end of Flow 2"},
                    {"Step": "35: ftue_flow3_step0", "Event": "impression_ftue_flow3_step0", "Description": "FTUE Flow 3 begins — chapter 2 tutorial"},
                    {"Step": "36: new_chapter_2", "Event": "scapes_tasks_new_chapter", "Description": "User reaches chapter 2"},
                    {"Step": "37: ftue_flow3_step1_ch2", "Event": "impression_ftue_flow3_step1", "Description": "FTUE Flow 3 step 1 (chapter 2)"},
                    {"Step": "38: ftue_flow3_step2_ch2", "Event": "impression_ftue_flow3_step2", "Description": "FTUE Flow 3 step 2 (chapter 2)"},
                    {"Step": "39: click_harvest_collect_ch2", "Event": "click_harvest_collect", "Description": "User collects harvest reward (chapter 2)"},
                    {"Step": "40: ftue_flow3_step6_ch2", "Event": "impression_ftue_flow3_step6", "Description": "FTUE Flow 3 step 6 (chapter 2)"},
                    {"Step": "41: click_reward_center", "Event": "click_reward_center", "Description": "User opens the reward center"},
                    {"Step": "42: ftue_flow3_step8_ch2", "Event": "impression_ftue_flow3_step8", "Description": "FTUE Flow 3 step 8 — end of chapter 2 tutorial"},
                    {"Step": "43: ftue_flow12_step0", "Event": "impression_ftue_flow12_step0", "Description": "FTUE Flow 12 begins — hint system intro"},
                    {"Step": "44: ftue_flow12_step4", "Event": "impression_ftue_flow12_step4", "Description": "FTUE Flow 12 step 4 — hint system completion"},
                    {"Step": "45: new_chapter_3", "Event": "scapes_tasks_new_chapter", "Description": "User reaches chapter 3"},
                    {"Step": "46: click_harvest_collect_ch3", "Event": "click_harvest_collect", "Description": "User collects harvest reward (chapter 3)"},
                ]
                st.dataframe(pd.DataFrame(step_reference), use_container_width=True, hide_index=True, height=400)

    # =========================================================================
    # TAB: FTUE STEPS TREND BY DATE
    # =========================================================================
    with tab_steps_trend:
        st.markdown("### FTUE Steps Trend by Date")
        st.caption("Compare how each FTUE step's conversion rate changes over time")

        if fdf.empty or 'install_date' not in fdf.columns:
            st.warning("No FTUE data for selected filters.")
        else:
            pct_cols_t = get_pct_columns(fdf)
            ratio_cols_t = get_ratio_columns(fdf)

            tc1, tc2, tc3 = st.columns(3)
            with tc1:
                t_metric_opts = []
                if pct_cols_t: t_metric_opts.append("Conversion vs Step 1")
                if ratio_cols_t: t_metric_opts.append("Conversion vs Previous Step")
                t_metric = st.selectbox("Metric type", t_metric_opts, key="trend_metric") if t_metric_opts else None
            with tc2:
                t_granularity = st.radio("Time granularity", ["Daily", "Weekly", "Monthly"], horizontal=True, key="trend_gran")
            with tc3:
                t_metrics = pct_cols_t if t_metric == "Conversion vs Step 1" else (ratio_cols_t if t_metric == "Conversion vs Previous Step" else pct_cols_t)
                t_labels = [format_step_label(m) for m in t_metrics] if t_metrics else []
                t_label_map = dict(zip(t_labels, t_metrics))

                t_selected = st.multiselect("Select steps to display", t_labels,
                    default=t_labels[:8] if len(t_labels) > 8 else t_labels, key="trend_steps")

            # Quick-select buttons
            bc1, bc2, bc3, bc4, bc5 = st.columns(5)
            if bc1.button("First 10 Steps", key="t_first10"):
                t_selected = t_labels[:10]
            if bc2.button("Steps 11-20", key="t_11_20"):
                t_selected = t_labels[10:20] if len(t_labels) > 10 else t_labels
            if bc3.button("Steps 21-30", key="t_21_30"):
                t_selected = t_labels[20:30] if len(t_labels) > 20 else t_labels
            if bc4.button("Steps 31-46", key="t_31_46"):
                t_selected = t_labels[30:] if len(t_labels) > 30 else t_labels
            if bc5.button("All Steps", key="t_all"):
                t_selected = t_labels

            if t_selected and t_metrics:
                # Determine time column
                time_col = 'install_date'
                if t_granularity == "Weekly" and 'install_week' in fdf.columns:
                    time_col = 'install_week'
                elif t_granularity == "Monthly" and 'install_month' in fdf.columns:
                    time_col = 'install_month'

                # Compute weighted average per time period per step
                trend_records = []
                for period_val, gdf in fdf.groupby(time_col):
                    tu = gdf['total_users'].sum() if 'total_users' in gdf.columns else len(gdf)
                    if tu == 0:
                        continue
                    for label in t_selected:
                        col = t_label_map.get(label)
                        if col and col in gdf.columns:
                            if 'total_users' in gdf.columns:
                                val = (gdf[col] * gdf['total_users']).sum() / tu
                            else:
                                val = gdf[col].mean()
                            trend_records.append({
                                'period': period_val,
                                'step': label,
                                'value': val,
                                'users': tu,
                            })

                trend_df = pd.DataFrame(trend_records)
                if trend_df.empty:
                    st.info("No trend data for selected steps.")
                else:
                    if time_col == 'install_date':
                        trend_df['period'] = pd.to_datetime(trend_df['period'])

                    # One chart per step
                    for step_label in t_selected:
                        sdf = trend_df[trend_df['step'] == step_label].sort_values('period')
                        if sdf.empty:
                            continue

                        fig_t = go.Figure()
                        fig_t.add_trace(go.Scatter(
                            x=sdf['period'], y=sdf['value'],
                            mode='lines+markers',
                            name=step_label,
                            line=dict(color=COLORS['before'], width=2.5),
                            marker=dict(size=6),
                            customdata=sdf[['users']].values,
                            hovertemplate='%{x}<br>' + step_label + ': %{y:.4f}<br>Users: %{customdata[0]:,}<extra></extra>',
                        ))

                        # Before/After averages
                        if time_col == 'install_date':
                            sb = sdf[sdf['period'] < pd.Timestamp(TEST_START_DATE)]
                            sa = sdf[sdf['period'] >= pd.Timestamp(TEST_START_DATE)]
                            if not sb.empty:
                                avg_b = sb['value'].mean()
                                fig_t.add_hline(y=avg_b, line_dash="dot", line_color=COLORS['before'], line_width=1.5,
                                    annotation_text=f"Avg Before: {avg_b:.4f}", annotation_position="top left",
                                    annotation_font=dict(size=10, color=COLORS['before']))
                            if not sa.empty:
                                avg_a = sa['value'].mean()
                                fig_t.add_hline(y=avg_a, line_dash="dot", line_color=COLORS['after'], line_width=1.5,
                                    annotation_text=f"Avg After: {avg_a:.4f}", annotation_position="top right",
                                    annotation_font=dict(size=10, color=COLORS['after']))
                            add_test_start_line(fig_t)

                        apply_chart_theme(fig_t,
                            title=dict(text=step_label),
                            xaxis_title="Date" if time_col == 'install_date' else t_granularity,
                            yaxis_title="Conversion Rate",
                            yaxis_tickformat='.2f',
                            height=350, hovermode='x unified',
                        )
                        st.plotly_chart(fig_t, use_container_width=True)

                    # Summary table: before vs after per step
                    if time_col == 'install_date':
                        st.markdown("#### Before vs After Summary")
                        summary_rows = []
                        for label in t_selected:
                            col = t_label_map.get(label)
                            if not col:
                                continue
                            bdf = fdf[fdf['install_date'] < TEST_START_DATE]
                            adf = fdf[fdf['install_date'] >= TEST_START_DATE]
                            tub = bdf['total_users'].sum() if 'total_users' in bdf.columns else len(bdf)
                            tua = adf['total_users'].sum() if 'total_users' in adf.columns else len(adf)
                            vb = (bdf[col] * bdf['total_users']).sum() / tub if 'total_users' in bdf.columns and tub > 0 else (bdf[col].mean() if not bdf.empty else 0)
                            va = (adf[col] * adf['total_users']).sum() / tua if 'total_users' in adf.columns and tua > 0 else (adf[col].mean() if not adf.empty else 0)
                            delta = va - vb
                            pct_ch = (delta / vb * 100) if vb > 0 else 0
                            summary_rows.append({
                                'Step': label,
                                'Before': f"{vb:.4f}",
                                'After': f"{va:.4f}" if tua > 0 else '-',
                                'Delta': f"{delta:+.4f}" if tua > 0 else '-',
                                '% Change': f"{pct_ch:+.1f}%" if tua > 0 else '-',
                            })
                        if summary_rows:
                            st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)

    # =========================================================================
    # TAB: INSTALL DISTRIBUTION
    # =========================================================================
    with tab_installs:
        st.markdown("### Install Distribution")

        if fdf.empty:
            st.warning("No FTUE data for selected filters.")
        else:
            total_installs = int(fdf['total_users'].sum()) if 'total_users' in fdf.columns else len(fdf)

            # --- Auto insights ---
            ver_dist_all = fdf.groupby('install_version_str')['total_users'].sum().reset_index() if 'total_users' in fdf.columns else fdf['install_version_str'].value_counts().reset_index()
            ver_dist_all.columns = ['version', 'users']
            ver_dist_all = ver_dist_all.sort_values('users', ascending=False)
            top_ver = ver_dist_all.iloc[0]
            top_ver_pct = top_ver['users'] / total_installs * 100

            mt_insight = ""
            if 'media_type' in fdf.columns:
                mt_d = fdf.groupby('media_type')['total_users'].sum().reset_index() if 'total_users' in fdf.columns else fdf['media_type'].value_counts().reset_index()
                mt_d.columns = ['media_type', 'users']
                mt_d = mt_d.sort_values('users', ascending=False)
                top_mt = mt_d.iloc[0]
                top_mt_pct = top_mt['users'] / total_installs * 100
                mt_insight = f" Top media type: <b>{top_mt['media_type']}</b> ({top_mt_pct:.0f}%)."

            n_versions = len(ver_dist_all)
            st.markdown(f'<div class="summary-box"><h4>Quick Insights</h4>'
                        f'<b>{total_installs:,}</b> total installs across <b>{n_versions}</b> versions. '
                        f'Dominant version: <b>v{top_ver["version"]}</b> ({top_ver_pct:.0f}% of installs, {int(top_ver["users"]):,} users).'
                        f'{mt_insight}</div>', unsafe_allow_html=True)

            col_pie1, col_pie2 = st.columns(2)

            # --- Installs by Version ---
            with col_pie1:
                st.markdown(f"**Installs by Version**")
                st.caption(f"Total: {total_installs:,} installs")
                if 'total_users' in fdf.columns:
                    ver_dist = fdf.groupby('install_version_str')['total_users'].sum().reset_index()
                    ver_dist.columns = ['version', 'users']
                else:
                    ver_dist = fdf['install_version_str'].value_counts().reset_index()
                    ver_dist.columns = ['version', 'users']
                ver_dist = ver_dist.sort_values('users', ascending=False)

                fig_v = go.Figure(data=[go.Pie(
                    labels=ver_dist['version'], values=ver_dist['users'],
                    hole=0.45, textinfo='label+percent',
                    textposition='inside', textfont=dict(size=12),
                    marker=dict(colors=[color_map.get(str(v), '#999') for v in ver_dist['version']]),
                    hovertemplate='<b>v%{label}</b><br>%{value:,} installs<br>%{percent}<extra></extra>',
                )])
                apply_chart_theme(fig_v, height=420, margin=dict(l=20, r=20, t=30, b=30),
                    showlegend=True, legend=dict(orientation='h', y=-0.1, x=0.5, xanchor='center'))
                st.plotly_chart(fig_v, use_container_width=True)

            # --- Installs by Media Type ---
            with col_pie2:
                if 'media_type' in fdf.columns:
                    st.markdown(f"**Installs by Media Type**")
                    st.caption(f"Total: {total_installs:,} installs")
                    if 'total_users' in fdf.columns:
                        mt_dist = fdf.groupby('media_type')['total_users'].sum().reset_index()
                        mt_dist.columns = ['media_type', 'users']
                    else:
                        mt_dist = fdf['media_type'].value_counts().reset_index()
                        mt_dist.columns = ['media_type', 'users']
                    mt_dist = mt_dist.sort_values('users', ascending=False)

                    mt_colors = ['#5B8DEF', '#AED6F1', '#E74C3C', '#F5B7B1', '#2ECB71', '#ABEBC6',
                                 '#F39C12', '#FAD7A0', '#8E44AD', '#D2B4DE']

                    fig_mt = go.Figure(data=[go.Pie(
                        labels=mt_dist['media_type'], values=mt_dist['users'],
                        hole=0.45, textinfo='label+percent',
                        textposition='inside', textfont=dict(size=12),
                        marker=dict(colors=mt_colors[:len(mt_dist)]),
                        hovertemplate='<b>%{label}</b><br>%{value:,} installs<br>%{percent}<extra></extra>',
                    )])
                    apply_chart_theme(fig_mt, height=420, margin=dict(l=20, r=20, t=30, b=30),
                        showlegend=True, legend=dict(orientation='h', y=-0.1, x=0.5, xanchor='center'))
                    st.plotly_chart(fig_mt, use_container_width=True)
                else:
                    st.info("No media_type column in FTUE data.")

            # --- Installs by Media Source (top 10) ---
            if 'mediasource' in fdf.columns:
                st.markdown("**Top Media Sources**")
                if 'total_users' in fdf.columns:
                    ms_dist = fdf.groupby('mediasource')['total_users'].sum().reset_index()
                    ms_dist.columns = ['mediasource', 'users']
                else:
                    ms_dist = fdf['mediasource'].value_counts().reset_index()
                    ms_dist.columns = ['mediasource', 'users']
                ms_dist = ms_dist.sort_values('users', ascending=True).tail(15)

                fig_ms = go.Figure(data=[go.Bar(
                    x=ms_dist['users'], y=ms_dist['mediasource'],
                    orientation='h', marker_color=COLORS['before'],
                    hovertemplate='<b>%{y}</b><br>%{x:,} installs<extra></extra>',
                    text=[f"{v:,}" for v in ms_dist['users']], textposition='outside',
                )])
                apply_chart_theme(fig_ms, title=dict(text="Top 15 Media Sources by Installs"),
                    xaxis_title="Installs", height=450, margin=dict(l=200))
                st.plotly_chart(fig_ms, use_container_width=True)

            # --- Summary table ---
            st.markdown("#### Install Summary by Version")
            if 'total_users' in fdf.columns:
                summary = fdf.groupby('install_version_str').agg(
                    total_users=('total_users', 'sum'),
                    days=('install_date', 'nunique'),
                ).reset_index()
                summary.columns = ['Version', 'Total Installs', 'Days with Data']
                summary = summary.sort_values('Total Installs', ascending=False)
                summary['% of Total'] = (summary['Total Installs'] / summary['Total Installs'].sum() * 100).round(1).astype(str) + '%'
                summary['Total Installs'] = summary['Total Installs'].apply(lambda x: f"{x:,.0f}")
                st.dataframe(summary, use_container_width=True, hide_index=True)

    # =========================================================================
    # TAB 2: DAILY STEP TRACKING
    # =========================================================================
    with tab_daily_steps:
        st.markdown("### Daily FTUE Step Tracking")

        if fdf.empty or 'install_date' not in fdf.columns:
            st.warning("No FTUE daily data.")
        else:
            n_days = fdf['install_date'].nunique() if 'install_date' in fdf.columns else 0
            has_post = fdf[fdf['install_date'] >= TEST_START_DATE].shape[0] > 0 if 'install_date' in fdf.columns else False
            st.markdown(f'<div class="summary-box"><h4>Quick Insights</h4>'
                        f'Tracking <b>{n_days}</b> days of data. '
                        f'{"Post-test data is available — look for trend changes after the red line." if has_post else "Pre-test only — the red dashed line marks where the test will begin."} '
                        f'Each chart shows one FTUE step over time per version. Select steps below to focus on specific areas of the funnel.</div>',
                        unsafe_allow_html=True)
            pct_cols = get_pct_columns(fdf)
            ratio_cols = get_ratio_columns(fdf)
            m_opts_d = []
            if pct_cols: m_opts_d.append("Conversion vs Step 1")
            if ratio_cols: m_opts_d.append("Conversion vs Previous Step")
            msd = st.selectbox("Metric set", m_opts_d, key="daily_metric_set") if m_opts_d else None
            md = pct_cols if msd == "Conversion vs Step 1" else (ratio_cols if msd == "Conversion vs Previous Step" else pct_cols)
            if md:
                sl = [format_step_label(m) for m in md]
                l2c = dict(zip(sl, md))
                sel = st.multiselect("Select steps to track", sl, default=sl[:5] if len(sl) > 5 else sl, key="daily_steps_select")
                for step_col in [l2c[s] for s in sel]:
                    step_label = format_step_label(step_col)
                    recs = []
                    for (dt, ver), gdf in fdf.groupby(['install_date', 'install_version_str']):
                        tu = gdf['total_users'].sum() if 'total_users' in gdf.columns else len(gdf)
                        val = (gdf[step_col] * gdf['total_users']).sum() / tu if 'total_users' in gdf.columns and tu > 0 else gdf[step_col].mean()
                        recs.append({'date': dt, 'version': str(ver), 'value': val, 'users': tu})
                    ddf = pd.DataFrame(recs).dropna(subset=['value'])
                    if ddf.empty: continue
                    ddf['date'] = pd.to_datetime(ddf['date'])
                    fig = px.line(ddf, x='date', y='value', color='version', color_discrete_map=color_map,
                                  hover_data=['users'], labels={'value': step_label, 'date': 'Install Date'})
                    add_test_start_line(fig)
                    fig.update_yaxes(tickformat='.1%')
                    apply_chart_theme(fig, title=dict(text=f"{step_label}"), height=380, hovermode='x unified')
                    st.plotly_chart(fig, use_container_width=True)

                st.markdown("#### Before vs After Comparison")
                bdf = fdf[fdf['install_date'] < TEST_START_DATE]
                adf = fdf[fdf['install_date'] >= TEST_START_DATE]
                if adf.empty:
                    st.info("No post-test data yet.")
                else:
                    drows = []
                    for ver in sorted(fdf['install_version_str'].unique(), key=version_sort_key):
                        vb, va = bdf[bdf['install_version_str'] == ver], adf[adf['install_version_str'] == ver]
                        ub = vb['total_users'].sum() if 'total_users' in vb.columns else len(vb)
                        ua = va['total_users'].sum() if 'total_users' in va.columns else len(va)
                        row = {'Version': ver, 'Users Before': fmt_number(ub), 'Users After': fmt_number(ua)}
                        for sc in [l2c[s] for s in sel]:
                            lb = format_step_label(sc)
                            vvb = (vb[sc] * vb['total_users']).sum() / ub if 'total_users' in vb.columns and ub > 0 else (vb[sc].mean() if not vb.empty else 0)
                            vva = (va[sc] * va['total_users']).sum() / ua if 'total_users' in va.columns and ua > 0 else (va[sc].mean() if not va.empty else 0)
                            row[f"{lb} Before"] = fmt_pct(vvb)
                            row[f"{lb} After"] = fmt_pct(vva)
                            d = vva - vvb
                            row[f"{lb} Delta"] = f"{d:+.2%}" if vvb and vva else "N/A"
                        drows.append(row)
                    st.dataframe(pd.DataFrame(drows), use_container_width=True, hide_index=True)

    # =========================================================================
    # TAB 3: RETENTION BY VERSION
    # =========================================================================
    with tab_retention:
        st.markdown("### Retention by Version")

        if rdf.empty:
            st.warning("No retention data.")
        else:
            platforms_in_data = sorted(rdf['platform'].dropna().unique().tolist()) if 'platform' in rdf.columns else ['All']
            rv = sorted(rdf['app_version'].unique().tolist(), key=version_sort_key)
            ret_days = [1, 3, 7, 14]

            for plat in platforms_in_data:
                st.markdown(f"#### {plat}")
                pdf = rdf[rdf['platform'] == plat] if 'platform' in rdf.columns else rdf

                trows = []
                d1_vals = {}
                for ver in rv:
                    vdf = pdf[pdf['app_version'] == ver]
                    cd0 = vdf[vdf['days_since_install'] == 0]['cohort_size'].sum()
                    if cd0 == 0:
                        continue
                    row = {'Version': ver, 'Cohort': fmt_number(cd0)}
                    for rd in ret_days:
                        r = weighted_retention(vdf[vdf['days_since_install'] == rd])
                        n = vdf[vdf['days_since_install'] == rd]['cohort_size'].sum()
                        row[f'D{rd}'] = fmt_pct(r) if n > 0 else 'N/A'
                        if rd == 1 and n > 0:
                            d1_vals[ver] = r
                    trows.append(row)

                if d1_vals:
                    best_d1 = max(d1_vals, key=d1_vals.get)
                    worst_d1 = min(d1_vals, key=d1_vals.get)
                    st.markdown(f'<div class="summary-box">'
                                f'<b>{plat}</b> — {len(trows)} versions. '
                                f'Best D1: <b>v{best_d1}</b> ({d1_vals[best_d1]:.1%}). '
                                f'Lowest D1: <b>v{worst_d1}</b> ({d1_vals[worst_d1]:.1%}).</div>',
                                unsafe_allow_html=True)
                if trows:
                    st.dataframe(pd.DataFrame(trows), use_container_width=True, hide_index=True)

            st.markdown("---")
            st.markdown("#### Retention Curve")
            ret_plat = st.radio("Platform", platforms_in_data, horizontal=True, key="ret_curve_plat")
            dr = st.radio("Range", ["D7 (0-7)", "D14 (0-14)", "D30 (0-30)"], horizontal=True, index=1, key="ret_curve_range")
            md = {"D7 (0-7)": 7, "D14 (0-14)": 14, "D30 (0-30)": 30}[dr]
            cdf = rdf[(rdf['days_since_install'] <= md) & (rdf['platform'] == ret_plat)] if 'platform' in rdf.columns else rdf[rdf['days_since_install'] <= md]
            cr = []
            for ver in rv:
                sub = cdf[cdf['app_version'] == ver]
                for d in range(0, md + 1):
                    ds = sub[(sub['days_since_install'] == d) & (sub['users_active'] > 0)]
                    cs, ua = ds['cohort_size'].sum(), ds['users_active'].sum()
                    if cs > 0:
                        cr.append({'day': d, 'retention': ua / cs, 'version': ver, 'cohort': cs})
            cp = pd.DataFrame(cr)
            if not cp.empty:
                fig = go.Figure()
                for ver in rv:
                    vg = cp[cp['version'] == ver].sort_values('day')
                    if vg.empty: continue
                    fig.add_trace(go.Scatter(x=vg['day'], y=vg['retention'], mode='lines+markers',
                        name=f"v{ver}", line=dict(color=color_map.get(ver, '#333'), width=2.5), marker=dict(size=5),
                        customdata=vg[['cohort']].values,
                        hovertemplate='Day %{x}<br>Ret: %{y:.2%}<br>Cohort: %{customdata[0]:,}<extra></extra>'))
                apply_chart_theme(fig, title=dict(text=f"Retention Curve — {ret_plat}"), xaxis_title="Days Since Install",
                    yaxis_title="Retention %", yaxis_tickformat='.1%', height=500, hovermode='x unified',
                    xaxis=dict(dtick=1 if md <= 14 else 5))
                st.plotly_chart(fig, use_container_width=True)

            # Per-User KPIs removed — live retention query doesn't include revenue/activity columns

    # =========================================================================
    # TAB 4: DAILY RETENTION
    # =========================================================================
    with tab_daily_retention:
        st.markdown("### Daily Retention Tracking")

        if rdf.empty:
            st.warning("No retention data.")
        else:
            n_ret_days = rdf['date'].nunique()
            has_post_ret = rdf[rdf['date'] >= TEST_START_DATE].shape[0] > 0
            n_ret_vers = rdf['app_version'].nunique()
            st.markdown(f'<div class="summary-box"><h4>Quick Insights</h4>'
                        f'<b>{n_ret_days}</b> days of retention data across <b>{n_ret_vers}</b> versions. '
                        f'{"Post-test data available — compare the trend before and after the red line." if has_post_ret else "Pre-test only. After the test starts, look for lines lifting above their pre-test average."} '
                        f'Select a retention day (D1/D3/D7/D14) to track.</div>', unsafe_allow_html=True)
            rds = st.selectbox("Retention day", [1, 3, 7, 14], index=0, key="daily_ret_day")
            recs = []
            for (dt, ver), gdf in rdf.groupby(['date', 'app_version']):
                ddf = gdf[(gdf['days_since_install'] == rds) & (gdf['users_active'] > 0)]
                cs, ua = ddf['cohort_size'].sum(), ddf['users_active'].sum()
                cd0 = gdf[gdf['days_since_install'] == 0]['cohort_size'].sum()
                recs.append({'date': dt, 'version': ver, 'retention': ua / cs if cs > 0 else None, 'cohort': cd0})
            drd = pd.DataFrame(recs).dropna(subset=['retention'])
            if not drd.empty:
                drd['date'] = pd.to_datetime(drd['date'])
                fig = px.line(drd, x='date', y='retention', color='version', color_discrete_map=color_map,
                              hover_data=['cohort'], labels={'retention': f'D{rds} Retention', 'date': 'Install Date'})
                add_test_start_line(fig)
                fig.update_yaxes(tickformat='.1%')
                apply_chart_theme(fig, title=dict(text=f"D{rds} Retention — Daily Trend"), height=500, hovermode='x unified')
                st.plotly_chart(fig, use_container_width=True)

            st.markdown("#### Before vs After Retention")
            bret = rdf[rdf['date'] < TEST_START_DATE]
            aret = rdf[rdf['date'] >= TEST_START_DATE]
            if aret.empty:
                st.info("No post-test retention data yet.")
            else:
                crows = []
                for ver in sorted(rdf['app_version'].unique().tolist(), key=version_sort_key):
                    row = {'Version': ver}
                    for rd in [1, 3, 7, 14]:
                        vb = bret[(bret['app_version'] == ver) & (bret['days_since_install'] == rd)]
                        va = aret[(aret['app_version'] == ver) & (aret['days_since_install'] == rd)]
                        rb, ra = weighted_retention(vb), weighted_retention(va)
                        nb = vb[vb['users_active'] > 0]['cohort_size'].sum()
                        na = va[va['users_active'] > 0]['cohort_size'].sum()
                        row[f'D{rd} Before'] = fmt_pct(rb) if nb > 0 else 'N/A'
                        row[f'D{rd} After'] = fmt_pct(ra) if na > 0 else 'N/A'
                        row[f'D{rd} Delta'] = f"{ra - rb:+.2%}" if nb > 0 and na > 0 else 'N/A'
                    crows.append(row)
                st.dataframe(pd.DataFrame(crows), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
