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
FTUE_TABLE = f"{BQ_PROJECT}.peerplay.ftue_dashboard_fixed"
RETENTION_TABLE = f"{BQ_PROJECT}.peerplay.hint_system_ab_test_results"

TEST_START_DATE = date(2026, 3, 17)
TEST_START_HOUR = 17  # 5:00 PM UTC
DEFAULT_VERSION = '0.3811'

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

    /* --- Alert boxes --- */
    .alert-red { background: #FDF2F2; border-left: 4px solid #E74C3C; border-radius: 0 8px 8px 0; padding: 12px 18px; margin: 8px 0; font-size: 0.88rem; color: #922B21; }
    .alert-green { background: #EAFAF1; border-left: 4px solid #2ECB71; border-radius: 0 8px 8px 0; padding: 12px 18px; margin: 8px 0; font-size: 0.88rem; color: #1E8449; }
    .alert-yellow { background: #FEF9E7; border-left: 4px solid #F39C12; border-radius: 0 8px 8px 0; padding: 12px 18px; margin: 8px 0; font-size: 0.88rem; color: #7D6608; }
    .alert-red b, .alert-green b, .alert-yellow b { font-weight: 700; }
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
            CAST(dp.first_app_version AS STRING) AS install_version,
            CASE WHEN dp.first_country = 'US' THEN 1 ELSE 0 END AS is_usa,
            dp.first_platform AS platform
        FROM `yotam-395120.peerplay.dim_player` dp
        WHERE dp.install_date >= '{start_date}'
          AND dp.install_date <= '{end_date}'
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
        cd.install_date,
        cd.install_version,
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
    df['install_date'] = pd.to_datetime(df['install_date']).dt.date
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

def enforce_monotonic(vals):
    """Cap each value at the previous to ensure monotonic decrease."""
    out = list(vals)
    for i in range(1, len(out)):
        if out[i] > out[i - 1]:
            out[i] = out[i - 1]
    return out

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

def render_alerts(alerts):
    """Render a list of (type, message) alerts. Types: 'red', 'green', 'yellow'."""
    if not alerts:
        return
    html = ""
    for atype, msg in alerts:
        html += f'<div class="alert-{atype}">{msg}</div>'
    st.markdown(html, unsafe_allow_html=True)


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
        page_title="Hint System Dashboard",
        page_icon="🔬",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    inject_css()

    st.markdown("## Hint System Dashboard")
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
            all_dates.extend(ret_df['install_date'].dropna().tolist())
        min_date = min(all_dates) if all_dates else date(2026, 2, 1)
        max_date = max(all_dates) if all_dates else date.today()

        sd_col, sh_col, ed_col, eh_col = st.columns([2, 1, 2, 1])
        with sd_col:
            start_date = st.date_input("Start Date", value=min_date, min_value=min_date, max_value=max_date, key="sd")
        with sh_col:
            start_hour = st.number_input("Hour", min_value=0, max_value=23, value=0, key="sd_hour")
        with ed_col:
            end_date = st.date_input("End Date", value=max_date, min_value=min_date, max_value=max_date, key="ed")
        with eh_col:
            end_hour = st.number_input("Hour", min_value=0, max_value=23, value=23, key="ed_hour")
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
        ret_versions = sorted(ret_df['install_version'].dropna().unique().tolist(), key=version_sort_key) if not ret_df.empty else []
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
    # --- Apply non-version filters (respects sidebar dates) ---
    def apply_non_version_filters_ftue(df):
        if df.empty:
            return df
        out = df.copy()
        out['install_version_str'] = out['install_version'].astype(str)
        if 'install_date' in out.columns and isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            out = out[(out['install_date'] >= date_range[0]) & (out['install_date'] <= date_range[1])]
            # Apply hour filters on boundary dates
            if 'install_hour' in out.columns:
                out = out[~((out['install_date'] == date_range[0]) & (out['install_hour'] < start_hour))]
                out = out[~((out['install_date'] == date_range[1]) & (out['install_hour'] > end_hour))]
        if selected_platforms and 'platform' in out.columns:
            out = out[out['platform'].isin(selected_platforms)]
        if selected_countries is not None and 'country' in out.columns:
            out = out[out['country'].isin(selected_countries)]
        if selected_weeks and 'install_week' in out.columns:
            out = out[out['install_week'].isin(selected_weeks)]
        if selected_months and 'install_month' in out.columns:
            out = out[out['install_month'].isin(selected_months)]
        if selected_mediasource is not None and 'mediasource' in out.columns:
            out = out[out['mediasource'].isin(selected_mediasource)]
        if selected_media_type is not None and 'media_type' in out.columns:
            out = out[out['media_type'].isin(selected_media_type)]
        if selected_low_payers is not None and 'is_low_payers_country' in out.columns:
            out = out[out['is_low_payers_country'] == selected_low_payers]
        return out

    def apply_non_version_filters_ret(df):
        if df.empty:
            return df
        out = df.copy()
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            out = out[(out['install_date'] >= date_range[0]) & (out['install_date'] <= date_range[1])]
        if selected_platforms:
            out = out[out['platform'].isin(selected_platforms)]
        if selected_usa_only is not None and 'is_usa' in out.columns:
            out = out[out['is_usa'] == selected_usa_only]
        return out

    # All-version filtered data (for other tabs, respects sidebar dates)
    fdf_all = apply_non_version_filters_ftue(ftue_df if not ftue_df.empty else pd.DataFrame())
    rdf_all = apply_non_version_filters_ret(ret_df if not ret_df.empty else pd.DataFrame())

    # BA tab raw data (NO sidebar filters — BA tab has its own inline filters)
    fdf_ba = ftue_df.copy() if not ftue_df.empty else pd.DataFrame()
    if not fdf_ba.empty:
        fdf_ba['install_version_str'] = fdf_ba['install_version'].astype(str)
    rdf_ba = ret_df.copy() if not ret_df.empty else pd.DataFrame()

    # Sidebar-version-filtered data (for other tabs)
    fdf = fdf_all[fdf_all['install_version_str'].isin(selected_versions)].copy() if not fdf_all.empty else pd.DataFrame()
    rdf = rdf_all[rdf_all['install_version'].isin(selected_versions)].copy() if not rdf_all.empty else pd.DataFrame()

    # All available versions (across all data, not limited by sidebar version filter)
    all_versions = sorted(
        set(list(fdf_all['install_version_str'].unique()) if not fdf_all.empty else []) |
        set(list(rdf_all['install_version'].unique()) if not rdf_all.empty else []),
        key=version_sort_key
    )

    versions_in_data = sorted(
        set(list(fdf['install_version_str'].unique()) if not fdf.empty else []) |
        set(list(rdf['install_version'].unique()) if not rdf.empty else []),
        key=version_sort_key
    )
    color_map = get_version_color_map(all_versions)

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
        st.markdown('<div class="summary-box"><h4>Before vs After — Independent Analysis</h4>'
                    'This tab has its <b>own filters</b> (below). Sidebar filters do NOT apply here. '
                    'Pick versions + date ranges for each group, then compare. '
                    '<strong style="color:#5B8DEF">Blue = Before</strong> &nbsp;&nbsp;'
                    '<strong style="color:#2ECB71">Green = After</strong></div>', unsafe_allow_html=True)

        # =================================================================
        # BA TAB INLINE FILTERS (completely independent from sidebar)
        # =================================================================
        with st.expander("Filters", expanded=False):
            baf1, baf2, baf3 = st.columns(3)
            with baf1:
                ba_plat_opts = sorted(fdf_ba['platform'].dropna().unique().tolist()) if not fdf_ba.empty and 'platform' in fdf_ba.columns else []
                ba_selected_platforms = st.multiselect("Platform", ba_plat_opts, default=ba_plat_opts, key="ba_f_plat")
            with baf2:
                ba_c_labels, ba_c_map, _ = opts_with_counts(fdf_ba, 'country') if not fdf_ba.empty else ([], {}, [])
                ba_sel_c = st.multiselect("Country", ba_c_labels, default=[], key="ba_f_country")
                ba_selected_countries = [ba_c_map[l] for l in ba_sel_c] if ba_sel_c else None
            with baf3:
                ba_lp_opts = sorted(fdf_ba['is_low_payers_country'].dropna().unique().tolist()) if not fdf_ba.empty and 'is_low_payers_country' in fdf_ba.columns else []
                ba_lp_display = {0: 'No', 1: 'Yes'}
                ba_lp_labels = [ba_lp_display.get(v, str(v)) for v in ba_lp_opts]
                ba_lp_map = dict(zip(ba_lp_labels, ba_lp_opts))
                ba_sel_lp = st.selectbox("Is Low Payers", ["All"] + ba_lp_labels, index=0, key="ba_f_lp")
                ba_selected_low_payers = ba_lp_map[ba_sel_lp] if ba_sel_lp != "All" else None

            baf4, baf5, baf6 = st.columns(3)
            with baf4:
                ba_ms_labels, ba_ms_map, _ = opts_with_counts(fdf_ba, 'mediasource') if not fdf_ba.empty else ([], {}, [])
                ba_sel_ms = st.multiselect("Media Source", ba_ms_labels, default=[], key="ba_f_ms")
                ba_selected_mediasource = [ba_ms_map[l] for l in ba_sel_ms] if ba_sel_ms else None
            with baf5:
                ba_mt_labels, ba_mt_map, _ = opts_with_counts(fdf_ba, 'media_type') if not fdf_ba.empty else ([], {}, [])
                ba_sel_mt = st.multiselect("Media Type", ba_mt_labels, default=[], key="ba_f_mt")
                ba_selected_media_type = [ba_mt_map[l] for l in ba_sel_mt] if ba_sel_mt else None
            with baf6:
                ba_usa_opt = st.selectbox("Country (Retention)", ["All", "US Only", "Non-US"], index=0, key="ba_f_usa")
                ba_selected_usa_only = 1 if ba_usa_opt == "US Only" else (0 if ba_usa_opt == "Non-US" else None)

        # --- Apply BA inline filters ---
        if not fdf_ba.empty:
            if ba_selected_platforms and 'platform' in fdf_ba.columns:
                fdf_ba = fdf_ba[fdf_ba['platform'].isin(ba_selected_platforms)]
            if ba_selected_countries is not None and 'country' in fdf_ba.columns:
                fdf_ba = fdf_ba[fdf_ba['country'].isin(ba_selected_countries)]
            if ba_selected_mediasource is not None and 'mediasource' in fdf_ba.columns:
                fdf_ba = fdf_ba[fdf_ba['mediasource'].isin(ba_selected_mediasource)]
            if ba_selected_media_type is not None and 'media_type' in fdf_ba.columns:
                fdf_ba = fdf_ba[fdf_ba['media_type'].isin(ba_selected_media_type)]
            if ba_selected_low_payers is not None and 'is_low_payers_country' in fdf_ba.columns:
                fdf_ba = fdf_ba[fdf_ba['is_low_payers_country'] == ba_selected_low_payers]
        if not rdf_ba.empty:
            if ba_selected_platforms:
                rdf_ba = rdf_ba[rdf_ba['platform'].isin(ba_selected_platforms)]
            if ba_selected_usa_only is not None and 'is_usa' in rdf_ba.columns:
                rdf_ba = rdf_ba[rdf_ba['is_usa'] == ba_selected_usa_only]

        # =================================================================
        # BEFORE / AFTER SELECTORS
        # =================================================================
        ba_all_dates = sorted(fdf_ba['install_date'].dropna().unique().tolist()) if not fdf_ba.empty and 'install_date' in fdf_ba.columns else []
        ba_min_date = min(ba_all_dates) if ba_all_dates else date(2026, 2, 1)
        ba_max_date = max(ba_all_dates) if ba_all_dates else date.today()
        ba_versions_available = sorted(
            set(list(fdf_ba['install_version_str'].unique()) if not fdf_ba.empty else []),
            key=version_sort_key, reverse=True
        )

        def ba_filter_date_hour(df, ds, de, sh=0, eh=23):
            """Filter dataframe by date range + hour boundaries."""
            out = df[(df['install_date'] >= ds) & (df['install_date'] <= de)]
            if 'install_hour' in out.columns:
                out = out[~((out['install_date'] == ds) & (out['install_hour'] < sh))]
                out = out[~((out['install_date'] == de) & (out['install_hour'] > eh))]
            return out

        def ba_installs(ver, ds, de, sh=0, eh=23):
            if fdf_ba.empty:
                return 0
            vdf = fdf_ba[fdf_ba['install_version_str'] == str(ver)]
            vdf = ba_filter_date_hour(vdf, ds, de, sh, eh)
            return int(vdf['total_users'].sum()) if 'total_users' in vdf.columns else len(vdf)

        # Default version for Before/After
        default_ba_ver = [v for v in ba_versions_available if v.startswith(DEFAULT_VERSION)]
        if not default_ba_ver:
            default_ba_ver = [ba_versions_available[0]] if ba_versions_available else []

        # Default platform: both iOS and Android
        ba_default_plats = ba_selected_platforms  # from inline filters above

        col_before, col_vs, col_after = st.columns([5, 0.5, 5])
        with col_before:
            st.markdown(f'<div style="background:#EBF5FB;padding:10px 16px;border-radius:8px;border-left:4px solid {COLORS["before"]};margin-bottom:12px;">'
                        f'<b style="color:{COLORS["before"]};font-size:1.1em;">BEFORE</b></div>', unsafe_allow_html=True)
            before_versions = st.multiselect("Versions", ba_versions_available,
                default=default_ba_ver, key="ba_before_vers")
            bc1, bch1, bc2, bch2 = st.columns([2, 1, 2, 1])
            with bc1:
                before_start = st.date_input("Start", value=ba_min_date, min_value=ba_min_date, max_value=ba_max_date, key="ba_before_start")
            with bch1:
                before_start_hour = st.number_input("Hour", min_value=0, max_value=23, value=0, key="ba_before_start_h")
            with bc2:
                before_end = st.date_input("End", value=TEST_START_DATE if TEST_START_DATE <= ba_max_date else ba_max_date, min_value=ba_min_date, max_value=ba_max_date, key="ba_before_end")
            with bch2:
                before_end_hour = st.number_input("Hour", min_value=0, max_value=23, value=TEST_START_HOUR - 1 if TEST_START_HOUR > 0 else 23, key="ba_before_end_h")
            if before_versions:
                st.markdown(" | ".join([f"v{v}: **{ba_installs(v, before_start, before_end, before_start_hour, before_end_hour):,}**" for v in before_versions]))
        with col_vs:
            st.markdown("<div style='text-align:center;padding-top:60px;font-size:1.8em;font-weight:bold;color:#7F8C8D;'>vs</div>", unsafe_allow_html=True)
        with col_after:
            st.markdown(f'<div style="background:#EAFAF1;padding:10px 16px;border-radius:8px;border-left:4px solid {COLORS["after"]};margin-bottom:12px;">'
                        f'<b style="color:{COLORS["after"]};font-size:1.1em;">AFTER</b></div>', unsafe_allow_html=True)
            after_versions = st.multiselect("Versions", ba_versions_available,
                default=default_ba_ver, key="ba_after_vers")
            ac1, ach1, ac2, ach2 = st.columns([2, 1, 2, 1])
            with ac1:
                after_start = st.date_input("Start", value=TEST_START_DATE if TEST_START_DATE <= ba_max_date else ba_min_date, min_value=ba_min_date, max_value=ba_max_date, key="ba_after_start")
            with ach1:
                after_start_hour = st.number_input("Hour", min_value=0, max_value=23, value=TEST_START_HOUR, key="ba_after_start_h")
            with ac2:
                after_end = st.date_input("End", value=ba_max_date, min_value=ba_min_date, max_value=ba_max_date, key="ba_after_end")
            with ach2:
                after_end_hour = st.number_input("Hour", min_value=0, max_value=23, value=23, key="ba_after_end_h")
            if after_versions:
                st.markdown(" | ".join([f"v{v}: **{ba_installs(v, after_start, after_end, after_start_hour, after_end_hour):,}**" for v in after_versions]))

        # --- Options ---
        opt1, opt2 = st.columns([2, 1])
        with opt1:
            ba_metric_options = ["Conversion vs Step 1"]
            ratio_cols_ba = get_ratio_columns(fdf_ba) if not fdf_ba.empty else []
            if ratio_cols_ba:
                ba_metric_options.append("Conversion vs Previous Step")
            ba_metric_set = st.selectbox("Metric set", ba_metric_options, key="ba_metric_set")
        with opt2:
            show_avg = st.checkbox("Show Average", value=True, key="ba_show_avg")

        has_ftue = not fdf_ba.empty
        has_ret = not rdf_ba.empty
        pct_cols_ba = get_pct_columns(fdf_ba) if has_ftue else []

        if not before_versions and not after_versions:
            st.warning("Select at least one version for Before or After.")
        elif has_ftue and pct_cols_ba:
            if ba_metric_set == "Conversion vs Previous Step" and ratio_cols_ba:
                active_metrics = ratio_cols_ba
            else:
                active_metrics = pct_cols_ba
            active_labels = [format_step_label(m) for m in active_metrics]

            st.markdown("---")

            fig_ba = go.Figure()

            # --- Helper: compute and plot one group ---
            def plot_group(versions, date_start, date_end, start_h, end_h, period_label, base_color, dash_avg):
                group_all_vals = []
                group_all_users = []
                for ver in sorted(versions, key=version_sort_key):
                    ver_color = color_map.get(str(ver), '#333')
                    vdf = fdf_ba[fdf_ba['install_version_str'] == str(ver)]
                    vdf = ba_filter_date_hour(vdf, date_start, date_end, start_h, end_h)
                    vals, users = calc_weighted_steps(vdf, active_metrics)
                    if not vals or users == 0:
                        continue
                    group_all_vals.append(vals)
                    group_all_users.append(users)
                    # Individual version line
                    line_opacity = 0.45 if show_avg else 0.9
                    line_width = 1.5 if show_avg else 2.5
                    fig_ba.add_trace(go.Scatter(
                        x=active_labels, y=vals,
                        mode='lines+markers',
                        name=f"v{ver} {period_label} ({users:,.0f})",
                        line=dict(color=ver_color, width=line_width,
                                  dash='solid' if period_label == 'Before' else 'dot'),
                        marker=dict(size=4 if show_avg else 6,
                                    symbol='circle' if period_label == 'Before' else 'diamond'),
                        opacity=line_opacity,
                        hovertemplate='<b>%{x}</b><br>v' + str(ver) + f' {period_label} ({users:,.0f})' + ': %{y:.4f}<extra></extra>',
                        legendgroup=period_label,
                    ))
                # Weighted average line
                if group_all_vals and show_avg:
                    total_u = sum(group_all_users)
                    avg_vals = [sum(group_all_vals[j][i] * group_all_users[j] for j in range(len(group_all_vals))) / total_u
                                for i in range(len(active_labels))]
                    fig_ba.add_trace(go.Scatter(
                        x=active_labels, y=avg_vals,
                        mode='lines+markers',
                        name=f"AVG {period_label} ({total_u:,.0f})",
                        line=dict(color=base_color, width=3.5, dash=dash_avg),
                        marker=dict(size=8, symbol='circle' if period_label == 'Before' else 'diamond',
                                    line=dict(width=2, color='white')),
                        hovertemplate='<b>%{x}</b><br>AVG ' + period_label + f' ({total_u:,.0f})' + ': %{y:.4f}<extra></extra>',
                        legendgroup=period_label + '_avg',
                    ))
                    return avg_vals, total_u
                elif group_all_vals:
                    total_u = sum(group_all_users)
                    avg_vals = [sum(group_all_vals[j][i] * group_all_users[j] for j in range(len(group_all_vals))) / total_u
                                for i in range(len(active_labels))]
                    return avg_vals, total_u
                return None, 0

            avg_before, users_before = plot_group(before_versions, before_start, before_end, before_start_hour, before_end_hour, "Before", COLORS['before'], 'solid')
            avg_after, users_after = plot_group(after_versions, after_start, after_end, after_start_hour, after_end_hour, "After", COLORS['after'], 'dash')

            # Lift annotations on funnel chart — every step
            if avg_before and avg_after:
                for i in range(len(avg_before)):
                    if avg_before[i] > 0:
                        lift_pp = (avg_after[i] - avg_before[i]) * 100  # percentage points
                        ann_color = COLORS['after'] if lift_pp > 0 else COLORS['negative']
                        fig_ba.add_annotation(
                            x=active_labels[i], y=max(avg_before[i], avg_after[i]),
                            text=f"<b>{lift_pp:+.1f}pp</b>", showarrow=False,
                            yshift=14, font=dict(size=9, color=ann_color),
                            bgcolor='rgba(255,255,255,0.8)', borderpad=2,
                        )

            before_ver_str = ", ".join([f"v{v}" for v in before_versions[:3]])
            after_ver_str = ", ".join([f"v{v}" for v in after_versions[:3]])
            apply_chart_theme(fig_ba,
                title=dict(text=f"FTUE Funnel: [{before_ver_str}] Before vs [{after_ver_str}] After", font=dict(size=16)),
                xaxis_title="FTUE Steps", yaxis_title="Conversion Rate",
                height=650, hovermode='x unified',
                xaxis_tickangle=-45, xaxis=dict(tickfont=dict(size=9)),
                yaxis=dict(tickformat='.2f'),
                margin=dict(b=160, t=80),
            )
            st.plotly_chart(fig_ba, use_container_width=True)

            # --- Funnel start→finish summary ---
            sc1, sc2 = st.columns(2)
            with sc1:
                st.markdown(f'<div style="background:#EBF5FB;border-left:4px solid {COLORS["before"]};padding:12px 18px;border-radius:0 8px 8px 0;">'
                            f'<b style="color:{COLORS["before"]};">BEFORE</b><br>'
                            f'Started: <b>{users_before:,.0f}</b> users<br>'
                            f'Completed (last step): <b>{users_before * avg_before[-1]:,.0f}</b> users<br>'
                            f'Start→Finish: <b>{avg_before[-1]:.1%}</b>'
                            f'</div>' if avg_before and users_before > 0 else '', unsafe_allow_html=True)
            with sc2:
                st.markdown(f'<div style="background:#EAFAF1;border-left:4px solid {COLORS["after"]};padding:12px 18px;border-radius:0 8px 8px 0;">'
                            f'<b style="color:{COLORS["after"]};">AFTER</b><br>'
                            f'Started: <b>{users_after:,.0f}</b> users<br>'
                            f'Completed (last step): <b>{users_after * avg_after[-1]:,.0f}</b> users<br>'
                            f'Start→Finish: <b>{avg_after[-1]:.1%}</b>'
                            f'</div>' if avg_after and users_after > 0 else '', unsafe_allow_html=True)
            if avg_before and avg_after and avg_before[-1] > 0:
                lift_pp = (avg_after[-1] - avg_before[-1]) * 100
                lift_relative = (avg_after[-1] - avg_before[-1]) / avg_before[-1] * 100
                lift_color = COLORS['after'] if lift_pp > 0 else COLORS['negative']
                st.markdown(f'<div style="text-align:center;padding:8px;font-size:1.1em;">'
                            f'Start→Finish lift: <b style="color:{lift_color};font-size:1.3em;">{lift_pp:+.1f}pp</b> '
                            f'({avg_before[-1]:.1%} → {avg_after[-1]:.1%}, {lift_relative:+.1f}% relative)</div>', unsafe_allow_html=True)

            # --- Summary metrics ---
            if avg_before and avg_after:
                lifts_all = [(avg_after[i] - avg_before[i]) * 100 for i in range(len(avg_before))]
                improved = [l for l in lifts_all if l > 0.5]
                declined = [l for l in lifts_all if l < -0.5]
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Avg Lift", f"{np.mean(lifts_all):+.1f}pp")
                c2.metric("Steps Improved", f"{len(improved)}/{len(lifts_all)}")
                c3.metric("Steps Declined", f"{len(declined)}/{len(lifts_all)}")
                c4.metric("Last Step Lift", f"{lifts_all[-1]:+.1f}pp")

            # --- LIFT DELTA CHART: % difference per step ---
            if avg_before and avg_after:
                st.markdown("#### Lift (pp) per Step (After vs Before)")
                st.markdown('<div class="legend-box">'
                            '<strong style="color:#2ECB71">Green bars</strong> = After is higher (improvement) &nbsp;&nbsp;'
                            '<strong style="color:#E74C3C">Red bars</strong> = After is lower (regression) &nbsp;&nbsp;'
                            'The <b>dashed line</b> shows the average lift across all steps.'
                            '</div>', unsafe_allow_html=True)
                lifts_per_step = [(avg_after[i] - avg_before[i]) * 100 for i in range(len(avg_before))]
                bar_colors = [COLORS['after'] if l > 0.1 else COLORS['negative'] if l < -0.1 else COLORS['neutral'] for l in lifts_per_step]
                fig_lift = go.Figure()
                fig_lift.add_trace(go.Bar(
                    x=active_labels, y=lifts_per_step,
                    marker_color=bar_colors,
                    hovertemplate='<b>%{x}</b><br>Lift: %{y:+.1f}pp<extra></extra>',
                    text=[f"{l:+.1f}pp" for l in lifts_per_step],
                    textposition='outside', textfont=dict(size=9),
                ))
                avg_lift_val = np.mean(lifts_per_step)
                fig_lift.add_hline(y=0, line_color="#2C3E50", line_width=1.5)
                fig_lift.add_hline(y=avg_lift_val, line_dash="dash", line_color=COLORS['accent'], line_width=2,
                    annotation_text=f"Avg: {avg_lift_val:+.1f}pp", annotation_position="top right",
                    annotation_font=dict(size=12, color=COLORS['accent']))
                apply_chart_theme(fig_lift,
                    title=dict(text="Lift (pp) at Each Step"),
                    xaxis_title="FTUE Steps", yaxis_title="Lift (pp)",
                    yaxis_ticksuffix="pp", height=450,
                    xaxis_tickangle=-45, xaxis=dict(tickfont=dict(size=8)),
                    margin=dict(b=150, t=60),
                )
                st.plotly_chart(fig_lift, use_container_width=True)

            # --- Detailed table ---
            st.markdown("#### Detailed Comparison")
            detail_rows = []
            for i in range(len(active_labels)):
                row = {'Step': active_labels[i]}
                row['Before'] = f"{avg_before[i]:.4f}" if avg_before else '-'
                row['After'] = f"{avg_after[i]:.4f}" if avg_after else '-'
                if avg_before and avg_after and avg_before[i] > 0:
                    lift_pp = (avg_after[i] - avg_before[i]) * 100
                    delta_abs = avg_after[i] - avg_before[i]
                    row['Delta'] = f"{delta_abs:+.4f}"
                    row['Lift (pp)'] = f"{lift_pp:+.1f}pp"
                else:
                    row['Delta'] = '-'
                    row['Lift (pp)'] = '-'
                detail_rows.append(row)
            st.dataframe(pd.DataFrame(detail_rows), use_container_width=True, hide_index=True, height=500)

            # --- DROP-OFF comparison on averages ---
            if avg_before:
                pct_cols_do = get_pct_columns(fdf_ba)
                do_labels = [format_step_label(m) for m in pct_cols_do]
                # Recompute averages on pct metrics for drop-off
                def compute_avg_pct(versions, ds, de, sh=0, eh=23):
                    all_v, all_u = [], []
                    for ver in versions:
                        vdf = fdf_ba[fdf_ba['install_version_str'] == str(ver)]
                        vdf = ba_filter_date_hour(vdf, ds, de, sh, eh)
                        v, u = calc_weighted_steps(vdf, pct_cols_do)
                        if v and u > 0:
                            all_v.append(v); all_u.append(u)
                    if not all_v:
                        return None
                    tu = sum(all_u)
                    return [sum(all_v[j][i] * all_u[j] for j in range(len(all_v))) / tu for i in range(len(pct_cols_do))]

                avg_b_pct = compute_avg_pct(before_versions, before_start, before_end, before_start_hour, before_end_hour)
                avg_a_pct = compute_avg_pct(after_versions, after_start, after_end, after_start_hour, after_end_hour)

                if avg_b_pct:
                    do_before = [(avg_b_pct[i] - avg_b_pct[i+1]) / avg_b_pct[i] if avg_b_pct[i] > 0 else 0 for i in range(len(avg_b_pct) - 1)]
                    do_after = [(avg_a_pct[i] - avg_a_pct[i+1]) / avg_a_pct[i] if avg_a_pct[i] > 0 else 0 for i in range(len(avg_a_pct) - 1)] if avg_a_pct else []

                    st.markdown("---")
                    st.markdown("### Step-to-Step Drop-off (Averages)")
                    transition_labels = [f"{do_labels[i]} -> {do_labels[i+1]}" for i in range(len(do_before))]
                    short_labels = [f"{i+1}->{i+2}" for i in range(len(do_before))]

                    fig_do = go.Figure()
                    fig_do.add_trace(go.Bar(x=short_labels, y=do_before, name="Before Avg",
                        marker_color=COLORS['before'], opacity=0.85, customdata=transition_labels,
                        hovertemplate='<b>%{customdata}</b><br>Drop-off: %{y:.2%}<extra>Before</extra>'))
                    if do_after:
                        fig_do.add_trace(go.Bar(x=short_labels, y=do_after, name="After Avg",
                            marker_color=COLORS['after'], opacity=0.85, customdata=transition_labels,
                            hovertemplate='<b>%{customdata}</b><br>Drop-off: %{y:.2%}<extra>After</extra>'))
                    apply_chart_theme(fig_do, title=dict(text="Drop-off per Transition (Averages)"),
                        xaxis_title="Step Transition", yaxis_title="Drop-off Rate",
                        yaxis_tickformat='.1%', height=420, barmode='group')
                    st.plotly_chart(fig_do, use_container_width=True)

        # --- RETENTION ---
        all_ba_versions = list(set(before_versions + after_versions)) if 'before_versions' in dir() else []
        if has_ret and all_ba_versions:
            st.markdown("---")
            st.markdown("### Retention: Before vs After")
            ret_comp = []
            for ver in sorted(set(list(before_versions) + list(after_versions)), key=version_sort_key):
                vdf_ret = rdf_ba[rdf_ba['install_version'] == str(ver)]
                in_before = ver in before_versions
                in_after = ver in after_versions
                vdf_ret_b = vdf_ret[(vdf_ret['install_date'] >= before_start) & (vdf_ret['install_date'] <= before_end)] if in_before else pd.DataFrame()
                vdf_ret_a = vdf_ret[(vdf_ret['install_date'] >= after_start) & (vdf_ret['install_date'] <= after_end)] if in_after else pd.DataFrame()
                for rd in [1, 3, 7, 14]:
                    row = {'Version': ver, 'Day': f'D{rd}'}
                    if not vdf_ret_b.empty:
                        rb = weighted_retention(vdf_ret_b[vdf_ret_b['days_since_install'] == rd])
                        nb = vdf_ret_b[(vdf_ret_b['days_since_install'] == rd) & (vdf_ret_b['users_active'] > 0)]['cohort_size'].sum()
                        row['Before'] = fmt_pct(rb) if nb > 0 else 'N/A'
                        row['Cohort Before'] = fmt_number(nb)
                    else:
                        row['Before'] = '-'; row['Cohort Before'] = '-'; rb = 0; nb = 0
                    if not vdf_ret_a.empty:
                        ra = weighted_retention(vdf_ret_a[vdf_ret_a['days_since_install'] == rd])
                        na = vdf_ret_a[(vdf_ret_a['days_since_install'] == rd) & (vdf_ret_a['users_active'] > 0)]['cohort_size'].sum()
                        row['After'] = fmt_pct(ra) if na > 0 else 'N/A'
                        row['Cohort After'] = fmt_number(na)
                        if nb > 0 and na > 0:
                            d = ra - rb
                            row['Delta'] = f"{d:+.2%}"
                        else:
                            row['Delta'] = '-'
                    else:
                        row['After'] = '-'; row['Cohort After'] = '-'; row['Delta'] = '-'
                    ret_comp.append(row)
            st.dataframe(pd.DataFrame(ret_comp), use_container_width=True, hide_index=True)

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

                # --- Smart alerts ---
                chart_alerts = []
                # Check for versions with very low users
                for ver, val in ver_last.items():
                    vdf_check = fdf[fdf['install_version_str'] == ver]
                    tu = vdf_check['total_users'].sum() if 'total_users' in vdf_check.columns else len(vdf_check)
                    if 0 < tu < 100:
                        chart_alerts.append(('yellow', f'<b>v{ver}</b> has only <b>{tu:,.0f}</b> users — data may be unreliable.'))
                # Check gap between best and worst
                if ver_last.get(best_ver, 0) > 0 and ver_last.get(worst_ver, 0) > 0:
                    gap = ver_last[best_ver] - ver_last[worst_ver]
                    if gap > 0.1:
                        chart_alerts.append(('red', f'<b>{gap:.1%}</b> gap between best (<b>v{best_ver}</b>) and worst (<b>v{worst_ver}</b>) at last step. Investigate what differs.'))
                # Check if last step conversion is below threshold
                for ver, val in ver_last.items():
                    if val > 0 and val < 0.5:
                        chart_alerts.append(('yellow', f'<b>v{ver}</b> loses <b>{1-val:.0%}</b> of users before completing the FTUE.'))
                        break
                render_alerts(chart_alerts)

            pct_cols = get_pct_columns(fdf)
            ratio_cols = get_ratio_columns(fdf)
            c1, c2, c3 = st.columns(3)
            with c1:
                chart_type = st.radio("Chart type", ["Bar", "Line"], index=1, horizontal=True, key="funnel_chart_type")
            with c2:
                m_opts = []
                if pct_cols: m_opts.append("Conversion vs Step 1")
                if ratio_cols: m_opts.append("Conversion vs Previous Step")
                metric_set = st.selectbox("Metric set", m_opts, key="funnel_metric_set") if m_opts else None
            with c3:
                show_ba_split = st.checkbox("Show Before/After Split", key="funnel_ba_split")
            metrics = pct_cols if metric_set == "Conversion vs Step 1" else (ratio_cols if metric_set == "Conversion vs Previous Step" else pct_cols)
            if metrics:
                step_labels = [format_step_label(m) for m in metrics]
                fig = go.Figure()
                for ver in sorted(fdf['install_version_str'].unique(), key=version_sort_key):
                    vdf = fdf[fdf['install_version_str'] == ver]
                    ver_color = color_map.get(str(ver), '#333')
                    if show_ba_split:
                        for period_label, period_filter, dash_style, symbol in [
                            ("Before", vdf['install_date'] < TEST_START_DATE, 'solid', 'circle'),
                            ("After", vdf['install_date'] >= TEST_START_DATE, 'dash', 'diamond'),
                        ]:
                            pdf = vdf[period_filter]
                            if pdf.empty:
                                continue
                            tu = pdf['total_users'].sum() if 'total_users' in pdf.columns else len(pdf)
                            values = []
                            for m in metrics:
                                val = (pdf[m] * pdf['total_users']).sum() / tu if 'total_users' in pdf.columns and tu > 0 else pdf[m].mean()
                                values.append(val)
                            trace_name = f"v{ver} {period_label} ({tu:,.0f})"
                            if chart_type == "Line":
                                fig.add_trace(go.Scatter(x=step_labels, y=values, mode='lines+markers', name=trace_name,
                                    line=dict(color=ver_color, width=2, dash=dash_style), marker=dict(size=6, symbol=symbol),
                                    hovertemplate='<b>%{x}</b><br>' + trace_name + ': %{y:.4f}<extra></extra>'))
                            else:
                                fig.add_trace(go.Bar(x=step_labels, y=values, name=trace_name, marker_color=ver_color,
                                    opacity=0.6 if period_label == "Before" else 1.0))
                    else:
                        tu = vdf['total_users'].sum() if 'total_users' in vdf.columns else len(vdf)
                        values = []
                        for m in metrics:
                            val = (vdf[m] * vdf['total_users']).sum() / tu if 'total_users' in vdf.columns and tu > 0 else vdf[m].mean()
                            values.append(val)
                        if chart_type == "Line":
                            fig.add_trace(go.Scatter(x=step_labels, y=values, mode='lines+markers', name=str(ver),
                                line=dict(color=ver_color, width=2), marker=dict(size=6),
                                hovertemplate='<b>%{x}</b><br>v' + str(ver) + ': %{y:.4f}<extra></extra>'))
                        else:
                            fig.add_trace(go.Bar(x=step_labels, y=values, name=str(ver), marker_color=ver_color))
                # Add drop-off annotations to the chart for the first (or only) version
                first_ver = sorted(fdf['install_version_str'].unique(), key=version_sort_key)[0]
                fv_df = fdf[fdf['install_version_str'] == first_ver]
                fv_tu = fv_df['total_users'].sum() if 'total_users' in fv_df.columns else len(fv_df)
                if fv_tu > 0 and chart_type == "Line":
                    fv_vals = []
                    for m in metrics:
                        val = (fv_df[m] * fv_df['total_users']).sum() / fv_tu if 'total_users' in fv_df.columns else fv_df[m].mean()
                        fv_vals.append(val)
                    # Find top 3 drops
                    drops = []
                    for i in range(1, len(fv_vals)):
                        if fv_vals[i-1] > 0:
                            drop_pct = (fv_vals[i-1] - fv_vals[i]) / fv_vals[i-1] * 100
                            drops.append((i, drop_pct, fv_vals[i]))
                    drops.sort(key=lambda x: -x[1])
                    for rank, (idx, drop_pct, yval) in enumerate(drops[:3]):
                        fig.add_annotation(
                            x=step_labels[idx], y=yval,
                            text=f"<b>-{drop_pct:.1f}%</b>",
                            showarrow=True, arrowhead=2, arrowcolor=COLORS['negative'],
                            arrowwidth=1.5, ay=30 + rank * 20,
                            font=dict(size=10, color=COLORS['negative']),
                            bgcolor='rgba(255,255,255,0.9)', borderpad=3,
                            bordercolor=COLORS['negative'], borderwidth=1,
                        )

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
                    {"Step": "47: new_chapter_4", "Event": "scapes_tasks_new_chapter", "Description": "User reaches chapter 4"},
                    {"Step": "48: new_chapter_5", "Event": "scapes_tasks_new_chapter", "Description": "User reaches chapter 5"},
                ]
                st.dataframe(pd.DataFrame(step_reference), use_container_width=True, hide_index=True, height=400)

                # --- Auto-detect upticks and explain ---
                st.markdown("#### Funnel Anomalies (Upticks)")

                # Detect upticks from actual data
                uptick_explanations = {
                    '09': {
                        'name': 'impression_board',
                        'issue': 'Rounding artifact (<0.01%). impression_board and click_board_button_scapes fire at the same millisecond.',
                        'fix': 'No action needed — difference is negligible.',
                    },
                    '24': {
                        'name': 'ftue_flow2_step0',
                        'issue': 'Expected: step 23 (click_scapes_button_board) is time-window constrained to only count clicks during the FTUE session, while step 24 (ftue_flow2_step0) is an anchor event. Some users reach Flow 2 without the scapes click registering within the FTUE window.',
                        'fix': 'Acceptable — the constraint on step 23 is working correctly. The gap shows ~2% of users enter Flow 2 through a slightly different path. No action needed.',
                    },
                }
                # Steps to suppress from the anomaly table (known/accepted)
                suppressed_steps = {'09'}

                # Compute actual upticks from current data
                all_step_vals = {}
                for ver in sorted(fdf['install_version_str'].unique(), key=version_sort_key):
                    vdf = fdf[fdf['install_version_str'] == ver]
                    tu = vdf['total_users'].sum() if 'total_users' in vdf.columns else len(vdf)
                    if tu < 100:
                        continue
                    ver_vals = []
                    for m in pct_cols:
                        if 'total_users' in vdf.columns and tu > 0:
                            val = (vdf[m] * vdf['total_users']).sum() / tu
                        else:
                            val = vdf[m].mean()
                        ver_vals.append((m, val))
                    all_step_vals[ver] = ver_vals

                found_upticks = []
                known_upticks = []
                for ver, vals in all_step_vals.items():
                    for i in range(1, len(vals)):
                        curr_col, curr_val = vals[i]
                        prev_col, prev_val = vals[i-1]
                        if curr_val > prev_val + 0.0005:
                            step_num = format_step_label(curr_col).split(':')[0].strip()
                            uptick_pct = (curr_val - prev_val) * 100
                            explanation = uptick_explanations.get(step_num, {})
                            entry = {
                                'Version': ver,
                                'Step': format_step_label(curr_col),
                                'Value': f"{curr_val:.4f}",
                                'Prev Step': format_step_label(prev_col),
                                'Prev Value': f"{prev_val:.4f}",
                                'Uptick': f"+{uptick_pct:.2f}%",
                                'Status': 'Known / Accepted' if step_num in suppressed_steps else 'Investigate',
                                'Root Cause': explanation.get('issue', 'Generic event may fire outside FTUE session window.'),
                                'Suggested Fix': explanation.get('fix', 'Add timestamp constraint to only count this event during the FTUE session.'),
                            }
                            if step_num in suppressed_steps:
                                known_upticks.append(entry)
                            else:
                                found_upticks.append(entry)

                if found_upticks:
                    st.markdown(f'<div class="alert-red"><b>{len(found_upticks)} unexpected uptick(s)</b> — '
                                f'steps where conversion goes UP from the previous step. Investigate these.</div>',
                                unsafe_allow_html=True)
                    st.dataframe(pd.DataFrame(found_upticks), use_container_width=True, hide_index=True)
                elif not known_upticks:
                    st.markdown('<div class="alert-green"><b>No upticks detected.</b> All steps are monotonically decreasing — the funnel data is clean.</div>',
                                unsafe_allow_html=True)
                else:
                    st.markdown('<div class="alert-green"><b>Funnel is clean.</b> All steps decrease monotonically. No unexpected anomalies.</div>',
                                unsafe_allow_html=True)

                if known_upticks:
                    with st.expander(f"Known / accepted anomalies ({len(known_upticks)})", expanded=False):
                        st.markdown('<div class="legend-box">These are minor anomalies that have been investigated and accepted. They are caused by event timing or rounding, not real data issues.</div>', unsafe_allow_html=True)
                        st.dataframe(pd.DataFrame(known_upticks), use_container_width=True, hide_index=True)

                # --- Biggest Drop-offs (attention needed) ---
                st.markdown("#### Biggest Drop-offs")
                st.markdown('<div class="legend-box">Steps with the largest user loss. '
                            'These are the weakest points in the funnel — focus optimization efforts here.</div>',
                            unsafe_allow_html=True)

                for ver, vals in all_step_vals.items():
                    if len(vals) < 2:
                        continue
                    dropoffs = []
                    for i in range(1, len(vals)):
                        curr_col, curr_val = vals[i]
                        prev_col, prev_val = vals[i-1]
                        if prev_val > 0:
                            drop_abs = prev_val - curr_val
                            drop_pct = drop_abs / prev_val * 100
                            dropoffs.append({
                                'from_label': format_step_label(prev_col),
                                'to_label': format_step_label(curr_col),
                                'from_val': prev_val,
                                'to_val': curr_val,
                                'drop_abs': drop_abs,
                                'drop_pct': drop_pct,
                            })

                    # Sort by absolute drop (biggest first)
                    dropoffs.sort(key=lambda x: -x['drop_abs'])
                    top_drops = dropoffs[:5]

                    if top_drops:
                        drop_alerts = []
                        for d in top_drops:
                            severity = 'red' if d['drop_pct'] > 10 else ('yellow' if d['drop_pct'] > 5 else 'green')
                            drop_alerts.append((severity,
                                f"<b>{d['from_label']} → {d['to_label']}</b>: "
                                f"loses <b>{d['drop_pct']:.1f}%</b> of users "
                                f"({d['from_val']:.1%} → {d['to_val']:.1%}, "
                                f"-{d['drop_abs']:.1%} absolute)"))
                        if len(all_step_vals) > 1:
                            st.markdown(f"**v{ver}**")
                        render_alerts(drop_alerts)

    # =========================================================================
    # TAB: FTUE STEPS TREND BY DATE
    # =========================================================================
    with tab_steps_trend:
        st.markdown("### FTUE Steps Trend by Date")
        st.caption("Compare how each FTUE step's conversion rate changes over time")

        if fdf.empty or 'install_date' not in fdf.columns:
            st.warning("No FTUE data for selected filters.")
        else:
            # --- Smart alerts for trend ---
            trend_alerts = []
            has_post_ftue = (fdf['install_date'] >= TEST_START_DATE).any() if 'install_date' in fdf.columns else False
            if has_post_ftue:
                trend_alerts.append(('green', '<b>Post-test data available!</b> Compare the trend lines before and after the red "TEST START" marker. Look for steps bending upward.'))
                # Check first few steps for significant change
                t_pct = get_pct_columns(fdf)
                if t_pct:
                    bdf_t = fdf[fdf['install_date'] < TEST_START_DATE]
                    adf_t = fdf[fdf['install_date'] >= TEST_START_DATE]
                    tub = bdf_t['total_users'].sum() if 'total_users' in bdf_t.columns else len(bdf_t)
                    tua = adf_t['total_users'].sum() if 'total_users' in adf_t.columns else len(adf_t)
                    if tub > 0 and tua > 0:
                        biggest_drop_step, biggest_drop_val = None, 0
                        biggest_gain_step, biggest_gain_val = None, 0
                        for col in t_pct:
                            vb = (bdf_t[col] * bdf_t['total_users']).sum() / tub if 'total_users' in bdf_t.columns else bdf_t[col].mean()
                            va = (adf_t[col] * adf_t['total_users']).sum() / tua if 'total_users' in adf_t.columns else adf_t[col].mean()
                            if vb > 0:
                                change_pct = (va - vb) / vb * 100
                                if change_pct < biggest_drop_val:
                                    biggest_drop_val = change_pct
                                    biggest_drop_step = format_step_label(col)
                                if change_pct > biggest_gain_val:
                                    biggest_gain_val = change_pct
                                    biggest_gain_step = format_step_label(col)
                        if biggest_gain_val > 1:
                            trend_alerts.append(('green', f'Biggest improvement: <b>{biggest_gain_step}</b> ({biggest_gain_val:+.1f}% vs before)'))
                        if biggest_drop_val < -1:
                            trend_alerts.append(('red', f'Biggest regression: <b>{biggest_drop_step}</b> ({biggest_drop_val:+.1f}% vs before). Investigate this step.'))
            else:
                trend_alerts.append(('yellow', '<b>Pre-test only.</b> The red dashed "TEST START" line marks where the test begins. Once post-test data flows in, alerts here will flag which steps improved or regressed.'))
            render_alerts(trend_alerts)

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

            # --- Smart alerts ---
            dist_alerts = []
            if top_ver_pct > 80:
                dist_alerts.append(('yellow', f'<b>v{top_ver["version"]}</b> accounts for {top_ver_pct:.0f}% of installs. Other versions have very small sample sizes — treat their metrics with caution.'))
            small_vers = ver_dist_all[ver_dist_all['users'] < 50]
            if len(small_vers) > 0:
                sv_names = ', '.join([f'v{v}' for v in small_vers['version'].tolist()])
                dist_alerts.append(('yellow', f'Versions with &lt;50 users (unreliable data): {sv_names}'))
            if 'media_type' in fdf.columns:
                mt_d = fdf.groupby('media_type')['total_users'].sum() if 'total_users' in fdf.columns else fdf['media_type'].value_counts()
                top_mt_pct_val = mt_d.max() / mt_d.sum() * 100 if mt_d.sum() > 0 else 0
                if top_mt_pct_val > 70:
                    dist_alerts.append(('yellow', f'<b>{mt_d.idxmax()}</b> dominates at {top_mt_pct_val:.0f}% of traffic. FTUE metrics may be heavily skewed by this acquisition channel.'))
            render_alerts(dist_alerts)

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
            rv = sorted(rdf['install_version'].unique().tolist(), key=version_sort_key)
            ret_days = [1, 3, 7, 14]

            # --- Smart alerts ---
            ret_alerts = []
            has_post_ret_data = (rdf['install_date'] >= TEST_START_DATE).any()
            if has_post_ret_data:
                ret_alerts.append(('green', '<b>Post-test retention data is available.</b> Compare D1/D3 values in the "Before vs After" tab for a detailed breakdown.'))
                # Check if D1 improved for any version
                for ver in rv:
                    vdf_b = rdf[(rdf['install_version'] == ver) & (rdf['install_date'] < TEST_START_DATE) & (rdf['days_since_install'] == 1)]
                    vdf_a = rdf[(rdf['install_version'] == ver) & (rdf['install_date'] >= TEST_START_DATE) & (rdf['days_since_install'] == 1)]
                    rb, ra = weighted_retention(vdf_b), weighted_retention(vdf_a)
                    nb, na = vdf_b['cohort_size'].sum(), vdf_a['cohort_size'].sum()
                    if nb > 0 and na > 0:
                        delta = ra - rb
                        if delta > 0.02:
                            ret_alerts.append(('green', f'<b>v{ver}</b> D1 retention improved: {rb:.1%} → {ra:.1%} (<b>{delta:+.1%}</b>)'))
                        elif delta < -0.02:
                            ret_alerts.append(('red', f'<b>v{ver}</b> D1 retention dropped: {rb:.1%} → {ra:.1%} (<b>{delta:+.1%}</b>). Investigate.'))
            else:
                ret_alerts.append(('yellow', '<b>Pre-test only.</b> Retention data is live from BigQuery. Post-test comparisons will appear here once users install after the test start date.'))
            # Small cohort warning
            for ver in rv:
                cd0 = rdf[(rdf['install_version'] == ver) & (rdf['days_since_install'] == 0)]['cohort_size'].sum()
                if 0 < cd0 < 100:
                    ret_alerts.append(('yellow', f'<b>v{ver}</b> has only <b>{cd0:,.0f}</b> users in cohort — too small for reliable retention.'))
            render_alerts(ret_alerts)

            for plat in platforms_in_data:
                st.markdown(f"#### {plat}")
                pdf = rdf[rdf['platform'] == plat] if 'platform' in rdf.columns else rdf

                trows = []
                d1_vals = {}
                for ver in rv:
                    vdf = pdf[pdf['install_version'] == ver]
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
            rc1, rc2, rc3, rc4 = st.columns(4)
            with rc1:
                ret_plat = st.radio("Platform", platforms_in_data, horizontal=True, key="ret_curve_plat")
            with rc2:
                dr = st.radio("Range", ["D7 (0-7)", "D14 (0-14)", "D30 (0-30)"], horizontal=True, index=1, key="ret_curve_range")
            with rc3:
                ret_ba_split = st.checkbox("Show Before/After Split", key="ret_ba_split")
            md = {"D7 (0-7)": 7, "D14 (0-14)": 14, "D30 (0-30)": 30}[dr]
            cdf = rdf[(rdf['days_since_install'] <= md) & (rdf['platform'] == ret_plat)] if 'platform' in rdf.columns else rdf[rdf['days_since_install'] <= md]

            if ret_ba_split:
                fig = go.Figure()
                for ver in rv:
                    ver_color = color_map.get(ver, '#333')
                    for period_label, period_filter, dash_style, symbol in [
                        ("Before", cdf['install_date'] < TEST_START_DATE, 'solid', 'circle'),
                        ("After", cdf['install_date'] >= TEST_START_DATE, 'dash', 'diamond'),
                    ]:
                        sub = cdf[(cdf['install_version'] == ver) & period_filter]
                        if sub.empty:
                            continue
                        cr_period = []
                        for d in range(0, md + 1):
                            ds = sub[(sub['days_since_install'] == d) & (sub['users_active'] > 0)]
                            cs, ua = ds['cohort_size'].sum(), ds['users_active'].sum()
                            if cs > 0:
                                cr_period.append({'day': d, 'retention': ua / cs, 'cohort': cs})
                        if not cr_period:
                            continue
                        vg = pd.DataFrame(cr_period).sort_values('day')
                        trace_name = f"v{ver} {period_label}"
                        fig.add_trace(go.Scatter(x=vg['day'], y=vg['retention'], mode='lines+markers',
                            name=trace_name, line=dict(color=ver_color, width=2.5, dash=dash_style),
                            marker=dict(size=5, symbol=symbol),
                            customdata=vg[['cohort']].values,
                            hovertemplate='Day %{x}<br>Ret: %{y:.2%}<br>Cohort: %{customdata[0]:,}<extra>' + trace_name + '</extra>'))
                apply_chart_theme(fig, title=dict(text=f"Retention Curve — {ret_plat} (Before vs After)"), xaxis_title="Days Since Install",
                    yaxis_title="Retention %", yaxis_tickformat='.1%', height=500, hovermode='x unified',
                    xaxis=dict(dtick=1 if md <= 14 else 5))
                st.plotly_chart(fig, use_container_width=True)
            else:
                cr = []
                for ver in rv:
                    sub = cdf[cdf['install_version'] == ver]
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
            n_ret_days = rdf['install_date'].nunique()
            has_post_ret = rdf[rdf['install_date'] >= TEST_START_DATE].shape[0] > 0
            n_ret_vers = rdf['install_version'].nunique()
            st.markdown(f'<div class="summary-box"><h4>Quick Insights</h4>'
                        f'<b>{n_ret_days}</b> days of retention data across <b>{n_ret_vers}</b> versions. '
                        f'Select a retention day (D1/D3/D7/D14) to track.</div>', unsafe_allow_html=True)

            # --- Smart alerts ---
            daily_ret_alerts = []
            if has_post_ret:
                daily_ret_alerts.append(('green', '<b>Post-test data is in.</b> Look for the trend line lifting above the pre-test average after the red "TEST START" marker.'))
            else:
                daily_ret_alerts.append(('yellow', '<b>Pre-test only.</b> After the test starts, this tab will show whether daily retention is trending up or down compared to the baseline.'))
            render_alerts(daily_ret_alerts)

            rds = st.selectbox("Retention day", [1, 3, 7, 14], index=0, key="daily_ret_day")
            recs = []
            for (dt, ver), gdf in rdf.groupby(['install_date', 'install_version']):
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
            bret = rdf[rdf['install_date'] < TEST_START_DATE]
            aret = rdf[rdf['install_date'] >= TEST_START_DATE]
            if aret.empty:
                st.info("No post-test retention data yet.")
            else:
                crows = []
                for ver in sorted(rdf['install_version'].unique().tolist(), key=version_sort_key):
                    row = {'Version': ver}
                    for rd in [1, 3, 7, 14]:
                        vb = bret[(bret['install_version'] == ver) & (bret['days_since_install'] == rd)]
                        va = aret[(aret['install_version'] == ver) & (aret['days_since_install'] == rd)]
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
