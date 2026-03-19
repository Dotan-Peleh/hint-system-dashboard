# Hint System A/B Test Dashboard

Streamlit dashboard comparing FTUE funnel and retention before/after the Hint System A/B test (started March 17, 2026).

## Live URL

https://hint-system-ab-test-57935720907.us-central1.run.app/ (IAP-protected, peerplay.com / peerplay.io domain access)

## Architecture

- **App**: Streamlit (`app.py`), deployed on GCP Cloud Run
- **Data**: BigQuery table `peerplay.ftue_dashboard_fixed` (48-step FTUE funnel)
- **SQL**: `ftue_query_fix.sql` — builds the funnel table with per-step time-window constraints and monotonic enforcement
- **Auth**: IAP (Identity-Aware Proxy), restricted to peerplay.com and peerplay.io domains

## Data Pipeline

| Time (UTC) | Job | Table |
|---|---|---|
| 04:05 | `dim_player` daily update | `peerplay.dim_player` (upstream) |
| 07:00 | `ftue_dashboard` original | `peerplay.ftue_dashboard` |
| **08:00** | **`ftue_dashboard_fixed` daily rebuild** | `peerplay.ftue_dashboard_fixed` |

BQ Scheduled Query ID: `69c80f9c-0000-266e-861e-30fd3815a4d0`

Source tables: `dim_player`, `vmp_master_event_normalized`, `sources_names_alignment`, `dim_country`, `potential_fraudsters`

## Funnel (48 steps)

- **Steps 1–46**: FTUE flow from privacy screen through chapter 3 harvest collect
- **Step 47**: `scapes_tasks_new_chapter` chapter 4 (optional, checkbox)
- **Step 48**: `scapes_tasks_new_chapter` chapter 5 (optional, checkbox)

### Key fixes in `_fixed` vs original `ftue_dashboard`

1. **30-min time constraints** on generic event steps (click_board_button_scapes, new_chapter_2, click_reward_center) to prevent counting events outside the FTUE session window
2. **Monotonic funnel enforcement** — per-user, a user can only pass step N if they passed all prior steps (0/1 flag multiplication). Eliminates all upticks regardless of anchor mismatches between steps
3. **Steps 47–48** — chapter 4 and 5 progression (hidden by default, toggle via "Include Ch 4 & 5" checkbox)

## Dashboard Tabs

| Tab | Description |
|---|---|
| Before vs After | Independent filters for comparing pre/post test periods. Has its own version, date, platform, country filters. |
| Chart: Version | Funnel comparison across versions |
| FTUE Steps Trend | Step-level trends over time |
| Install Distribution | Install volume by date/hour |
| Daily Step Tracking | Per-step daily conversion tracking |
| Retention by Version | D1–D30 retention by version |
| Daily Retention | Daily retention trends |

## Shareable Links

Filter state is encoded in URL query params. Two mechanisms:

1. **Sidebar filters** → automatically synced to URL (version, platform, date range, country, etc.)
2. **Before/After tab** → "Copy Share Link" button copies the full URL with all BA-specific filters to clipboard

Example: `?tab=ba&before_ver=0.3811&before_sd=2026-03-01&after_sd=2026-03-17&after_sh=17`

## Deploy

```bash
bash deploy.sh
```

Deploys to Cloud Run with IAP authentication. Requires `gcloud` CLI authenticated with the `yotam-395120` project.

## Local Development

```bash
pip install -r requirements.txt
streamlit run app.py
```
