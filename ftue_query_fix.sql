-- FTUE Funnel Analysis - WITH TIME-WINDOW FIX
-- Generic events are now constrained to occur between their surrounding FTUE flow steps
-- This prevents counting events that happen outside the scripted FTUE flow

CREATE OR REPLACE TABLE peerplay.ftue_dashboard
partition by install_date
AS

WITH source_name_mapping AS (
  SELECT
    cost_data_name,
    ARRAY_AGG(final_name ORDER BY final_name LIMIT 1)[OFFSET(0)] AS final_name
  FROM `yotam-395120.peerplay.sources_names_alignment`
  GROUP BY cost_data_name
),

media_type_mapping AS (
  SELECT
    LOWER(final_name) AS mediasource,
    ARRAY_AGG(media_type ORDER BY media_type LIMIT 1)[OFFSET(0)] AS media_type
  FROM `yotam-395120.peerplay.sources_names_alignment`
  GROUP BY LOWER(final_name)
),

low_payers_countries AS (
  SELECT country_code
  FROM `yotam-395120.peerplay.dim_country`
  WHERE is_low_payers_country = true
),

installs AS (
  SELECT
    p.distinct_id,
    p.install_date,
    DATE_TRUNC(p.install_date, WEEK(MONDAY)) AS install_week,
    DATE_TRUNC(p.install_date, MONTH) AS install_month,
    p.first_app_version AS install_version,
    p.first_platform AS platform,
    p.first_country AS country,
    CASE WHEN lpc.country_code IS NOT NULL THEN true ELSE false END AS is_low_payers_country,
    LOWER(COALESCE(snm.final_name, COALESCE(p.first_mediasource, 'organic'))) AS mediasource,
    CASE
      WHEN SAFE_CAST(p.first_app_version AS FLOAT64) >= 0.3731
       AND SAFE_CAST(p.first_app_version AS FLOAT64) <= 0.374
      THEN true
      ELSE false
    END AS is_buggy_ftue_flow3_version
  FROM `yotam-395120.peerplay.dim_player` p
  LEFT JOIN source_name_mapping snm
    ON p.first_mediasource = snm.cost_data_name
  LEFT JOIN low_payers_countries lpc
    ON p.first_country = lpc.country_code
  WHERE p.install_date >= CURRENT_DATE() - 90
    AND p.install_date <= CURRENT_DATE() - 1
    AND p.first_country NOT IN ('IL', 'UA', 'AM')
    AND p.distinct_id NOT IN (SELECT distinct_id FROM `yotam-395120.peerplay.potential_fraudsters`)
),

first_merge AS (
  SELECT
    e.distinct_id,
    MIN(e.res_timestamp) AS first_merge_timestamp
  FROM `yotam-395120.peerplay.vmp_master_event_normalized` e
  INNER JOIN installs i ON e.distinct_id = i.distinct_id
  WHERE e.mp_event_name = 'merge'
    AND e.date >= CURRENT_DATE() - 90
    AND e.date <= CURRENT_DATE() - 1
  GROUP BY e.distinct_id
),

user_events AS (
  SELECT
    e.distinct_id,
    e.mp_event_name,
    CAST(e.chapter AS INT64) AS chapter,
    e.res_timestamp,
    e.counter_per_session_game_side,
    CAST(e.dialog_id AS STRING) AS dialog_id,
    i.install_date,
    i.install_week,
    i.install_month,
    i.install_version,
    i.platform,
    i.country,
    i.is_low_payers_country,
    i.mediasource,
    i.is_buggy_ftue_flow3_version,
    fm.first_merge_timestamp
  FROM `yotam-395120.peerplay.vmp_master_event_normalized` e
  INNER JOIN installs i ON e.distinct_id = i.distinct_id
  LEFT JOIN first_merge fm ON e.distinct_id = fm.distinct_id
  WHERE e.date >= CURRENT_DATE() - 90
    AND e.date <= CURRENT_DATE() - 1
),

-- Compute FTUE anchor timestamps per user (the scripted flow steps)
ftue_anchors AS (
  SELECT
    distinct_id,
    -- Flow 1 anchors
    MIN(CASE WHEN mp_event_name = 'impression_ftue_flow1_step0' THEN res_timestamp END) AS ts_flow1_step0,
    MIN(CASE WHEN mp_event_name = 'impression_ftue_flow1_step1' THEN res_timestamp END) AS ts_flow1_step1,
    MIN(CASE WHEN mp_event_name = 'impression_ftue_flow1_step2' THEN res_timestamp END) AS ts_flow1_step2,
    MIN(CASE WHEN mp_event_name = 'impression_ftue_flow1_step3' THEN res_timestamp END) AS ts_flow1_step3,
    MIN(CASE WHEN mp_event_name = 'impression_ftue_flow1_step4' THEN res_timestamp END) AS ts_flow1_step4,
    MIN(CASE WHEN mp_event_name = 'impression_ftue_flow1_step5' THEN res_timestamp END) AS ts_flow1_step5,
    MIN(CASE WHEN mp_event_name = 'impression_ftue_flow1_step6' THEN res_timestamp END) AS ts_flow1_step6,
    MIN(CASE WHEN mp_event_name = 'impression_ftue_flow1_step7' THEN res_timestamp END) AS ts_flow1_step7,
    -- Flow 2 anchors
    MIN(CASE WHEN mp_event_name = 'impression_ftue_flow2_step0' THEN res_timestamp END) AS ts_flow2_step0,
    MIN(CASE WHEN mp_event_name = 'impression_ftue_flow2_step1' THEN res_timestamp END) AS ts_flow2_step1,
    MIN(CASE WHEN mp_event_name = 'impression_ftue_flow2_step2' THEN res_timestamp END) AS ts_flow2_step2,
    MIN(CASE WHEN mp_event_name = 'impression_ftue_flow2_step5' THEN res_timestamp END) AS ts_flow2_step5,
    -- Flow 3 anchors
    MIN(CASE WHEN mp_event_name = 'impression_ftue_flow3_step0' THEN res_timestamp END) AS ts_flow3_step0,
    MIN(CASE WHEN mp_event_name = 'impression_ftue_flow3_step6' AND chapter = 2 THEN res_timestamp END) AS ts_flow3_step6_ch2,
    MIN(CASE WHEN mp_event_name = 'impression_ftue_flow3_step8' AND chapter = 2 THEN res_timestamp END) AS ts_flow3_step8_ch2,
    -- Flow 12 anchors
    MIN(CASE WHEN mp_event_name = 'impression_ftue_flow12_step0' THEN res_timestamp END) AS ts_flow12_step0,
    -- Privacy (start of FTUE)
    MIN(CASE WHEN mp_event_name = 'impression_privacy' THEN res_timestamp END) AS ts_privacy,
    -- How to play (between flow1 and flow2)
    MIN(CASE WHEN mp_event_name = 'impression_how_to_play' THEN res_timestamp END) AS ts_how_to_play,
    -- click_board_button_scapes (for step 09 constraint)
    MIN(CASE WHEN mp_event_name = 'click_board_button_scapes' THEN res_timestamp END) AS ts_click_board_scapes
  FROM user_events
  GROUP BY distinct_id
),

-- Flag each user for reaching each funnel step (with time-window constraints)
funnel_flags AS (
  SELECT
    ue.distinct_id,
    ue.install_date,
    ue.install_week,
    ue.install_month,
    ue.install_version,
    ue.platform,
    ue.country,
    ue.is_low_payers_country,
    ue.mediasource,

    -- Step 01: impression_privacy (anchor - no constraint needed)
    MAX(CASE WHEN ue.mp_event_name = 'impression_privacy' THEN 1 ELSE 0 END) AS step_01,

    -- Step 02: impression_scapes ch1 (early FTUE, constrain to before flow1_step2)
    MAX(CASE WHEN ue.mp_event_name = 'impression_scapes' AND ue.chapter = 1
          AND (a.ts_flow1_step2 IS NULL OR ue.res_timestamp < a.ts_flow1_step2)
          THEN 1 ELSE 0 END) AS step_02,

    -- Step 03: board_tasks_new_task (constrain: after privacy, before flow1_step2)
    MAX(CASE WHEN ue.mp_event_name = 'board_tasks_new_task'
          AND a.ts_privacy IS NOT NULL AND ue.res_timestamp >= a.ts_privacy
          AND (a.ts_flow1_step2 IS NULL OR ue.res_timestamp < a.ts_flow1_step2)
          THEN 1 ELSE 0 END) AS step_03,

    -- Step 04: impression_dialog 1001199 (anchor-like, keep as is)
    MAX(CASE WHEN ue.mp_event_name = 'impression_dialog' AND ue.dialog_id = '1001199' THEN 1 ELSE 0 END) AS step_04,

    -- Step 05: click_dialog_exit 1001199 (anchor-like, keep as is)
    MAX(CASE WHEN ue.mp_event_name = 'click_dialog_exit' AND ue.dialog_id = '1001199' THEN 1 ELSE 0 END) AS step_05,

    -- Step 06: ftue_flow1_step0 (anchor)
    MAX(CASE WHEN ue.mp_event_name = 'impression_ftue_flow1_step0' THEN 1 ELSE 0 END) AS step_06,

    -- Step 07: ftue_flow1_step1 (anchor)
    MAX(CASE WHEN ue.mp_event_name = 'impression_ftue_flow1_step1' THEN 1 ELSE 0 END) AS step_07,

    -- Step 08: click_board_button_scapes (constrain: between flow1_step1 and flow1_step2)
    MAX(CASE WHEN ue.mp_event_name = 'click_board_button_scapes'
          AND a.ts_flow1_step1 IS NOT NULL AND ue.res_timestamp >= a.ts_flow1_step1
          AND (a.ts_flow1_step2 IS NULL OR ue.res_timestamp <= a.ts_flow1_step2)
          THEN 1 ELSE 0 END) AS step_08,

    -- Step 09: impression_board (constrain: between flow1_step1 and flow1_step2, AND after click_board_scapes)
    MAX(CASE WHEN ue.mp_event_name = 'impression_board'
          AND a.ts_flow1_step1 IS NOT NULL AND ue.res_timestamp >= a.ts_flow1_step1
          AND a.ts_click_board_scapes IS NOT NULL AND ue.res_timestamp >= a.ts_click_board_scapes
          AND (a.ts_flow1_step2 IS NULL OR ue.res_timestamp <= a.ts_flow1_step2)
          THEN 1 ELSE 0 END) AS step_09,

    -- Step 10: ftue_flow1_step2 (anchor)
    MAX(CASE WHEN ue.mp_event_name = 'impression_ftue_flow1_step2' THEN 1 ELSE 0 END) AS step_10,

    -- Step 11: generation BEFORE first merge (constrain: between flow1_step2 and flow1_step3)
    MAX(CASE WHEN ue.mp_event_name = 'generation'
          AND (ue.first_merge_timestamp IS NULL OR ue.res_timestamp < ue.first_merge_timestamp)
          AND a.ts_flow1_step2 IS NOT NULL AND ue.res_timestamp >= a.ts_flow1_step2
          AND (a.ts_flow1_step3 IS NULL OR ue.res_timestamp <= a.ts_flow1_step3)
          THEN 1 ELSE 0 END) AS step_11,

    -- Step 12: ftue_flow1_step3 (anchor)
    MAX(CASE WHEN ue.mp_event_name = 'impression_ftue_flow1_step3' THEN 1 ELSE 0 END) AS step_12,

    -- Step 13: merge (constrain: between flow1_step3 and flow1_step4)
    MAX(CASE WHEN ue.mp_event_name = 'merge'
          AND a.ts_flow1_step3 IS NOT NULL AND ue.res_timestamp >= a.ts_flow1_step3
          AND (a.ts_flow1_step4 IS NULL OR ue.res_timestamp <= a.ts_flow1_step4)
          THEN 1 ELSE 0 END) AS step_13,

    -- Step 14: ftue_flow1_step4 (anchor)
    MAX(CASE WHEN ue.mp_event_name = 'impression_ftue_flow1_step4' THEN 1 ELSE 0 END) AS step_14,

    -- Step 15: board_tasks_task_ready (constrain: between flow1_step4 and flow1_step5)
    MAX(CASE WHEN ue.mp_event_name = 'board_tasks_task_ready'
          AND a.ts_flow1_step4 IS NOT NULL AND ue.res_timestamp >= a.ts_flow1_step4
          AND (a.ts_flow1_step5 IS NULL OR ue.res_timestamp <= a.ts_flow1_step5)
          THEN 1 ELSE 0 END) AS step_15,

    -- Step 16: ftue_flow1_step5 (anchor)
    MAX(CASE WHEN ue.mp_event_name = 'impression_ftue_flow1_step5' THEN 1 ELSE 0 END) AS step_16,

    -- Step 17: click_board_tasks_go (constrain: between flow1_step5 and flow1_step6)
    MAX(CASE WHEN ue.mp_event_name = 'click_board_tasks_go'
          AND a.ts_flow1_step5 IS NOT NULL AND ue.res_timestamp >= a.ts_flow1_step5
          AND (a.ts_flow1_step6 IS NULL OR ue.res_timestamp <= a.ts_flow1_step6)
          THEN 1 ELSE 0 END) AS step_17,

    -- Step 18: rewards_board_task (constrain: between flow1_step5 and flow1_step6)
    MAX(CASE WHEN ue.mp_event_name = 'rewards_board_task'
          AND a.ts_flow1_step5 IS NOT NULL AND ue.res_timestamp >= a.ts_flow1_step5
          AND (a.ts_flow1_step6 IS NULL OR ue.res_timestamp <= a.ts_flow1_step6)
          THEN 1 ELSE 0 END) AS step_18,

    -- Step 19: ftue_flow1_step6 (anchor)
    MAX(CASE WHEN ue.mp_event_name = 'impression_ftue_flow1_step6' THEN 1 ELSE 0 END) AS step_19,

    -- Step 20: generation AFTER first merge (constrain: between flow1_step6 and flow1_step7)
    MAX(CASE WHEN ue.mp_event_name = 'generation'
          AND ue.first_merge_timestamp IS NOT NULL
          AND ue.res_timestamp >= ue.first_merge_timestamp
          AND a.ts_flow1_step6 IS NOT NULL AND ue.res_timestamp >= a.ts_flow1_step6
          AND (a.ts_flow1_step7 IS NULL OR ue.res_timestamp <= a.ts_flow1_step7)
          THEN 1 ELSE 0 END) AS step_20,

    -- Step 21: ftue_flow1_step7 (anchor)
    MAX(CASE WHEN ue.mp_event_name = 'impression_ftue_flow1_step7' THEN 1 ELSE 0 END) AS step_21,

    -- Step 22: impression_how_to_play (constrain: between flow1_step7 and flow2_step0)
    MAX(CASE WHEN ue.mp_event_name = 'impression_how_to_play'
          AND a.ts_flow1_step7 IS NOT NULL AND ue.res_timestamp >= a.ts_flow1_step7
          AND (a.ts_flow2_step0 IS NULL OR ue.res_timestamp <= a.ts_flow2_step0)
          THEN 1 ELSE 0 END) AS step_22,

    -- Step 23: click_scapes_button_board (constrain: AFTER how_to_play AND before flow2_step0)
    MAX(CASE WHEN ue.mp_event_name = 'click_scapes_button_board'
          AND a.ts_how_to_play IS NOT NULL AND ue.res_timestamp >= a.ts_how_to_play
          AND (a.ts_flow2_step0 IS NULL OR ue.res_timestamp <= a.ts_flow2_step0)
          THEN 1 ELSE 0 END) AS step_23,

    -- Step 24: ftue_flow2_step0 (constrain: must have seen how_to_play first)
    MAX(CASE WHEN ue.mp_event_name = 'impression_ftue_flow2_step0'
          AND a.ts_how_to_play IS NOT NULL AND ue.res_timestamp >= a.ts_how_to_play
          THEN 1 ELSE 0 END) AS step_24,

    -- Step 25: impression_dialog 10013 (constrain: after how_to_play AND between flow2_step0 and flow2_step1)
    MAX(CASE WHEN ue.mp_event_name = 'impression_dialog' AND ue.dialog_id = '10013'
          AND a.ts_how_to_play IS NOT NULL
          AND a.ts_flow2_step0 IS NOT NULL AND ue.res_timestamp >= a.ts_flow2_step0
          AND (a.ts_flow2_step1 IS NULL OR ue.res_timestamp <= a.ts_flow2_step1)
          THEN 1 ELSE 0 END) AS step_25,

    -- Step 26: click_scapes_tasks_go_button (constrain: requires how_to_play, between flow2_step0 and flow2_step1)
    MAX(CASE WHEN ue.mp_event_name = 'click_scapes_tasks_go_button'
          AND a.ts_how_to_play IS NOT NULL
          AND a.ts_flow2_step0 IS NOT NULL AND ue.res_timestamp >= a.ts_flow2_step0
          AND (a.ts_flow2_step1 IS NULL OR ue.res_timestamp <= a.ts_flow2_step1)
          THEN 1 ELSE 0 END) AS step_26,

    -- Step 27: scapes_tasks_cash_deducted (constrain: requires how_to_play, between flow2_step0 and flow2_step1)
    MAX(CASE WHEN ue.mp_event_name = 'scapes_tasks_cash_deducted'
          AND a.ts_how_to_play IS NOT NULL
          AND a.ts_flow2_step0 IS NOT NULL AND ue.res_timestamp >= a.ts_flow2_step0
          AND (a.ts_flow2_step1 IS NULL OR ue.res_timestamp <= a.ts_flow2_step1)
          THEN 1 ELSE 0 END) AS step_27,

    -- Step 28: rewards_scape_task (constrain: requires how_to_play, between flow2_step0 and flow2_step1)
    MAX(CASE WHEN ue.mp_event_name = 'rewards_scape_task'
          AND a.ts_how_to_play IS NOT NULL
          AND a.ts_flow2_step0 IS NOT NULL AND ue.res_timestamp >= a.ts_flow2_step0
          AND (a.ts_flow2_step1 IS NULL OR ue.res_timestamp <= a.ts_flow2_step1)
          THEN 1 ELSE 0 END) AS step_28,

    -- Step 29: click_dialog_exit 10013 (constrain: requires how_to_play, between flow2_step0 and flow2_step1)
    MAX(CASE WHEN ue.mp_event_name = 'click_dialog_exit' AND ue.dialog_id = '10013'
          AND a.ts_how_to_play IS NOT NULL
          AND a.ts_flow2_step0 IS NOT NULL AND ue.res_timestamp >= a.ts_flow2_step0
          AND (a.ts_flow2_step1 IS NULL OR ue.res_timestamp <= a.ts_flow2_step1)
          THEN 1 ELSE 0 END) AS step_29,

    -- Step 30: ftue_flow2_step1 (anchor, requires how_to_play)
    MAX(CASE WHEN ue.mp_event_name = 'impression_ftue_flow2_step1'
          AND a.ts_how_to_play IS NOT NULL THEN 1 ELSE 0 END) AS step_30,

    -- Step 31: impression_dialog 10015 (constrain: requires how_to_play, between flow2_step1 and flow2_step2)
    MAX(CASE WHEN ue.mp_event_name = 'impression_dialog' AND ue.dialog_id = '10015'
          AND a.ts_how_to_play IS NOT NULL
          AND a.ts_flow2_step1 IS NOT NULL AND ue.res_timestamp >= a.ts_flow2_step1
          AND (a.ts_flow2_step2 IS NULL OR ue.res_timestamp <= a.ts_flow2_step2)
          THEN 1 ELSE 0 END) AS step_31,

    -- Step 32: ftue_flow2_step2 (anchor, requires how_to_play)
    MAX(CASE WHEN ue.mp_event_name = 'impression_ftue_flow2_step2'
          AND a.ts_how_to_play IS NOT NULL THEN 1 ELSE 0 END) AS step_32,

    -- Step 33: ship_animation (constrain: requires how_to_play, between flow2_step2 and flow2_step5)
    MAX(CASE WHEN ue.mp_event_name = 'impression_ship_animation_started'
          AND a.ts_how_to_play IS NOT NULL
          AND a.ts_flow2_step2 IS NOT NULL AND ue.res_timestamp >= a.ts_flow2_step2
          AND (a.ts_flow2_step5 IS NULL OR ue.res_timestamp <= a.ts_flow2_step5)
          THEN 1 ELSE 0 END) AS step_33,

    -- Step 34: ftue_flow2_step5 (anchor, requires how_to_play)
    MAX(CASE WHEN ue.mp_event_name = 'impression_ftue_flow2_step5'
          AND a.ts_how_to_play IS NOT NULL THEN 1 ELSE 0 END) AS step_34,

    -- Step 35: ftue_flow3_step0 (anchor, requires how_to_play, buggy version handling)
    MAX(CASE WHEN a.ts_how_to_play IS NOT NULL AND (
      (ue.is_buggy_ftue_flow3_version AND ue.mp_event_name = 'impression_ftue_flow3_step1') OR
      (NOT ue.is_buggy_ftue_flow3_version AND ue.mp_event_name = 'impression_ftue_flow3_step0')
    ) THEN 1 ELSE 0 END) AS step_35,

    -- Step 36: scapes_tasks_new_chapter ch2 (requires how_to_play, between flow3_step0 and flow3_step6_ch2)
    MAX(CASE WHEN ue.mp_event_name = 'scapes_tasks_new_chapter' AND ue.chapter = 2
          AND a.ts_how_to_play IS NOT NULL
          AND a.ts_flow3_step0 IS NOT NULL AND ue.res_timestamp >= a.ts_flow3_step0
          AND (a.ts_flow3_step6_ch2 IS NULL OR ue.res_timestamp <= a.ts_flow3_step6_ch2)
          THEN 1 ELSE 0 END) AS step_36,

    -- Step 37: ftue_flow3_step1 ch2 (anchor, requires how_to_play, buggy version handling)
    MAX(CASE WHEN a.ts_how_to_play IS NOT NULL AND (
      (ue.is_buggy_ftue_flow3_version AND ue.mp_event_name = 'impression_ftue_flow3_step2' AND ue.chapter = 2) OR
      (NOT ue.is_buggy_ftue_flow3_version AND ue.mp_event_name = 'impression_ftue_flow3_step1' AND ue.chapter = 2)
    ) THEN 1 ELSE 0 END) AS step_37,

    -- Step 38: ftue_flow3_step2 ch2 (anchor, requires how_to_play, buggy version handling)
    MAX(CASE WHEN a.ts_how_to_play IS NOT NULL AND (
      (ue.is_buggy_ftue_flow3_version AND ue.mp_event_name = 'impression_ftue_flow3_step3' AND ue.chapter = 2) OR
      (NOT ue.is_buggy_ftue_flow3_version AND ue.mp_event_name = 'impression_ftue_flow3_step2' AND ue.chapter = 2)
    ) THEN 1 ELSE 0 END) AS step_38,

    -- Step 39: click_harvest_collect ch2 (requires how_to_play, between flow3_step0 and flow3_step6_ch2)
    MAX(CASE WHEN ue.mp_event_name = 'click_harvest_collect' AND ue.chapter = 2
          AND a.ts_how_to_play IS NOT NULL
          AND a.ts_flow3_step0 IS NOT NULL AND ue.res_timestamp >= a.ts_flow3_step0
          AND (a.ts_flow3_step6_ch2 IS NULL OR ue.res_timestamp <= a.ts_flow3_step6_ch2)
          THEN 1 ELSE 0 END) AS step_39,

    -- Step 40: ftue_flow3_step6 ch2 (anchor, requires how_to_play, buggy version handling)
    MAX(CASE WHEN a.ts_how_to_play IS NOT NULL AND (
      (ue.is_buggy_ftue_flow3_version AND ue.mp_event_name = 'impression_ftue_flow3_step7' AND ue.chapter = 2) OR
      (NOT ue.is_buggy_ftue_flow3_version AND ue.mp_event_name = 'impression_ftue_flow3_step6' AND ue.chapter = 2)
    ) THEN 1 ELSE 0 END) AS step_40,

    -- Step 41: click_reward_center (requires how_to_play, between flow3_step6_ch2 and flow3_step8_ch2)
    MAX(CASE WHEN ue.mp_event_name = 'click_reward_center'
          AND a.ts_how_to_play IS NOT NULL
          AND a.ts_flow3_step6_ch2 IS NOT NULL AND ue.res_timestamp >= a.ts_flow3_step6_ch2
          AND (a.ts_flow3_step8_ch2 IS NULL OR ue.res_timestamp <= a.ts_flow3_step8_ch2)
          THEN 1 ELSE 0 END) AS step_41,

    -- Step 42: ftue_flow3_step8 ch2 (anchor, requires how_to_play, buggy version handling)
    MAX(CASE WHEN a.ts_how_to_play IS NOT NULL AND (
      (ue.is_buggy_ftue_flow3_version AND ue.mp_event_name = 'impression_ftue_flow3_step9' AND ue.chapter = 2) OR
      (NOT ue.is_buggy_ftue_flow3_version AND ue.mp_event_name = 'impression_ftue_flow3_step8' AND ue.chapter = 2)
    ) THEN 1 ELSE 0 END) AS step_42,

    -- Step 43: ftue_flow12_step0 (constrain: only if user completed flow3_step8_ch2)
    MAX(CASE WHEN ue.mp_event_name = 'impression_ftue_flow12_step0'
          AND a.ts_flow3_step8_ch2 IS NOT NULL AND ue.res_timestamp >= a.ts_flow3_step8_ch2
          THEN 1 ELSE 0 END) AS step_43,

    -- Step 44: ftue_flow12_step4 (constrain: only if user completed flow3_step8_ch2)
    MAX(CASE WHEN ue.mp_event_name = 'impression_ftue_flow12_step4'
          AND a.ts_flow3_step8_ch2 IS NOT NULL AND ue.res_timestamp >= a.ts_flow3_step8_ch2
          THEN 1 ELSE 0 END) AS step_44,

    -- Step 45: scapes_tasks_new_chapter ch3 (constrain: after flow3_step8_ch2)
    MAX(CASE WHEN ue.mp_event_name = 'scapes_tasks_new_chapter' AND ue.chapter = 3
          AND a.ts_flow3_step8_ch2 IS NOT NULL AND ue.res_timestamp >= a.ts_flow3_step8_ch2
          THEN 1 ELSE 0 END) AS step_45,

    -- Step 46: click_harvest_collect ch3 (constrain: after flow3_step8_ch2)
    MAX(CASE WHEN ue.mp_event_name = 'click_harvest_collect' AND ue.chapter = 3
          AND a.ts_flow3_step8_ch2 IS NOT NULL AND ue.res_timestamp >= a.ts_flow3_step8_ch2
          THEN 1 ELSE 0 END) AS step_46

  FROM user_events ue
  LEFT JOIN ftue_anchors a ON ue.distinct_id = a.distinct_id
  GROUP BY ue.distinct_id, ue.install_date, ue.install_week, ue.install_month, ue.install_version, ue.platform, ue.country, ue.is_low_payers_country, ue.mediasource
),

-- Aggregate funnel metrics by all dimensions
version_aggregates AS (
  SELECT
    install_date,
    install_week,
    install_month,
    install_version,
    platform,
    country,
    is_low_payers_country,
    mediasource,
    COUNT(DISTINCT distinct_id) AS total_users,
    SUM(step_01) AS step_01, SUM(step_02) AS step_02, SUM(step_03) AS step_03,
    SUM(step_04) AS step_04, SUM(step_05) AS step_05, SUM(step_06) AS step_06,
    SUM(step_07) AS step_07, SUM(step_08) AS step_08, SUM(step_09) AS step_09,
    SUM(step_10) AS step_10, SUM(step_11) AS step_11, SUM(step_12) AS step_12,
    SUM(step_13) AS step_13, SUM(step_14) AS step_14, SUM(step_15) AS step_15,
    SUM(step_16) AS step_16, SUM(step_17) AS step_17, SUM(step_18) AS step_18,
    SUM(step_19) AS step_19, SUM(step_20) AS step_20, SUM(step_21) AS step_21,
    SUM(step_22) AS step_22, SUM(step_23) AS step_23, SUM(step_24) AS step_24,
    SUM(step_25) AS step_25, SUM(step_26) AS step_26, SUM(step_27) AS step_27,
    SUM(step_28) AS step_28, SUM(step_29) AS step_29, SUM(step_30) AS step_30,
    SUM(step_31) AS step_31, SUM(step_32) AS step_32, SUM(step_33) AS step_33,
    SUM(step_34) AS step_34, SUM(step_35) AS step_35, SUM(step_36) AS step_36,
    SUM(step_37) AS step_37, SUM(step_38) AS step_38, SUM(step_39) AS step_39,
    SUM(step_40) AS step_40, SUM(step_41) AS step_41, SUM(step_42) AS step_42,
    SUM(step_43) AS step_43, SUM(step_44) AS step_44, SUM(step_45) AS step_45,
    SUM(step_46) AS step_46
  FROM funnel_flags
  GROUP BY install_date, install_week, install_month, install_version, platform, country, is_low_payers_country, mediasource
)

SELECT
  v.install_date, v.install_week, v.install_month, v.install_version,
  v.platform, v.country, v.is_low_payers_country, v.mediasource,
  COALESCE(m.media_type, CASE WHEN v.mediasource = 'organic' THEN 'organic' ELSE 'none' END) AS media_type,
  v.total_users,

  -- Conversion rates (% of step 1)
  ROUND(step_01 / NULLIF(step_01, 0), 4) AS pct_01_impression_privacy,
  ROUND(step_02 / NULLIF(step_01, 0), 4) AS pct_02_impression_scapes_ch1,
  ROUND(step_03 / NULLIF(step_01, 0), 4) AS pct_03_board_tasks_new_task,
  ROUND(step_04 / NULLIF(step_01, 0), 4) AS pct_04_impression_dialog_1001199,
  ROUND(step_05 / NULLIF(step_01, 0), 4) AS pct_05_click_dialog_exit_1001199,
  ROUND(step_06 / NULLIF(step_01, 0), 4) AS pct_06_ftue_flow1_step0,
  ROUND(step_07 / NULLIF(step_01, 0), 4) AS pct_07_ftue_flow1_step1,
  ROUND(step_08 / NULLIF(step_01, 0), 4) AS pct_08_click_board_button_scapes,
  ROUND(step_09 / NULLIF(step_01, 0), 4) AS pct_09_impression_board,
  ROUND(step_10 / NULLIF(step_01, 0), 4) AS pct_10_ftue_flow1_step2,
  ROUND(step_11 / NULLIF(step_01, 0), 4) AS pct_11_generation_before_merge,
  ROUND(step_12 / NULLIF(step_01, 0), 4) AS pct_12_ftue_flow1_step3,
  ROUND(step_13 / NULLIF(step_01, 0), 4) AS pct_13_first_merge,
  ROUND(step_14 / NULLIF(step_01, 0), 4) AS pct_14_ftue_flow1_step4,
  ROUND(step_15 / NULLIF(step_01, 0), 4) AS pct_15_board_tasks_task_ready,
  ROUND(step_16 / NULLIF(step_01, 0), 4) AS pct_16_ftue_flow1_step5,
  ROUND(step_17 / NULLIF(step_01, 0), 4) AS pct_17_click_board_tasks_go,
  ROUND(step_18 / NULLIF(step_01, 0), 4) AS pct_18_rewards_board_task,
  ROUND(step_19 / NULLIF(step_01, 0), 4) AS pct_19_ftue_flow1_step6,
  ROUND(step_20 / NULLIF(step_01, 0), 4) AS pct_20_generation_after_merge,
  ROUND(step_21 / NULLIF(step_01, 0), 4) AS pct_21_ftue_flow1_step7,
  ROUND(step_22 / NULLIF(step_01, 0), 4) AS pct_22_impression_how_to_play,
  ROUND(step_23 / NULLIF(step_01, 0), 4) AS pct_23_click_scapes_button_board,
  ROUND(step_24 / NULLIF(step_01, 0), 4) AS pct_24_ftue_flow2_step0,
  ROUND(step_25 / NULLIF(step_01, 0), 4) AS pct_25_impression_dialog_10013,
  ROUND(step_26 / NULLIF(step_01, 0), 4) AS pct_26_click_scapes_tasks_go_button,
  ROUND(step_27 / NULLIF(step_01, 0), 4) AS pct_27_scapes_tasks_cash_deducted,
  ROUND(step_28 / NULLIF(step_01, 0), 4) AS pct_28_rewards_scape_task,
  ROUND(step_29 / NULLIF(step_01, 0), 4) AS pct_29_click_dialog_exit_10013,
  ROUND(step_30 / NULLIF(step_01, 0), 4) AS pct_30_ftue_flow2_step1,
  ROUND(step_31 / NULLIF(step_01, 0), 4) AS pct_31_impression_dialog_10015,
  ROUND(step_32 / NULLIF(step_01, 0), 4) AS pct_32_ftue_flow2_step2,
  ROUND(step_33 / NULLIF(step_01, 0), 4) AS pct_33_ship_animation,
  ROUND(step_34 / NULLIF(step_01, 0), 4) AS pct_34_ftue_flow2_step5,
  ROUND(step_35 / NULLIF(step_01, 0), 4) AS pct_35_ftue_flow3_step0,
  ROUND(step_36 / NULLIF(step_01, 0), 4) AS pct_36_new_chapter_2,
  ROUND(step_37 / NULLIF(step_01, 0), 4) AS pct_37_ftue_flow3_step1_ch2,
  ROUND(step_38 / NULLIF(step_01, 0), 4) AS pct_38_ftue_flow3_step2_ch2,
  ROUND(step_39 / NULLIF(step_01, 0), 4) AS pct_39_click_harvest_collect_ch2,
  ROUND(step_40 / NULLIF(step_01, 0), 4) AS pct_40_ftue_flow3_step6_ch2,
  ROUND(step_41 / NULLIF(step_01, 0), 4) AS pct_41_click_reward_center,
  ROUND(step_42 / NULLIF(step_01, 0), 4) AS pct_42_ftue_flow3_step8_ch2,
  ROUND(step_43 / NULLIF(step_01, 0), 4) AS pct_43_ftue_flow12_step0,
  ROUND(step_44 / NULLIF(step_01, 0), 4) AS pct_44_ftue_flow12_step4,
  ROUND(step_45 / NULLIF(step_01, 0), 4) AS pct_45_new_chapter_3,
  ROUND(step_46 / NULLIF(step_01, 0), 4) AS pct_46_click_harvest_collect_ch3,

  -- Step-to-step conversion rates
  1.0 AS ratio_01_to_prev,
  ROUND(step_02 / NULLIF(step_01, 0), 4) AS ratio_02_to_01,
  ROUND(step_03 / NULLIF(step_02, 0), 4) AS ratio_03_to_02,
  ROUND(step_04 / NULLIF(step_03, 0), 4) AS ratio_04_to_03,
  ROUND(step_05 / NULLIF(step_04, 0), 4) AS ratio_05_to_04,
  ROUND(step_06 / NULLIF(step_05, 0), 4) AS ratio_06_to_05,
  ROUND(step_07 / NULLIF(step_06, 0), 4) AS ratio_07_to_06,
  ROUND(step_08 / NULLIF(step_07, 0), 4) AS ratio_08_to_07,
  ROUND(step_09 / NULLIF(step_08, 0), 4) AS ratio_09_to_08,
  ROUND(step_10 / NULLIF(step_09, 0), 4) AS ratio_10_to_09,
  ROUND(step_11 / NULLIF(step_10, 0), 4) AS ratio_11_to_10,
  ROUND(step_12 / NULLIF(step_11, 0), 4) AS ratio_12_to_11,
  ROUND(step_13 / NULLIF(step_12, 0), 4) AS ratio_13_to_12,
  ROUND(step_14 / NULLIF(step_13, 0), 4) AS ratio_14_to_13,
  ROUND(step_15 / NULLIF(step_14, 0), 4) AS ratio_15_to_14,
  ROUND(step_16 / NULLIF(step_15, 0), 4) AS ratio_16_to_15,
  ROUND(step_17 / NULLIF(step_16, 0), 4) AS ratio_17_to_16,
  ROUND(step_18 / NULLIF(step_17, 0), 4) AS ratio_18_to_17,
  ROUND(step_19 / NULLIF(step_18, 0), 4) AS ratio_19_to_18,
  ROUND(step_20 / NULLIF(step_19, 0), 4) AS ratio_20_to_19,
  ROUND(step_21 / NULLIF(step_20, 0), 4) AS ratio_21_to_20,
  ROUND(step_22 / NULLIF(step_21, 0), 4) AS ratio_22_to_21,
  ROUND(step_23 / NULLIF(step_22, 0), 4) AS ratio_23_to_22,
  ROUND(step_24 / NULLIF(step_23, 0), 4) AS ratio_24_to_23,
  ROUND(step_25 / NULLIF(step_24, 0), 4) AS ratio_25_to_24,
  ROUND(step_26 / NULLIF(step_25, 0), 4) AS ratio_26_to_25,
  ROUND(step_27 / NULLIF(step_26, 0), 4) AS ratio_27_to_26,
  ROUND(step_28 / NULLIF(step_27, 0), 4) AS ratio_28_to_27,
  ROUND(step_29 / NULLIF(step_28, 0), 4) AS ratio_29_to_28,
  ROUND(step_30 / NULLIF(step_29, 0), 4) AS ratio_30_to_29,
  ROUND(step_31 / NULLIF(step_30, 0), 4) AS ratio_31_to_30,
  ROUND(step_32 / NULLIF(step_31, 0), 4) AS ratio_32_to_31,
  ROUND(step_33 / NULLIF(step_32, 0), 4) AS ratio_33_to_32,
  ROUND(step_34 / NULLIF(step_33, 0), 4) AS ratio_34_to_33,
  ROUND(step_35 / NULLIF(step_34, 0), 4) AS ratio_35_to_34,
  ROUND(step_36 / NULLIF(step_35, 0), 4) AS ratio_36_to_35,
  ROUND(step_37 / NULLIF(step_36, 0), 4) AS ratio_37_to_36,
  ROUND(step_38 / NULLIF(step_37, 0), 4) AS ratio_38_to_37,
  ROUND(step_39 / NULLIF(step_38, 0), 4) AS ratio_39_to_38,
  ROUND(step_40 / NULLIF(step_39, 0), 4) AS ratio_40_to_39,
  ROUND(step_41 / NULLIF(step_40, 0), 4) AS ratio_41_to_40,
  ROUND(step_42 / NULLIF(step_41, 0), 4) AS ratio_42_to_41,
  ROUND(step_43 / NULLIF(step_42, 0), 4) AS ratio_43_to_42,
  ROUND(step_44 / NULLIF(step_43, 0), 4) AS ratio_44_to_43,
  ROUND(step_45 / NULLIF(step_44, 0), 4) AS ratio_45_to_44,
  ROUND(step_46 / NULLIF(step_45, 0), 4) AS ratio_46_to_45

FROM version_aggregates v
LEFT JOIN media_type_mapping m ON v.mediasource = m.mediasource
WHERE COALESCE(step_01, 0) <> 0
