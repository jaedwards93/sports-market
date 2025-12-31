{{ config(
    materialized='incremental',
    tags=['gold','the_odds_api'],
    database='sports_market',
    schema='nds_sports_market',
) }}

SELECT
    sports_sid,
    sport_key,
    sport_title,
    commence_time::timestamptz(0) AT TIME ZONE 'America/New_York' commence_time,
    home_team,
    away_team,
    bookmaker_key,
    bookmaker_title,
    bookmaker_last_update::timestamptz(0) AT TIME ZONE 'America/New_York' bookmaker_last_update,
    market_key,
    market_last_update::timestamptz(0) AT TIME ZONE 'America/New_York' market_last_update,
    outcome_name,
    outcome_price::INTEGER outcome_price,
    outcome_point::DECIMAL(10,2) outcome_point,
    batch_id::BIGINT,
    load_timestamp::timestamptz(0) AT TIME ZONE 'America/New_York' load_timestamp,
    CURRENT_TIMESTAMP::timestamptz(0) AT TIME ZONE 'America/New_York' CRT_DT
FROM {{ ref('stg_odds')}}


{% if is_incremental() %}
  -- only pull rows with batch_id newer than what we've already loaded
  where batch_id > coalesce((
      select max(batch_id) from {{ this }}
  ), 0)
{% endif %}