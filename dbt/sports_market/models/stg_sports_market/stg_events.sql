{{ config(
    materialized='incremental',
    tags=['silver','the_odds_api'],
    database='sports_market',
    schema='stg_sports_market',
) }}

WITH source_cte AS (
	SELECT
		pk,
		json payload,
		batch_id,
		crt_dt
	FROM {{source('raw_the_odds_api','events')}}
), flat_cte AS (
    SELECT
        a.payload->>'id' event_id,
        a.payload->>'home_team' home_team,
        a.payload->>'away_team' away_team,
        a.payload->>'sport_key' sport_key,
        a.payload->>'sport_title' sport_title,
        (a.payload->>'commence_time')::timestamptz(0) AT TIME ZONE 'America/New_York' commence_time,
        pk raw_pk,
        batch_id::bigint,
        crt_dt::timestamptz(0) AT TIME ZONE 'America/New_York' LOAD_TIMESTAMP
    FROM source_cte a
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['event_id', 'batch_id']) }} pk,
    event_id,
    home_team,
    away_team,
    sport_key,
    sport_title,
    commence_time,
    raw_pk,
    batch_id,
    LOAD_TIMESTAMP
FROM flat_cte


{% if is_incremental() %}
  -- only pull rows with batch_id newer than what we've already loaded
  where batch_id > coalesce((
      select max(batch_id) from {{ this }}
  ), 0)
{% endif %}