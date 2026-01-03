{{ config(
    materialized='incremental',
    tags=['silver','the_odds_api'],
    database='sports_market',
    schema='stg_sports_market',
    indexes=[
        {'columns': ['event_id', 'bookmaker_key', 'market_key', 'outcome_name', 'batch_id'], 'unique': true},
        {'columns': ['event_id', 'home_team','away_team','commence_time']}
    ]
) }}

WITH source_cte AS (
	SELECT
		pk,
		json payload,
		batch_id,
		crt_dt
	FROM {{source('raw_the_odds_api','odds')}}
), flat_cte AS (
    SELECT
        (a.payload->>'id')::text event_id,
        a.payload->>'sport_key' sport_key,
        a.payload->>'sport_title' sport_title,
        (a.payload->>'commence_time')::timestamptz(0) AT TIME ZONE 'America/New_York' commence_time,
        a.payload->>'home_team' home_team,
        a.payload->>'away_team' away_team,
        bookmaker->>'key' bookmaker_key,
        bookmaker->>'title' bookmaker_title,
        (bookmaker->>'last_update')::timestamptz(0) AT TIME ZONE 'America/New_York' bookmaker_last_update,
        market->>'key' market_key,
        (market->>'last_update')::timestamptz(0) AT TIME ZONE 'America/New_York' market_last_update,
        outcome->>'name' outcome_name,
        (outcome->>'price')::INTEGER outcome_price,
        (outcome->>'point')::DECIMAL(10,2) outcome_point,
		pk raw_pk,
        batch_id::bigint,
        crt_dt::timestamptz(0) AT TIME ZONE 'America/New_York' LOAD_TIMESTAMP
    FROM source_cte a
    CROSS JOIN jsonb_array_elements(payload->'bookmakers') as bookmaker
    CROSS JOIN jsonb_array_elements(bookmaker->'markets') as market
    CROSS JOIN jsonb_array_elements(market->'outcomes') as outcome
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['event_id', 'bookmaker_key', 'market_key', 'outcome_name', 'batch_id']) }} as pk,
    event_id,
	sport_key,
	sport_title,
	commence_time,
	home_team,
	away_team,
	bookmaker_key,
	bookmaker_title,
	bookmaker_last_update,
	market_key,
	market_last_update,
	outcome_name,
	outcome_price,
	outcome_point,
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