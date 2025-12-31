{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    tags=['silver','the_odds_api'],
    database='sports_market',
    schema='stg_sports_market',
) }}

WITH source_cte AS (
	SELECT
		odds_sid,
		json payload,
		batch_id,
		crt_dt
	FROM {{source('raw_the_odds_api','odds')}}
), parsed_cte AS (
    SELECT
        a.odds_sid::bigint,
        a.payload->>'sport_key' sport_key,
        a.payload->>'sport_title' sport_title,
        (a.payload->>'commence_time') commence_time,
        a.payload->>'home_team' home_team,
        a.payload->>'away_team' away_team,
        bookmaker->>'key' bookmaker_key,
        bookmaker->>'title' bookmaker_title,
        (bookmaker->>'last_update') bookmaker_last_update,
        market->>'key' market_key,
        (market->>'last_update') market_last_update,
        outcome->>'name' outcome_name,
        outcome->>'price' outcome_price,
        outcome->>'point' outcome_point,
        batch_id::bigint,
        crt_dt::timestamp(0) LOAD_TIMESTAMP
    FROM source_cte a
    CROSS JOIN jsonb_array_elements(payload->'bookmakers') as bookmaker
    CROSS JOIN jsonb_array_elements(bookmaker->'markets') as market
    CROSS JOIN jsonb_array_elements(market->'outcomes') as outcome
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['odds_sid', 'bookmaker_key', 'market_key', 'outcome_name', 'batch_id']) }} as sports_sid,
	odds_sid raw_odds_sid,
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
	batch_id,
	LOAD_TIMESTAMP
FROM parsed_cte


{% if is_incremental() %}
  -- only pull rows with batch_id newer than what we've already loaded
  where batch_id > coalesce((
      select max(batch_id) from {{ this }}
  ), 0)
{% endif %}