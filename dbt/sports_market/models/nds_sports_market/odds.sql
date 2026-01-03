{{ config(
    materialized='incremental',
    tags=['gold','the_odds_api'],
    database='sports_market',
    schema='nds_sports_market',
) }}

SELECT DISTINCT ON (event_id, bookmaker_key, market_key, batch_id)
	event_id,
	sport_key,
	sport_title,
	commence_time,
	bookmaker_key,
	bookmaker_title,
	bookmaker_last_update,
	market_key,
	market_last_update,
	home_team,
	MAX(CASE WHEN outcome_name = home_team THEN outcome_price END) home_team_outcome_price,
	MAX(CASE WHEN outcome_name = home_team THEN outcome_point END) home_team_outcome_point,
	away_team,
	MAX(CASE WHEN outcome_name = away_team THEN outcome_price END) away_team_outcome_price,
	MAX(CASE WHEN outcome_name = away_team THEN outcome_point END) away_team_outcome_point,
	batch_id,
    CURRENT_TIMESTAMP::timestamptz(0) AT TIME ZONE 'America/New_York' load_timestamp
FROM {{ ref('stg_odds')}}
where batch_id > coalesce((
      select max(batch_id) from {{ this }}
  ), 0)
GROUP BY
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
	batch_id
ORDER BY event_id, bookmaker_key, market_key, batch_id DESC
