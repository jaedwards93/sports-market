{{ config(
    materialized='incremental',
    tags=['silver','the_odds_api'],
    database='sports_market',
    schema='stg_sports_market',
    indexes=[
        {'columns': ['event_id', 'score_name', 'batch_id'], 'unique': true},
        {'columns': ['event_id', 'home_team','away_team','commence_time']},
        {'columns': ['completed_flg']}
    ]
) }}

WITH source_cte AS (
	SELECT
		pk,
		json payload,
		batch_id,
		crt_dt
	FROM {{source('raw_the_odds_api','scores')}}
), flat_cte AS (
    SELECT
        a.payload->>'id' event_id,
        a.payload->>'sport_key' sport_key,
        a.payload->>'sport_title' sport_title,
        a.payload->>'home_team' home_team,
        a.payload->>'away_team' away_team,
        (a.payload->>'commence_time')::timestamptz(0) AT TIME ZONE 'America/New_York' commence_time,
        (a.payload->>'completed')::boolean::int completed_flg,
        (a.payload->>'last_update')::timestamptz(0) AT TIME ZONE 'America/New_York' last_update,
        (score->>'name') score_name,
		(score->>'score')::DECIMAL(10,2) score_score,
		pk raw_pk,
        batch_id::bigint,
        crt_dt::timestamptz(0) AT TIME ZONE 'America/New_York' LOAD_TIMESTAMP
    FROM source_cte a
	CROSS JOIN jsonb_array_elements(
        CASE
            WHEN jsonb_typeof(a.payload->'scores') = 'array' THEN a.payload->'scores'
            ELSE '[]'::jsonb
        END) score
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['event_id', 'score_name', 'batch_id']) }} as pk,
    event_id,
    sport_key,
    sport_title,
    home_team,
    away_team,
    commence_time,
    completed_flg,
    last_update,
    score_name,
    score_score,
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