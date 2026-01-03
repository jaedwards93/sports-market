{{ config(
    materialized='table',
    tags=['gold','the_odds_api'],
    database= 'sports_market',
    schema='nds_sports_market',
    indexes=[
        {'columns': ['event_id'], 'unique': true}
    ]
) }}

SELECT DISTINCT ON (event_id)
	event_id,
	sport_key,
	sport_title,
	home_team,
	away_team,
	commence_time,
	pk stg_pk,
	raw_pk,
	batch_id,
	CURRENT_TIMESTAMP::timestamptz(0) AT TIME ZONE 'America/New_York' LOAD_TIMESTAMP
FROM {{ ref('stg_events') }}
ORDER BY event_id, batch_id DESC
