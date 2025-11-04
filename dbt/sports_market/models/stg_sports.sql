{{ config(
    materialized='table',
    tags=['silver','the_odds_api'],
    indexes=[
        {'columns': ['sports_sid'], 'unique': true},
        {'columns': ['sport_key', 'sport_group','actv_flg','load_timestampd']}
    ]
) }}

WITH source_cte AS (
    SELECT
        sports_sid,
        json payload,
        batch_id,
        crt_dt
    FROM {{source('raw_the_odds_api','sports')}}
)
SELECT
	sports_sid::bigint,
	payload->>'key' sport_key,
	payload->>'group' sport_group,
	payload->>'title' sport_title,
	(payload->>'active')::boolean::int ACTV_FLG,
	payload->>'description' sport_description,
	(payload->>'has_outrights')::boolean::int OUTRIGHTS_FLG,
	batch_id::bigint,
	crt_dt::timestamp(0) LOAD_TIMESTAMP
FROM source_cte