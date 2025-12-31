{{ config(
    materialized='table',
    tags=['gold','the_odds_api'],
    database= 'sports_market',
    schema='nds_sports_market',
    indexes=[
        {'columns': ['sport_key','actv_flg'], 'unique': true}
    ]
) }}

WITH rnk_cte AS (
	SELECT
		sport_key,
		sport_group,
		sport_title,
		actv_flg,
		sport_description,
		outrights_flg,
		batch_id,
		CURRENT_TIMESTAMP::timestamp(0) CRT_DT,
		ROW_NUMBER() OVER(PARTITION BY sport_key ORDER BY batch_id DESC) rnk
	FROM {{ ref('stg_sports') }}
)
SELECT
	sport_key,
	sport_group,
	sport_title,
	actv_flg,
	sport_description,
	outrights_flg,
	batch_id,
	CRT_DT
FROM rnk_cte
WHERE rnk = 1