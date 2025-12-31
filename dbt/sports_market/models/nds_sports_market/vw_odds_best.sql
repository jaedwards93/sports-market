{{ config(
    tags=['gold','the_odds_api'],
    database= 'sports_market',
    schema='nds_sports_market'
) }}

WITH most_recent_cte AS (
	SELECT
		sport_key,
		sport_title,
		commence_time,
		home_team,
		away_team,
		bookmaker_key,
		bookmaker_title,
		market_key,
		outcome_name,
		outcome_price,
		outcome_point,
		batch_id,
		load_timestamp,
		ROW_NUMBER() OVER(PARTITION BY sport_key, commence_time, bookmaker_key, market_key, outcome_name ORDER BY batch_id DESC) rnk
	FROM {{ ref('odds') }}
), best_payout_rnk_cte AS (
SELECT *,
	DENSE_RANK() OVER(PARTITION BY sport_key, commence_time, market_key, outcome_name ORDER BY outcome_price DESC, COALESCE(outcome_point,0) DESC) payout_rnk
FROM most_recent_cte
WHERE rnk = 1
)
SELECT
    sport_key,
    sport_title,
    commence_time,
    home_team,
    away_team,
    bookmaker_key,
    bookmaker_title,
    market_key,
    outcome_name,
    outcome_price,
    outcome_point,
    batch_id,
    load_timestamp
FROM best_payout_rnk_cte
WHERE payout_rnk = 1
    AND commence_time >= CURRENT_DATE
ORDER BY commence_time, outcome_name, market_key, bookmaker_key