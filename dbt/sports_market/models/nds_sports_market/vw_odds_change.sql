{{ config(
    tags=['gold','the_odds_api'],
    database= 'sports_market',
    schema='nds_sports_market'
) }}

with derived_value_cte AS (
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
		FIRST_VALUE(load_timestamp) OVER (
		    PARTITION BY sport_key, commence_time, bookmaker_key, market_key, outcome_name
		    ORDER BY load_timestamp
		    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
		 ) FIRST_LOAD_TIMESTAMP ,
		LAST_VALUE(load_timestamp) OVER (
		    PARTITION BY sport_key, commence_time, bookmaker_key, market_key, outcome_name
		    ORDER BY load_timestamp
		    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
		  ) LAST_LOAD_TIMESTAMP,
		FIRST_VALUE(outcome_price) OVER (
		    PARTITION BY sport_key, commence_time, bookmaker_key, market_key, outcome_name
		    ORDER BY load_timestamp
		    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
		  ) FIRST_OUTCOME_PRICE,
		LAST_VALUE(outcome_price) OVER (
		    PARTITION BY sport_key, commence_time, bookmaker_key, market_key, outcome_name
		    ORDER BY load_timestamp
		    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
		  ) LAST_OUTCOME_PRICE,
		 FIRST_VALUE(outcome_point) OVER (
		    PARTITION BY sport_key, commence_time, bookmaker_key, market_key, outcome_name
		    ORDER BY load_timestamp
		    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
		  ) FIRST_OUTCOME_POINT,
		LAST_VALUE(outcome_point) OVER (
		    PARTITION BY sport_key, commence_time, bookmaker_key, market_key, outcome_name
		    ORDER BY load_timestamp
		    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
		  ) LAST_OUTCOME_POINT
	FROM {{ ref('odds') }}
	WHERE commence_time >= CURRENT_DATE
)
SELECT DISTINCT
	sport_key,
	sport_title,
	commence_time,
	home_team,
	away_team,
	bookmaker_key,
	bookmaker_title,
	market_key,
	outcome_name,
	FIRST_LOAD_TIMESTAMP ,
	LAST_LOAD_TIMESTAMP,
	LAST_LOAD_TIMESTAMP - FIRST_LOAD_TIMESTAMP TIMESTAMP_DIFF,
	FIRST_OUTCOME_PRICE,
	LAST_OUTCOME_PRICE,
	ROUND(
		ABS(((LAST_OUTCOME_PRICE-FIRST_OUTCOME_PRICE*1.0)/FIRST_OUTCOME_PRICE) * 100)
	,2)
	OUTCOME_PRICE_DIFF_PERCENT,
	CASE
      WHEN LAST_OUTCOME_PRICE > FIRST_OUTCOME_PRICE THEN 'UP'
      WHEN LAST_OUTCOME_PRICE < FIRST_OUTCOME_PRICE THEN 'DOWN'
      ELSE 'FLAT'
    END AS OUTCOME_PRICE_DIRECTION,
	FIRST_OUTCOME_POINT,
	LAST_OUTCOME_POINT,
	ROUND(
		ABS(((LAST_OUTCOME_POINT-FIRST_OUTCOME_POINT*1.0)/FIRST_OUTCOME_POINT) * 100)
	,2)
	OUTCOME_POINT_DIFF_PERCENT,
	CASE
      WHEN LAST_OUTCOME_POINT > FIRST_OUTCOME_POINT THEN 'UP'
      WHEN LAST_OUTCOME_POINT < FIRST_OUTCOME_POINT THEN 'DOWN'
      ELSE 'FLAT'
    END AS OUTCOME_POINT_DIRECTION
FROM derived_value_cte
ORDER BY commence_time, outcome_name, market_key