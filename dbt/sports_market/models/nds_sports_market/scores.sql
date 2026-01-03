{{ config(
    materialized='incremental',
    tags=['gold','the_odds_api'],
    database='sports_market',
    schema='nds_sports_market',
) }}

SELECT DISTINCT ON (event_id, batch_id)
	event_id,
	commence_time,
	completed_flg,
	last_update,
	home_team,
	MAX(CASE WHEN score_name = home_team THEN score_score END) home_team_score,
	away_team,
	MAX(CASE WHEN score_name = away_team THEN score_score END) away_team_score,
	batch_id
FROM {{ ref('stg_scores') }}
  where batch_id > coalesce((
      select max(batch_id) from {{ this }}
  ), 0)
GROUP BY event_id,
	commence_time,
	completed_flg,
	last_update,
	home_team,
	away_team,
	batch_id
ORDER BY event_id, batch_id, commence_time
