{{ config(
    materialized='incremental',
    tags=['silver','the_odds_api'],
    database='sports_market',
    schema='stg_sports_market',
    indexes=[
        {'columns': ['sports_sid'], 'unique': true},
        {'columns': ['sport_key', 'sport_group','actv_flg','load_timestamp']}
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
    {{ dbt_utils.generate_surrogate_key(['sports_sid', 'batch_id']) }} as sports_sid,
	sports_sid::bigint raw_sports_sid,
	payload->>'key' sport_key,
	payload->>'group' sport_group,
	payload->>'title' sport_title,
	(payload->>'active')::boolean::int ACTV_FLG,
	payload->>'description' sport_description,
	(payload->>'has_outrights')::boolean::int OUTRIGHTS_FLG,
	batch_id::bigint,
	crt_dt::timestamp(0) LOAD_TIMESTAMP
FROM source_cte

{% if is_incremental() %}
  -- only pull rows with batch_id newer than what we've already loaded
  where batch_id > coalesce((
      select max(batch_id) from {{ this }}
  ), 0)
{% endif %}