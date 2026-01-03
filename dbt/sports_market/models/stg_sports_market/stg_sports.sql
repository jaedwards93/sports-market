{{ config(
    materialized='incremental',
    tags=['silver','the_odds_api'],
    database='sports_market',
    schema='stg_sports_market',
    indexes=[
        {'columns': ['sport_key', 'batch_id'], 'unique': true},
        {'columns': ['sport_key', 'sport_group','actv_flg','load_timestamp']}
    ]
) }}

WITH source_cte AS (
    SELECT
        pk,
        json payload,
        batch_id,
        crt_dt
    FROM {{source('raw_the_odds_api','sports')}}
), flat_cte AS (
    SELECT
        payload->>'key' sport_key,
        payload->>'group' sport_group,
        payload->>'title' sport_title,
        (payload->>'active')::boolean::int ACTV_FLG,
        payload->>'description' sport_description,
        (payload->>'has_outrights')::boolean::int OUTRIGHTS_FLG,
        pk raw_pk,
        batch_id::bigint,
        crt_dt::timestamptz(0) AT TIME ZONE 'America/New_York' LOAD_TIMESTAMP
    FROM source_cte
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['sport_key', 'batch_id']) }} as pk,
    sport_key,
    sport_group,
    sport_title,
    ACTV_FLG,
    sport_description,
    OUTRIGHTS_FLG,
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