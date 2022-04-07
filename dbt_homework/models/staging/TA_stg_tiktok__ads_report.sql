{{
    config(
        materialized='incremental',
        unique_key='unique_airbyte_id'
    )
}}

with final as (
    select 
        _airbyte_ab_id as unique_airbyte_id, 
        _airbyte_emitted_at as pulled_from_data_source_at,
        PARSE_TIMESTAMP('"%Y-%m-%d %H:%M:%S"', json_extract(_airbyte_data, '$.dimensions.stat_time_day')) as metrics_timestamp,
        json_extract(_airbyte_data, '$.metrics.campaign_id') as campaign_id,
        json_extract(_airbyte_data, '$.metrics.campaign_name') as campaign_name,
        json_extract(_airbyte_data, '$.metrics.adgroup_id') as adgroup_id,
        json_extract(_airbyte_data, '$.dimensions.ad_id') as ad_id,
        CAST(json_extract(_airbyte_data, '$.metrics.impressions') AS integer) as impressions,
        CAST(json_extract(_airbyte_data, '$.metrics.clicks') AS integer) as clicks,
        CAST(json_extract(_airbyte_data, '$.metrics.conversion') AS integer) as conversions,
        CAST(json_extract(_airbyte_data, '$.metrics.spend') AS numeric) as cost,
        CAST(json_extract(_airbyte_data, '$.metrics.cpc') AS numeric) as cpc
        
    from {{ source('tiktok', '_airbyte_raw_tiktok_ads_reports') }}

    {% if is_incremental() %}

    -- check the last 'pulled_from_data_source' timestamp in our staging table 
    -- and then check if there are newer timestamps in the source data, if there are, we take those rows to be transformed.
    where pulled_from_data_source_at > (select max(pulled_from_data_source_at) from {{ this }})

    {% endif %}
)

select * from final
