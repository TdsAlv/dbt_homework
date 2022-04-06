{{
    config(
        materialized='incremental',
        unique_key='unique_row_id'
    )
}}

with final as (
    select 
        _airbyte_ab_id as unique_row_id, 
        _airbyte_emitted_at as pulled_from_data_source_at,
        json_extract(_airbyte_data, '$.metrics.campaign_id') as campaign_id,
        json_extract(_airbyte_data, '$.metrics.campaign_name') as campaign_name,
        json_extract(_airbyte_data, '$.metrics.adgroup_id') as adgroup_id,
        json_extract(_airbyte_data, '$.dimensions.ad_id') as ad_id,
        CAST(json_extract(_airbyte_data, '$.metrics.impressions') AS integer) as impressions,
        CAST(json_extract(_airbyte_data, '$.metrics.clicks') AS integer) as clicks,
        CAST(json_extract(_airbyte_data, '$.metrics.conversion') AS integer) as conversions,
        CAST(json_extract(_airbyte_data, '$.metrics.spend') AS numeric) as cost,
        CAST(json_extract(_airbyte_data, '$.metrics.cpc') AS numeric) as cpc,
        json_extract(_airbyte_data, '$.dimensions.stat_time_day') as metrics_date
        
    from {{ source('tiktok', '_airbyte_raw_tiktok_ads_reports') }}

    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    -- and here it will check for any newly added events
    where metrics_date > (select max(metrics_date) from {{ this }})

    {% endif %}
)

select * from final
