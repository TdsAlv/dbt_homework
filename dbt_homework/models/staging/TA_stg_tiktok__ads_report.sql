{{
    config(
        materialized='incremental'
    )
}}

select 
    _airbyte_ab_id as airbyte_row_id, 
    _airbyte_emitted_at as pulled_from_data_source_at,
    json_extract(_airbyte_data, '$.metrics.campaign_id') as campaign_id,
    json_extract(_airbyte_data, '$.metrics.campaign_name') as campaign_name,
    json_extract(_airbyte_data, '$.metrics.adgroup_id') as adgroup_id,
    json_extract(_airbyte_data, '$.dimensions.ad_id') as ad_id,
    json_extract(_airbyte_data, '$.metrics.impressions') as impressions,
    json_extract(_airbyte_data, '$.metrics.clicks') as clicks,
    json_extract(_airbyte_data, '$.metrics.conversion') as conversions,
    json_extract(_airbyte_data, '$.metrics.spend') as cost,
    json_extract(_airbyte_data, '$.metrics.cpc') as cpc,
    json_extract(_airbyte_data, '$.dimensions.stat_time_day') as stat_time_day
    
from {{ source('tiktok', '_airbyte_raw_tiktok_ads_reports') }}

