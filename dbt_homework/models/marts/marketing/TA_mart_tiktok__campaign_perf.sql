{{
    config(
        materialized='incremental',
        unique_key='unique_row_id'
    )
}}

-- temp table so that we could check for last timestamp of pulled data
-- unique_row_id will be used to not add duplicate data
with t1 as (
    select 
        max(pulled_from_data_source_at) as pulled_from_data_source_at,
        {{ dbt_utils.surrogate_key(['metrics_timestamp', 'ad_id']) }} as unique_row_id,
    
    from {{ ref('TA_stg_tiktok__ads_report') }}
),

aggregates as (

    select
        DATE(metrics_timestamp) as metrics_date,
        campaign_name,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        sum(conversions) as conversions,
        sum(cost) as cost,
        round(sum(cost) / NULLIF(sum(clicks),0), 2) as cpc,
        round(sum(cost) / NULLIF(sum(conversions),0), 2) as cpa

    from {{ ref('TA_stg_tiktok__ads_report') }}


    {% if is_incremental() %}
      
      -- check if new data is available in intermediate table
        where pulled_from_data_source_at > (select max(pulled_from_data_source_at) from {{ this }})

    {% endif %}

    group by metrics_date, campaign_name
),

-- select everything from aggregates and add the last pull timestamp with unique_row_id
final as (
    select 
        aggregates.*, t1.* from aggregates, t1
)

select * from final
