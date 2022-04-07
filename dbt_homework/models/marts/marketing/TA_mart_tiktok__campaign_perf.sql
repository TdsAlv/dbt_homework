{{
    config(
        materialized='incremental',
        unique_key='unique_data_id'
    )
}}

-- aggregations for final report
with aggregates as (

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
        where _pulled_from_data_source_at > (select max(_pulled_from_data_source_at) from {{ this }})

    {% endif %}

    group by metrics_date, campaign_name
),

-- temp table so that we could check for last timestamp of pulled data
t1 as (
    select 
        max(_pulled_from_data_source_at) as _pulled_from_data_source_at,
    from {{ ref('TA_stg_tiktok__ads_report') }}
),

-- select everything from aggregates and add the last pull timestamp 
subfinal as (
    select 
        aggregates.*, t1._pulled_from_data_source_at from aggregates, t1
),

-- add a unique_data_id
final as (
    select 
        *,
        {{ dbt_utils.surrogate_key(['metrics_date', 'campaign_name']) }} as unique_data_id
    from subfinal
)

select * from final
