{{
    config(
        materialized='incremental',
    )
}}

with final as (
    select
        metrics_date,
        campaign_name,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        sum(conversions) as conversions,
        sum(cost) as cost,
        sum(cost) / NULLIF(sum(clicks),0) as cpc,
        sum(cost) / NULLIF(sum(conversions),0) as cpa
    from {{ ref('TA_stg_tiktok__ads_report')}}


    {% if is_incremental() %}

      where metrics_date > (select max(metrics_date) from {{ this }})

    {% endif %}
    
    group by
        metrics_date, campaign_name
)

select * from final