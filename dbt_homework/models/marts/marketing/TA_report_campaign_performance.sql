with final as (
    select
        metrics_date,
        campaign_name,
        sum(clicks) as total_clicks,
        sum(impressions) as total_impressions,
        sum(conversions) as total_conversions,
        sum(cost) as total_cost,
        sum(cost) / sum(clicks) as cpc,
        sum(cost) / sum(conversions) as cpa
    group by
        metrics_date, campaign_name
    from {{ ref('TA_stg_tiktok_ads_report')}}
)

select * from final;