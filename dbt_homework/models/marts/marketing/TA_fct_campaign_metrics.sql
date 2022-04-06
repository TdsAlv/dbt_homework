{{
    config(
        materialized='incremental'
    )
}}

with final as (
    SELECT

        impressions,
        clicks,
        conversions,
        cost
    from {{ref('TA_stg_tiktok_ads_report')}}
)

select * from final