{{
    config(
        materialized='incremental'
    )
}}

with final as (

    SELECT DISTINCT
        campaign_id,
        campaign_name

    from {{ref('TA_stg_tiktok_ads_report')}}
)

Select * from final