{{
    config(
        materialized='incremental',
        unique_key='unique_row_id'
    )
}}

-- t1 will be just used for adding a surrogate key to check uniqueness and for checking which new rows have been
-- added to our staging table
with t1 as (
    select
        -- a row can be uniquely identified by ad_id + metrics_timestamp combination
        {{ dbt_utils.surrogate_key(['metrics_timestamp', 'ad_id']) }} as unique_row_id,
        *
    from {{ ref('TA_stg_tiktok__ads_report')}}


    {% if is_incremental() %}
      
      -- just as with the staging model we check for new rows by looking at 'pulled_from_data_source' timestamp
      where pulled_from_data_source_at > (select max(pulled_from_data_source_at) from {{ this }})

    {% endif %}
),

final as (
    select
        DATE(metrics_timestamp) as metrics_date,
        campaign_name,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        sum(conversions) as conversions,
        sum(cost) as cost,
        round(sum(cost) / NULLIF(sum(clicks),0), 2) as cpc,
        round(sum(cost) / NULLIF(sum(conversions),0), 2) as cpa
    from t1
    group by metrics_date, campaign_name
)

select * from final
