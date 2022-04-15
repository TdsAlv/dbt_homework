{{
    config(
        materialized='incremental',
        unique_key='_unique_data_id'
    )
}}


-- This model will deal with possible 'old data updates', that is, if for some reason the same data arrives
-- in the next Airbyte pull, it will be updated (not inserted as a new row) in the staging table.
-- This means that for this aggregated model, we will need to re-aggregate old data.

-- fetch a list of last airbyte pull timestamps
with last_airbyte_pull as (
    select max(_pulled_from_data_source_at) as _pulled_from_data_source_at
    
    from {{ ref('TA_stg_tiktok__ads_report') }}

    {% if is_incremental() %}

        where _pulled_from_data_source_at > (select max(_pulled_from_data_source_at) from {{ this }})

    {% endif %}
),

-- this will get all unique dates that we might need to re-fetch from staging
-- in case some older data arrives that will need to be re-aggregated.
event_dates_from_last_airbyte_pull as (
    select distinct date(metrics_timestamp) as days_to_reaggregate
    from {{ ref('TA_stg_tiktok__ads_report') }}

    where _pulled_from_data_source_at > (select max(_pulled_from_data_source_at) from {{ this }})
),

-- select all the data that was added into staging during last pull and also older rows to re-aggregate
all_data as (

    select *
    from {{ ref('TA_stg_tiktok__ads_report') }}

    {% if is_incremental() %}

        -- select newly added data to staging and also old data that will need to be re-aggregated
        where _pulled_from_data_source_at > (select max(_pulled_from_data_source_at) from {{ this }})
        or DATE(metrics_timestamp) in (select days_to_reaggregate from event_dates_from_last_airbyte_pull)

    {% endif %}
),


-- aggregations for final report
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

    from all_data

    group by metrics_date, campaign_name
),

-- select everything from aggregates and add _pulled_from_data_source_at timestamp 
subfinal as (
    select 
        aggregates.*, last_airbyte_pull._pulled_from_data_source_at 
        
    from aggregates, last_airbyte_pull
),

-- add a _unique_data_id so that we can update the re-aggregated values
final as (
    select 
        *,
        {{ dbt_utils.surrogate_key(['metrics_date', 'campaign_name']) }} as _unique_data_id
    from subfinal
)

select * from final