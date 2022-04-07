-- Check if all values of 'impressions' are positive
select
    impressions
from {{ ref('TA_mart_tiktok__campaign_perf')}}
where impressions < 0