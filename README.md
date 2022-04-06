# dbt_homework

homework for analytics engineer position

Step 1: Register source in dbt.
We are getting a dump from Airbyte, and the column with data is **\_airbyte_dat**a** which contains JSON objects.
So the first step would be to register the **raw.\_airbyte_raw_tiktok_ads_reports\*\* table as a source.

Step 2: Stage raw data.
First we flatten the JSON data into separate columns, then do some column renaming.
stg_tiktok\_\_ads_report.sql file co

Step 3: Aggregate campaign data.

Last step: Make models incremental.
According to dbt documentation on incremental models, dbt will run transformation on full data the first time it runs. Next time it will only transform the rows that we tell it to.
This is done via is_incremental() macro, which should wrap a WHERE clause that would filter for the newly added rows.
I will assume that **stat_time_day** will be the field to filter for new rows as new events will be added every day.

{% if is_incremental() %}

-- this filter will only be applied on an incremental run
where stat_time_day > (select max(stat_time_day) from {{ this }})

{% endif %}

Also there is an optional parameter of 'unique_key' for incremental models, which should make sure that no duplicate rows are inserted into target table, and instead if duplicate key is found, the row in target table is updated. (at least I think this is how it works :) )
