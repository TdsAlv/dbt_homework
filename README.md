# dbt_homework

homework for analytics engineer position

Step 1: Register source in dbt.
We are getting a dump from Airbyte, and the column with data is **\_airbyte_dat**a** which contains JSON objects.
So the first step would be to register the **raw.\_airbyte_raw_tiktok_ads_reports\*\* table as a source.

Step 2: Stage raw data.
First we flatten the JSON data into separate columns, then do some column renaming.
stg_tiktok\_\_ads_report.sql file co

Step 3: Create fact table for metrics.
Pull metrics into fct_advertising_metrics table.
