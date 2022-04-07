# dbt_homework

## Homework for analytics engineer position

**Step 1**: Register source in dbt.
We are getting a data dump from Airbyte, and the column with data is **\_airbyte_dat**a** which contains JSON objects.
So the first step would be to register the **raw.\_airbyte_raw_tiktok_ads_reports\*\* table as a source.

**Step 2**: Stage raw data.
We flatten the JSON data into tabular format, then do some column renaming and data type recasting.
Also adding a hash of JSON blob, this will make sure that on incremental loads, the rows are updated and not newly created.

**Step 3**: Aggregate campaign data. Metrics grouped by each day and campaign. Added NULLIF() function in case we get 0 clicks or conversions in a day, so that we don't get division by 0.

**Step 4**: Make models incremental.
According to dbt documentation on incremental models, dbt will run transformation on full data the first time it runs, and next time it will only transform the rows that we tell it to.
This is done via is_incremental() macro, which should wrap a WHERE clause that would filter for the newly added rows.
By looking at the **\_airbyte_emitted_at** field it seems that the pulling of data from source is also incremental, as there are multiple different timestamps for different data chunks, so the pulls happened on multiple days.

So I'll use this field as a cursor for the incremental model.

**Step 5**: Add tests.
Testing source: test if \_airbyte_ab_id is unique aad not_null. Test relationship: we want that each '\_airbyte_ab_id' in the source, exists as an id in staging tables 'unique_airbyte_id'.

Testing stg: test if unique_airbyte_id is unique and not_null, test if clicks amount is non-negative (using dbt_utils.expression_is_true).

Testing mart: test if date & campaign combination is unique. Report shows data by day for each campaign, so we don't want duplicate dates for a single campaign somehow to happen, and our test should fail so that we can be alerted and maybe do a full refresh. Also we can test if impressions are non-negative.

**Comment what data modeling and dbt best practices you use and why**
I tried to create models that dbt documentation says we should pretty much always have:

- staging model, where it is a 1:1 match with source data just with some cleaning (renaming, recasting or JSON flattening as in this case)

- marts model, where we usually should provide fact and dimension tables, but my experience with those is limited as I'm not exactly sure if we should use them with event data like this. So in this case I just produced the final report in the marts directory.

Models themselves should be created by using CTEs.
For each model we should provide some documentation on the columns and at least a few tests.

I've used source and ref macros in the project so that in case the table names change, you only need to change it in one place (yaml configuration)

At least one test done on all levels (source/staging/marts)

Also I've tried to use some built-in dbt functions like dbt_utils.expression_is_true for tests.

**Describe how you would monitor that everything is ok and ensure data quality. Is the dbt tool enough for that? What other solutions would you use?**
I guess one of the first steps would be to have separate dev and prod environments, where analytics engineer does all the development in dev, making sure all the tests pass before implementing the solution to production.
That would be at least the first step to ensuring data quality.

Some sort of CI/CD process might be implemented for this (like pushing code to git development branch, doing a pull request to main branch, which when accepted would trigger additional tests or something like that)

There also seems to be a popular python package 'great-expectations' for some additional statistical tests on data. It has been ported to dbt as dbt_expectations, but I haven't used it, so not sure what additional benefits it gives.

These tools will test data, but they do not check if the whole data pipeline succeeded or not.

Airflow could be used as pipeline orchestrator with an integration to Slack for example, so that if an error occurs anywhere in the pipeline, data engineers would be notified via message.
I've heard about some other tools to measure data pipeline performance (Prometheus for monitoring + Grafana for visualization of those monitoring metrics), but I have not used or seen them in action.

**Which data schema do you think is best?**
Tough question - I don't have a good answer to that.

Looks like STAR schema is still quite popular, although it was designed when data storage was costly, which is not the case right now. I guess the main advantage of it right now is readability - organizing your data around events (facts) and description of those events (dimensions).

On one end of the spectrum there seems to be people who like Data Vault, but that seems to be a very cumbersome modeling technique.

On the other end, some people are just pushing for OBT (one big table), as with current data warehouses the speed may not be an issue.
