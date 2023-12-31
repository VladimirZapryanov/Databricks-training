# Databricks notebook source
1-Which of the following describes a benefit of a data lakehouse that is unavailable in a
traditional data warehouse? - E. A data lakehouse enables both batch and streaming analytics.

2-Which of the following locations hosts the driver and worker nodes of a
Databricks-managed cluster? - A. Data plane

3-A data architect is designing a data model that works for both video-based machine
learning workloads and highly audited batch ETL/ELT workloads.
Which of the following describes how using a data lakehouse can help the data architect
meet the needs of both workloads? - D. A data lakehouse stores unstructured data and is ACID-compliant.

4-Which of the following describes a scenario in which a data engineer will want to use a Job
cluster instead of an all-purpose cluster? - C. An automated workflow needs to be run every 30 minutes.

5-A data engineer has created a Delta table as part of a data pipeline. Downstream data
analysts now need SELECT permission on the Delta table.
Assuming the data engineer is the Delta table owner, which part of the Databricks
Lakehouse Platform can the data engineer use to grant the data analysts the appropriate
access? - C. Data Explorer

6-Two junior data engineers are authoring separate parts of a single data pipeline notebook.
They are working on separate Git branches so they can pair program on the same notebook
simultaneously. A senior data engineer experienced in Databricks suggests there is a better
alternative for this type of collaboration.
Which of the following supports the senior data engineer’s claim? - B. Databricks Notebooks support real-time coauthoring on a single notebook

7-Which of the following describes how Databricks Repos can help facilitate CI/CD workflows
on the Databricks Lakehouse Platform? - E. Databricks Repos can commit or push code changes to trigger a CI/CD process

8-Which of the following statements describes Delta Lake? - B. Delta Lake is an open format storage layer that delivers reliability, security, and
performance.

9-A data architect has determined that a table of the following format is necessary:
id birthDate avgRating
a1 1990-01-06 5.5
a2 1974-11-21 7.1
… … …
Which of the following code blocks uses SQL DDL commands to create an empty Delta
table in the above format regardless of whether a table already exists with this name? - B. CREATE OR REPLACE TABLE table_name (
id STRING,
birthDate DATE,
avgRating FLOAT
)

10-Which of the following SQL keywords can be used to append new rows to an existing Delta
table? - C. INSERT INTO

11-A data engineering team needs to query a Delta table to extract rows that all meet the same
condition. However, the team has noticed that the query is running slowly. The team has
already tuned the size of the data files. Upon investigating, the team has concluded that the
rows meeting the condition are sparsely located throughout each of the data files.
Based on the scenario, which of the following optimization techniques could speed up the
query? - B. Z-Ordering

12-A data engineer needs to create a database called customer360 at the location
/customer/customer360. The data engineer is unsure if one of their colleagues has
already created the database.
Which of the following commands should the data engineer run to complete this task? - C. CREATE DATABASE IF NOT EXISTS customer360 LOCATION '/customer/customer360';

13-A junior data engineer needs to create a Spark SQL table my_table for which Spark
manages both the data and the metadata. The metadata and data should also be stored in
the Databricks Filesystem (DBFS).
Which of the following commands should a senior data engineer share with the junior data
engineer to complete this task? - E. CREATE TABLE my_table (id STRING, value STRING);

14-A data engineer wants to create a relational object by pulling data from two tables. The
relational object must be used by other data engineers in other sessions. In order to save on
storage costs, the data engineer wants to avoid copying and storing physical data.
Which of the following relational objects should the data engineer create? - A. View

15-A data engineering team has created a series of tables using Parquet data stored in an
external system. The team is noticing that after appending new rows to the data in the
external system, their queries within Databricks are not returning the new rows. They identify
the caching of the previous data as the cause of this issue.
Which of the following approaches will ensure that the data returned by queries is always
up-to-date? - A. The tables should be converted to the Delta format

16-A table customerLocations exists with the following schema:
id STRING,
date STRING,
city STRING,
country STRING
A senior data engineer wants to create a new table from this table using the following
command:
CREATE TABLE customersPerCountry AS
SELECT country,
COUNT(*) AS customers
FROM customerLocations
GROUP BY country;
A junior data engineer asks why the schema is not being declared for the new table.
Which of the following responses explains why declaring the schema is not necessary? - A. CREATE TABLE AS SELECT statements adopt schema details from the source table and query.

17-A data engineer is overwriting data in a table by deleting the table and recreating the table.
Another data engineer suggests that this is inefficient and the table should simply be
overwritten instead.
Which of the following reasons to overwrite the table instead of deleting and recreating the
table is incorrect? - B. Overwriting a table results in a clean table history for logging and audit purposes.

18-Which of the following commands will return records from an existing Delta table my_table
where duplicates have been removed? - C. SELECT DISTINCT * FROM my_table;

19-A data engineer wants to horizontally combine two tables as a part of a query. They want to
use a shared column as a key column, and they only want the query result to contain rows
whose value in the key column is present in both tables.
Which of the following SQL commands can they use to accomplish this task? - A. INNER JOIN

20-A junior data engineer has ingested a JSON file into a table raw_table with the following
schema:
cart_id STRING,
items ARRAY<item_id:STRING>
The junior data engineer would like to unnest the items column in raw_table to result in a
new table with the following schema:
cart_id STRING,
item_id STRING
Which of the following commands should the junior data engineer run to complete this
task? - D. SELECT cart_id, explode(items) AS item_id FROM raw_table;

21-A data engineer has ingested a JSON file into a table raw_table with the following schema:
transaction_id STRING,
payload ARRAY<customer_id:STRING, date:TIMESTAMP, store_id:STRING>
The data engineer wants to efficiently extract the date of each transaction into a table with
the following schema:
transaction_id STRING,
date TIMESTAMP
Which of the following commands should the data engineer run to complete this task? - B. SELECT transaction_id, payload.date FROM raw_table;

22-A data analyst has provided a data engineering team with the following Spark SQL query:
SELECT district,
avg(sales)
FROM store_sales_20220101
GROUP BY district;
The data analyst would like the data engineering team to run this query every day. The date
at the end of the table name (20220101) should automatically be replaced with the current
date each time the query is run.
Which of the following approaches could be used by the data engineering team to
efficiently automate this process? - A. They could wrap the query using PySpark and use Python’s string variable system to automatically update the table name.

23-A data engineer has ingested data from an external source into a PySpark DataFrame
raw_df. They need to briefly make this data available in SQL for a data analyst to perform a
quality assurance check on the data.
Which of the following commands should the data engineer run to make this data available
in SQL for only the remainder of the Spark session? - A. raw_df.createOrReplaceTempView("raw_df")

24-A data engineer needs to dynamically create a table name string using three Python
variables: region, store, and year. An example of a table name is below when region =
"nyc", store = "100", and year = "2021":
nyc100_sales_2021
Which of the following commands should the data engineer use to construct the table name
in Python? - D. f"{region}{store}_sales_{year}"

25-A data engineer has developed a code block to perform a streaming read on a data source.
The code block is below:
(spark
.read
.schema(schema)
.format("cloudFiles")
.option("cloudFiles.format", "json")
.load(dataSource)
)
The code block is returning an error.
Which of the following changes should be made to the code block to configure the block to
successfully perform a streaming read? - A. The .read line should be replaced with .readStream.

26-A data engineer has configured a Structured Streaming job to read from a table, manipulate
the data, and then perform a streaming write into a new table.
The code block used by the data engineer is below:
(spark.table("sales")
.withColumn("avg_price", col("sales") / col("units"))
.writeStream
.option("checkpointLocation", checkpointPath)
.outputMode("complete")
._____
.table("new_sales")
)
If the data engineer only wants the query to execute a single micro-batch to process all of
the available data, which of the following lines of code should the data engineer use to fill in
the blank? - A. trigger(once=True)

27-A data engineer is designing a data pipeline. The source system generates files in a shared
directory that is also used by other processes. As a result, the files should be kept as is and
will accumulate in the directory. The data engineer needs to identify which files are new
since the previous run in the pipeline, and set up the pipeline to only ingest those new files
with each run.
Which of the following tools can the data engineer use to solve this problem? - E. Auto Loader

28-A data engineering team is in the process of converting their existing data pipeline to utilize
Auto Loader for incremental processing in the ingestion of JSON files. One data engineer
comes across the following code block in the Auto Loader documentation:
(streaming_df = spark.readStream.format("cloudFiles")
.option("cloudFiles.format", "json")
.option("cloudFiles.schemaLocation", schemaLocation)
.load(sourcePath))
Assuming that schemaLocation and sourcePath have been set correctly, which of the
following changes does the data engineer need to make to convert this code block to use
Auto Loader to ingest the data? - C. There is no change required. The inclusion of format("cloudFiles") enables the use of
Auto Loader.

29-Which of the following data workloads will utilize a Bronze table as its source? - E. A job that enriches data by parsing its timestamps into a human-readable format

30-Which of the following data workloads will utilize a Silver table as its source? - D. A job that aggregates cleaned data to create standard summary statistics

31-Which of the following Structured Streaming queries is performing a hop from a Bronze table
to a Silver table? - C. (spark.table("sales")
.withColumn("avgPrice", col("sales") / col("units"))
.writeStream
.option("checkpointLocation", checkpointPath)
.outputMode("append")
.table("cleanedSales")
)


32-Which of the following benefits does Delta Live Tables provide for ELT pipelines over
standard data pipelines that utilize Spark and Delta Lake on Databricks? - A. The ability to declare and maintain data table dependencies

33-A data engineer has three notebooks in an ELT pipeline. The notebooks need to be executed
in a specific order for the pipeline to complete successfully. The data engineer would like to
use Delta Live Tables to manage this process.
Which of the following steps must the data engineer take as part of implementing this
pipeline using Delta Live Tables? - B. They need to create a Delta Live Tables pipeline from the Jobs page.

34-A data engineer has written the following query:
SELECT *
FROM json.`/path/to/json/file.json`;
The data engineer asks a colleague for help to convert this query for use in a Delta Live
Tables (DLT) pipeline. The query should create the first table in the DLT pipeline.
Which of the following describes the change the colleague needs to make to the query? - B. They need to add a CREATE LIVE TABLE table_name AS line at the beginning of the query.

35-A dataset has been defined using Delta Live Tables and includes an expectations clause:
CONSTRAINT valid_timestamp EXPECT (timestamp > '2020-01-01')
What is the expected behavior when a batch of data containing data that violates these
constraints is processed? - A. Records that violate the expectation are added to the target dataset and recorded as invalid in the event log.

36-A Delta Live Table pipeline includes two datasets defined using STREAMING LIVE TABLE.
Three datasets are defined against Delta Lake table sources using LIVE TABLE.
The table is configured to run in Development mode using the Triggered Pipeline Mode.
Assuming previously unprocessed data exists and all definitions are valid, what is the
expected outcome after clicking Start to update the pipeline? - D. All datasets will be updated once and the pipeline will shut down. The compute resources will persist to allow for additional testing

37-A data engineer has a Job with multiple tasks that runs nightly. One of the tasks
unexpectedly fails during 10 percent of the runs.
Which of the following actions can the data engineer perform to ensure the Job completes
each night while minimizing compute costs? - D. They can institute a retry policy for the task that periodically fails

38-A data engineer has set up two Jobs that each run nightly. The first Job starts at 12:00 AM,
and it usually completes in about 20 minutes. The second Job depends on the first Job, and
it starts at 12:30 AM. Sometimes, the second Job fails when the first Job does not complete
by 12:30 AM.
Which of the following approaches can the data engineer use to avoid this problem? - A. They can utilize multiple tasks in a single job with a linear dependency

39-A data engineer has set up a notebook to automatically process using a Job. The data
engineer’s manager wants to version control the schedule due to its complexity.
Which of the following approaches can the data engineer use to obtain a
version-controllable configuration of the Job’s schedule? - C. They can download the JSON description of the Job from the Job’s page.

40-A data analyst has noticed that their Databricks SQL queries are running too slowly. They
claim that this issue is affecting all of their sequentially run queries. They ask the data
engineering team for help. The data engineering team notices that each of the queries uses
the same SQL endpoint, but the SQL endpoint is not used by any other user.
Which of the following approaches can the data engineering team use to improve the
latency of the data analyst’s queries? - C. They can increase the cluster size of the SQL endpoint.

41-An engineering manager uses a Databricks SQL query to monitor their team’s progress on
fixes related to customer-reported bugs. The manager checks the results of the query every
day, but they are manually rerunning the query each day and waiting for the results.
Which of the following approaches can the manager use to ensure the results of the query
are updated each day? - B. They can schedule the query to refresh every 1 day from the query’s page in
Databricks SQL.

42-A data engineering team has been using a Databricks SQL query to monitor the
performance of an ELT job. The ELT job is triggered by a specific number of input records
being ready to process. The Databricks SQL query returns the number of minutes since the
job’s most recent runtime.
Which of the following approaches can enable the data engineering team to be notified if
the ELT job has not been run in an hour? - D. They can set up an Alert for the query to notify them if the returned value is greater than 60.

43-A data engineering manager has noticed that each of the queries in a Databricks SQL
dashboard takes a few minutes to update when they manually click the “Refresh” button.
They are curious why this might be occurring, so a team member provides a variety of
reasons on why the delay might be occurring.
Which of the following reasons fails to explain why the dashboard might be taking a few
minutes to update? - D. The Job associated with updating the dashboard might be using a non-pooled
endpoint.

44-A new data engineer has started at a company. The data engineer has recently been added
to the company’s Databricks workspace as new.engineer@company.com. The data
engineer needs to be able to query the table sales in the database retail. The new data
engineer already has been granted USAGE on the database retail.
Which of the following commands can be used to grant the appropriate permissions to the
new data engineer? - C. GRANT SELECT ON TABLE sales TO new.engineer@company.com;

45-A new data engineer new.engineer@company.com has been assigned to an ELT project.
The new data engineer will need full privileges on the table sales to fully manage the
project.
Which of the following commands can be used to grant full permissions on the table to the
new data engineer? - A. GRANT ALL PRIVILEGES ON TABLE sales TO new.engineer@company.com;
