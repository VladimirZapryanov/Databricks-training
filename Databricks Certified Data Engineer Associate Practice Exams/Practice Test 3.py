# Databricks notebook source
1) Which of the statements are incorrect when choosing between lakehouse and Datawarehouse?
- Lakehouse cannot serve low query latency with high reliability for BI workloads, only suitable for batch workloads

2) Which of the statements are correct about lakehouse?
- Lakehouse supports schema enforcement and evolution

3) Which of the following are stored in the control pane of Databricks Architecture?
- Databricks Web Application

4) You have written a notebook to generate a summary data set for reporting, Notebook was scheduled using the job cluster, but you realized it takes 8 minutes to start the cluster, what feature can be used to start the cluster in a timely fashion so your job can run immediatley?
- Use the Databricks cluster pool feature to reduce the startup time

5) Which of the following developer operations in the CI/CD can only be implemented through a GIT provider when using Databricks Repos.
- Pull request and review process

6) You have noticed the Data scientist team is using the notebook versioning feature with git integration, you have recommended them to switch to using Databricks Repos, which of the below reasons could be the reason the why the team needs to switch to Databricks Repos.
- Databricks Repos allow you to add comments and select the changes you want to commit

7) Data science team members are using a single cluster to perform data analysis, although cluster size was chosen to handle multiple users and auto-scaling was enabled, the team realized queries are still running slow, what would be the suggested fix for this?
- Use high concurrency mode instead of the standard mode

8) Which of the following SQL commands are used to append rows to an existing delta table?
- INSERT INTO table_name

9) How are Delt tables stored?
- A Directory where parquet data files are stored , a sub directory _delta_log where meta data , and the transaction log is stored as JSON files

10) While investigating a data issue in a Delta table, you wanted to review logs to see when and who updated the table, what is the best way to review this data?
- Run SQL command DESCRIBE HISTORY table_name

11) While investigating a performance issue, you realized that you have too many small files for a given table, which command are you going to run to fix this issue
- OPTIMIZE table_name

12) Create a sales database using the DBFS location : 'dbfs:/mnt/delta/databases/sales.db/'
- CREATE DATABASE sales 
  LOCATION 'dbfs:/mnt/delta/databases/sales.db/';

13) What is the type of table created when you issue SQL DDL command CREATE TABLE sales (id int, units int)
- Managed Delta table

14) How to determine if a table is a managed table vs external table? 
- Run SQL command DESCRIBE EXTENDED table_name and check type

15) Which of the below SQL commands creates a session scoped temporary view?
CREATE OR REPLACE TEMPORARY VIEW view_name
AS SELECT * FROM table_name

16) Drop the customers database and associated tables and data, all of the tables inside the database are managed tables. Which of the following SQL commands will help you accomplish this?
- DROP DATABASE customers CASCADE

17) Define an external SQL table by connecting to a local instance of an SQLite database using JDBC
- CREATE TABLE users_jdbc
  USING org.apache.spark.sql.jdbc
  OPTIONS (
    url = "jdbc:sqlite:/sqmple_db",
    dbtable = "users"
  )

18) When defining external tables using formats CSV, JSON, TEXT, BINARY any query on the external tables caches the data and location for performance reasons, so within a given spark session any new files that may have arrived will not be available after the initial query. How can we address this limitation?
- REFRESH TABLE table_name

19) Which of the following table constraints that can be enforced on Delta lake tables are supported?
- Not Null, Check Constraints

20) The data engineering team is looking to add a new column to the table, but the QA team would like to test the change before implementing in production, which of the below options allow you to quickly copy the table from Prod to the QA environment, modify and run the tests?
- SHALLOW CLONE

21) Sales team is looking to get a report on a measure number of units sold by date, below is the schema. Fill in the blank with the appropriate array function.

Table orders: orderDate DATE, orderIds ARRAY<INT>
Table orderDetail: orderId INT, unitsSold INT, salesAmt DOUBLE

SELECT orderDate, SUM(unitsSold)
      FROM orderDetail od
JOIN (select orderDate, ___________(orderIds) as orderId FROM orders) o
    ON o.orderId = od.orderId
GROUP BY orderDate

- EXPLODE

22) You are asked to write a python function that can read data from a delta table and return the DataFrame, which of the following is correct?
- Python function can return a DataFrame

23) What is the output of the below function when executed with input parameters 1, 3  :
def check_input(x,y):
    if x < y:
        x= x+1
        if x<y:
            x= x+1
            if x <y:
                x = x+1
     return x

check_input(1,3)

- 3

24) Which of the following SQL statements can replace a python variable, when the notebook is set in SQL mode
table_name = "sales"
schema_name = "bronze"
- spark.sql(f"SELECT * FROM {schema_name}.{table_name}")

25) When writing streaming data, Spark’s structured stream supports the below write modes
- Append, Complete, Update

26) When using the complete mode to write stream data, how does it impact the target table?
- Target table is overwritten for each batch

27) At the end of the inventory process a file gets uploaded to the cloud object storage, you are asked to build a process to ingest data which of the following method can be used to ingest the data incrementally, the schema of the file is expected to change overtime ingestion process should be able to handle these changes automatically. Below is the auto loader command to load the data, fill in the blanks for successful execution of the below code.

spark.readStream
.format("cloudfiles")
.option("cloudfiles.format",”csv)
.option("_______", ‘dbfs:/location/checkpoint/’)
.load(data_source)
.writeStream
.option("_______",’ dbfs:/location/checkpoint/’)
.option("mergeSchema", "true")
.table(table_name))

- cloudfiles.schemalocation, checkpointlocation

28) When working with AUTO LOADER you noticed that most of the columns that were inferred as part of loading are string data types including columns that were supposed to be integers, how can we fix this?
- Provide schema hints

29) You have configured AUTO LOADER to process incoming IOT data from cloud object storage every 15 mins, recently a change was made to the notebook code to update the processing logic but the team later realized that the notebook was failing for the last 24 hours, what steps team needs to take to reprocess the data that was not loaded after the notebook was corrected?
- Autoloader automatically re-processes data that was not loaded

30) Which of the following Structured Streaming queries is performing a hop from a bronze table to a Silver table?
- (spark.table("sales")
  .withColumn("avgPrice", col("sales") / col("units"))
  .writeStream
  .option("checkpointLocation", checkpointPath)
  .outputMode("append") 
  .table("cleanedSales"))

31) Which of the following Structured Streaming queries successfully performs a hop from a Silver to Gold table?
- (spark.table("sales") 
  .groupBy("store") 
  .agg(sum("sales")) 
  .writeStream 
  .option("checkpointLocation", checkpointPath) 
  .outputMode("complete") 
  .table("aggregatedSales") )

32) Which of the following Auto loader structured streaming commands successfully performs a hop from the landing area into Bronze?
- spark\
  .readStream\
  .format("cloudFiles") \# use Auto loader
  .option("cloudFiles.format","csv") \ # csv format files
  .option("cloudFiles.schemaLocation", checkpoint_directory)\
  .load('landing')\
  .writeStream.option("checkpointLocation", checkpoint_directory)\
  .table(raw)

33) A DELTA LIVE TABLE pipelines can be scheduled to run in two different modes, what are these two different modes?
- Triggered, Continuous

34) Your team member is trying to set up a delta pipeline and build a second gold table to the same pipeline with aggregated metrics based on an existing Delta Live table called sales_orders_cleaned but he is facing a problem in starting the pipeline, the pipeline is failing to state it cannot find the table sales_orders_cleaned, you are asked to identify and fix the problem.

CREATE LIVE TABLE sales_order_in_chicago
AS
SELECT order_date, city, sum(price) as sales,
FROM sales_orders_cleaned
WHERE city = 'Chicago')
GROUP BY order_date, city

- Sales_orders_cleaned table is missing schema name LIVE

35) Which of the following type of tasks cannot setup through a job?
- Databricks SQL Dashboard refresh

36) Which of the following approaches can the data engineer use to obtain a version-controllable configuration of the Job’s schedule and configuration?
- They can download the JSON equivalent of the job from the Job's page

37) What steps need to be taken to set up a DELTA LIVE PIPELINE as a job using the workspace UI?
- Select Workflows UI and Delta live tables tab, under task type select Delta live tables pipeline and select the notebook.

38) Data engineering team has provided 10 queries and asked Data Analyst team to build a dashboard and refresh the data every day at 8 AM, identify the best approach to set up data refresh for this dashaboard?
- The entire dashboard with 10 queries can be refreshed at once, single schedule needs to be set up to refresh at 8 AM

39) The data engineering team is using a SQL query to review data completeness every day to monitor the ETL job, and query output is being used in multiple dashboards which of the following approaches can be used to set up a schedule and automate this process?
- They can schedule the query to refresh every day from the query's page in Databricks SQL

40) A data engineer is using a Databricks SQL query to monitor the performance of an ELT job. The ELT job is triggered by a specific number of input records being ready to process. The Databricks SQL query returns the number of minutes since the job’s most recent runtime. Which of the following approaches can enable the data engineering team to be notified if the ELT job has not been run in an hour?
- The answer is, They can set up an Alert for the query to notify them if the returned value is greater than 60.

41) Which of the following is true, when building a Databricks SQL dashboard?
- More than one visualization can be developed using a single query result

42) A newly joined team member John Smith in the Marketing team currently has access read access to sales tables but does not have access to update the table, which of the following commands help you accomplish this?
- GRANT MODIFY ON TABLE table_name TO john.smith@marketing.com

43) A new user who currently does not have access to the catalog or schema is requesting access to the customer table in sales schema, but the customer table contains sensitive information, so you have decided to create view on the table excluding columns that are sensitive and granted access to the view GRANT SELECT ON view_name to user@company.com but when the user tries to query the view, gets the error view does not exist. What is the issue preventing user to access the view and how to fix it?
-  User requires USAGE privilege on Sales schema

44) How do you access or use tables in the unity catalog?
- catalog_name.schema_name.table_name

45) How do you upgrade an existing workspace managed table to a unity catalog table?
- Create table catalog_name.schema_name.table_name
  as select * from hive_metastore.old_schema.old_table

