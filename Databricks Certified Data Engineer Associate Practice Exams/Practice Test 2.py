# Databricks notebook source
 1) What is the best way to describe a data lakehouse compared to a data warehouse?
- A data lakehouse enables both batch and streaming analytics

2) You are designing an analytical to store structured data from your e-commerce platform and unstructured data from website traffic and app store, how would you approach where you store this data?
- Data lakehouse can store structured and unstructured data and can enforce schema

3) You are currently working on a production job failure with a job set up in job clusters due to a data issue, what cluster do you need to start to investigate and analyze the data?
- All-purpose cluster/ interactive cluster is the recommended way to run commands and view the data

4) Which of the following describes how Databricks Repos can help facilitate CI/CD workflows on the Databricks Lakehouse Platform?
- Databricks Repos can commit or push code changes to trigger a CI/CD process

5) You noticed that colleague is manually copying the notebook with _bkp to store the previous versions, which of the following feature would you recommend instead.
- Databricks notebooks support change tracking and versioning

6) Newly joined data analyst requested read-only access to tables, assuming you are owner/admin which section of Databricks platform is going to facilitate granting select access to the user
- Data explorer

7) How does a Delta Lake differ from a traditional data lake?
- Delta lake is an open storage format like parquet with additional capabilities that can provide realibility, security, and performance

8) As a Data Engineer, you were asked to create a delta table to store below transaction data?
- CREATE TABLE transactions (
    transactionId int,
    transactionDate timestamp,
    unitsSold int);

9) Which of the following is a correct statement on how the data is organized in the storage when when managing a DELTA table?
- All of the data is broken down into one or many parquet files, log files are broken down into one or many JSON files, and each transaction creates a new data file(s) and log file.

10) What is the underlying technology that makes the Auto Loader work?
- Structured Streaming

11) You are currently working to ingest millions of files that get uploaded to the cloud object storage for consumption, and you are asked to build a process to ingest this data, the schema of the file is expected to change over time, and the ingestion process should be able to handle these changes automatically. Which of the following method can be used to ingest the data incrementally?
- AUTO LOADER

12) At the end of the inventory process, a file gets uploaded to the cloud object storage, you are asked to build a process to ingest data which of the following method can be used to ingest the data incrementally, schema of the file is expected to change overtime ingestion process should be able to handle these changes automatically. Below is the auto loader to command to load the data, fill in the blanks for successful execution of below code. 

spark.readStream
.format("cloudfiles")
.option("_______",”csv)
.option("_______", ‘dbfs:/location/checkpoint/’)
.load(data_source)
.writeStream
.option("_______",’ dbfs:/location/checkpoint/’)
.option("_______", "true")
.table(table_name))

- cloudfiles.format
  cloudfiles.schemalocation
  checkpointlocation
  mergeSchema

13) What is the purpose of the bronze layer in a Multi-hop architecture?
- Provides efficient storage and querying of full unprocessed history of data

14) What is the purpose of a silver layer in Multi hop architecture?
- A schema is enforced, with data quality checks

15) What is the purpose of a gold layer in Multi-hop architecture?
- Power ML applications, reporting, dashboards and adhoc reports

16) You are currently asked to work on building a data pipeline, you have noticed that you are currently working on a very large scale ETL many data dependencies, which of the following tools can be used to address this problem?
- DELTA LIVE TABLES

17) How do you create a delta live tables pipeline and deploy using DLT UI?
- Within the Workspace UI, click on Workflows, select Delta Live tables and create a pipeline and select the notebook with DLT code

18) You are noticing job cluster is taking 6 to 8 mins to start which is delaying your job to finish on time, what steps you can take to reduce the amount of time cluster startup time?
- Use cluster pools to reduce the startup time of the jobs

19) Data engineering team has a job currently setup to run a task load data into a reporting table every day at 8: 00 AM takes about 20 mins, Operations teams are planning to use that data to run a second job, so they access latest complete set of data. What is the best to way to orchestrate this job setup?
- Add Operation reporting task in the same job and set the operation reporting task to depend on Data Engineering task

20) The data engineering team noticed that one of the job normally finishes in 15 mins but gets stuck randomly when reading remote databases due to a network packet drop, which of the following steps can be used to improve the stability of the job?
- Modify the task, to include a timeout to kill the job if it runs more than 15 mins

21) Which of the following programming languages can be used to build a Databricks SQL dashboard?
- SQL

22) The data analyst team had put together queries that identify items that are out of stock based on orders and replenishment but when they run all together for final output the team noticed it takes a really long time, you were asked to look at the reason why queries are running slow and identify steps to improve the performance and when you looked at it you noticed all the code queries are running sequentially and using a SQL endpoint cluster. Which of the following steps can be taken to resolve the issue?

Here is the example query
--- Get order summary 
create or replace table orders_summary
as 
select product_id, sum(order_count) order_count
from 
 (
  select product_id,order_count from orders_instore
  union all 
  select product_id,order_count from orders_online
 )
group by product_id
-- get supply summary 
create or repalce tabe supply_summary
as 
select product_id, sum(supply_count) supply_count
from supply
group by product_id
 
-- get on hand based on orders summary and supply summary
 
with stock_cte
as (
select nvl(s.product_id,o.product_id) as product_id,
	 nvl(supply_count,0) -  nvl(order_count,0) as on_hand
from supply_summary s 
full outer join orders_summary o
        on s.product_id = o.product_id
)
select *
from 
stock_cte
where on_hand = 0 

- Increase the cluster size of the SQL endpoint

23) The operations team is interested in monitoring the recently launched product, team wants to set up an email alert when the number of units sold increases by more than 10,000 units. They want to monitor this every 5 mins.
Fill in the below blanks to finish the steps we need to take

· Create ___ query that calculates total units sold
· Setup ____ with query on trigger condition Units Sold > 10,000
· Setup ____ to run every 5 mins
· Add destination ______

- SQL, Alert, Refresh, email address

24) The marketing team is launching a new campaign to monitor the performance of the new campaign for the first two weeks, they would like to set up a dashboard with a refresh schedule to run every 5 minutes, which of the below steps can be taken to reduce of the cost of this refresh over time?
- Setup the dashboard refresh schedule to end in two weeks

25) Which of the following tool provides Data Access control, Access Audit, Data Lineage, and Data discovery?
- Unity Catalog

26) Data engineering team is required to share the data with Data science team and both the teams are using different workspaces in the same organizationwhich of the following techniques can be used to simplify sharing data across?
*Please note the question is asking how data is shared within an organization across multiple workspaces.
- Unity Catalog

27) A newly joined team member John Smith in the Marketing team who currently does not have any access to the data requires read access to customers table, which of the following statements can be used to grant access.
- GRANT SELECT, USAGE ON TABLE customers TO john.smith@marketing.com

28) Grant full privileges to new marketing user Kevin Smith to table sales.
- GRANT ALL PRIVILEGE ON TABLE sales TO kevin.smith@marketing.com

29) Which of the following locations in the Databricks product architecture hosts the notebooks and jobs?
- Control plane

30) A dataset has been defined using Delta Live Tables and includes an expectations clause: CONSTRAINT valid_timestamp EXPECT (timestamp > '2020-01-01') ON VIOLATION FAIL
What is the expected behavior when a batch of data containing data that violates these constraints is processed?
- Records that violate the expectation cause the job to fail

31) You are still noticing slowness in query after performing optimize which helped you to resolve the small files problem, the column(transactionId) you are using to filter the data has high cardinality and auto incrementing number. Which delta optimization can you enable to filter data effectively based on this column?
- Perform Optimize with Zorder on transactionId

32) If you create a database sample_db with the statement CREATE DATABASE sample_db what will be the default location of the database in DBFS?
- Default Location, dbfs:/user/hive/warehouse

33) Which of the following results in the creation of an external table?
- CREATE TABLE transactions (
    id int,
    desc string)
  LOCATION '/mnt/delta/transactions'

34) When you drop an external DELTA table using the SQL Command DROP TABLE table_name, how does it impact metadata(delta log, history), and data stored in the storage?
- Drop table from metastore, but keeps metadata(delta log, history) and data in storage

35) Which of the following is a true statement about the global temporary view?
- A global temporary view is available only on the cluster it was created, when the cluster restarts global temporary view is automatically dropped

36) You are trying to create an object by joining two tables that and it is accessible to data scientist’s team, so it does not get dropped if the cluster restarts or if the notebook is detached. What type of object are you trying to create?
- View

37) What is the best way to query external csv files located on DBFS Storage to inspect the data using SQL?
- SELECT * FROM CSV.'dbfs:/location/csv_files/'

38) Direct query on external files limited options, create external tables for CSV files with header and pipe delimited CSV files, fill in the blanks to complete the create table statement
- CREATE TABLE sales (id int, unitsSold int, price FLOAT, items STRING)
  USING CSV
  OPTIONS(header="true", delimiter="|")
  LOCATION "dbfs:/mnt/sales/*.csv"

39) What could be the expected output of query SELECT COUNT (DISTINCT *) FROM user on this table
- 2

40) You are working on a table called orders which contains data for 2021 and you have the second table called orders_archive which contains data for 2020, you need to combine the data from two tables and there could be a possibility of the same rows between both the tables, you are looking to combine the results from both the tables and eliminate the duplicate rows, which of the following SQL statements helps you accomplish this?
- SELECT * FROM orders UNION SELECT * FROM orders_archive

41) Which of the following python statement can be used to replace the schema name and table name in the query statement?
- table_name = 'sales'
  schema_name = 'bronze'
  query = f'select * from {schema_name}.{table_name}'

42) Which of the following SQL statements can replace python variables in Databricks SQL code, when the notebook is set in SQL mode?
%python 
table_name = "sales"
schema_name = "bronze"
 
%sql
SELECT * FROM ____________________

- SELECT * FROM ${schema_name}.${table_name}

43) A notebook accepts an input parameter that is assigned to a python variable called department and this is an optional parameter to the notebook, you are looking to control the flow of the code using this parameter. you have to check department variable is present then execute the code and if no department value is passed then skip the code execution. How do you achieve this using python?
- if department is not None:
    #Execute code
  else:
      pass

44) Which of the following operations are not supported on a streaming dataset view?
spark.readStream.format("delta").table("sales").createOrReplaceTempView("streaming_view")
- SELECT * FROM streadming_view order by id;

45) Which of the following techniques structured streaming uses to ensure recovery of failures during stream processing?
-  Checkpointing and write-ahead logging

