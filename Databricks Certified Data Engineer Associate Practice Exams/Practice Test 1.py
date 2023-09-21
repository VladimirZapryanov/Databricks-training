# Databricks notebook source
1)You were asked to create a table that can store the below data, orderTime is a timestamp but the finance team when they query this data normally prefer the orderTime in date format, you would like to create a calculated column that can convert the orderTime column timestamp datatype to date and store it, fill in the blank to complete the DDL.

CREATE TABLE orders (
    orderId int,
    orderTime timestamp,
    orderdate date _____________________________________________ ,
    units int)

- GENERATED ALWAYS AS (CAST(orderTime as Date))

2) The data engineering team noticed that one of the job fails randomly as a result of using spot instances, what feature in Jobs/Tasks can be used to address this issue so the job is more stable when using spot instances?
- Add a retry policy to the task

3) What is the main difference between AUTO LOADER  and COPY INTO?
- AUTO LOADER Supports file notification when performing incremental loads

4) Why does AUTO LOADER require schema location?
- Schema location is used to store schema inferred by AUTO LOADER

5) Which of the following statements are incorrect about the lakehouse
- Storage is coupled with Compute

6) You are designing a data model that works for both machine learning using images and Batch ETL/ELT workloads. Which of the following features of data lakehouse can help you meet the needs of both workloads?
- Data lakehouse can store unstructured data and support ACID transactions

7) Which of the following locations in Databricks product architecture hosts jobs/pipelines and queries?
- Control plane

8) You are currently working on a notebook that will populate a reporting table for downstream process consumption, this process needs to run on a schedule every hour. what type of cluster are you going to use to set up this job?
- The job cluster is best suited for this purpose

9) Which of the following developer operations in CI/CD flow can be implemented in Databricks Repos?
- Trigger Databricks Repos API to pull the latest version of code into production folder

10) You are currently working with the second team and both teams are looking to modify the same notebook, you noticed that the second member is copying the notebooks to the personal folder to edit and replace the collaboration notebook, which notebook feature do you recommend to make the process easier to collaborate.
- Databricks Notebooks support real-time coauthoring on a single notebook

11) You are currently working on a project that requires the use of SQL and Python in a given notebook, what would be your approach
- A single notebook can support multiple languages, use the magic command to switch between the two

12) Which of the following statements are correct on how Delta Lake implements a lake house?
- Delta lake uses open source, open format, optimized cloud storage and scalable meta data

13) You were asked to create or overwrite an existing delta table to store the below transaction data.
- CREATE OR REPLACE TABLE transactions (
    transactionId int,
    transactionDate timestamp,
    unitsSold int
)

14) if you run the command VACUUM transactions retain 0 hours? What is the outcome of this command?
- Command will fail, you cannot run the command with retentionDurationcheck enabled

15) You noticed a colleague is manually copying the data to the backup folder prior to running an update command, incase if the update command did not provide the expected outcome so he can use the backup copy to replace table, which Delta Lake feature would you recommend simplifying the process?
- Use time travel feature to refer old data instead of manually copying

16) Which one of the following is not a Databricks lakehouse object?
- Stored Procedures

17) What type of table is created when you create delta table with below command?
CREATE TABLE transactions USING DELTA LOCATION "DBFS:/mnt/bronze/transactions"
- External table

18) Which of the following command can be used to drop a managed delta table and the underlying files in the storage?
-  DROP TABLE table_name

19) Which of the following is the correct statement for a session scoped temporary view?
- Temporary views are lost once the notebook is detached and reattached

20) Which of the following is correct for the global temporary view?
- Global Temporary Views can be still accessed even if the notebook is detached and attached

21) You are currently working on reloading customer_sales tables using the below query

INSERT OVERWRITE customer_sales
SELECT * FROM customers c
INNER JOIN sales_monthly s on s.customer_id = c.customer_id

After you ran the above command, the Marketing team quickly wanted to review the old data that was in the table. How does INSERT OVERWRITE impact the data in the customer_sales table if you want to see the previous version of the data prior to running the above statement?
- Overwrites the data in the table but preserves all historical versions of the data, you can time travel to previous versions

22) Which of the following SQL statement can be used to query a table by eliminating duplicate rows from the query results?
- SELECT DISTINCT * FROM table_name

23) Which of the below SQL Statements can be used to create a SQL UDF to convert Celsius to Fahrenheit and vice versa, you need to pass two parameters to this function one, actual temperature, and the second that identifies if its needs to be converted to Fahrenheit or Celcius with a one-word letter F or C?

select udf_convert(60,'C')  will result in 15.5
select udf_convert(10,'F')  will result in 50

- CREATE FUNCTION udf_convert(temp DOUBLE, measure STRING)
  RETURNS DOUBLE
  RETURN CASE WHEN measure == 'F' THEN (temp * 9/5) + 32
              ELSE (temp - 33) * 5/9
         END

24) You are trying to calculate total sales made by all the employees by parsing a complex struct data type that stores employee and sales data, how would you approach this in SQL

Table definition,
batchId INT, performance ARRAY<STRUCT<employeeId: BIGINT, sales: INT>>, insertDate TIMESTAMP

Sample data of performance column

[
{ "employeeId":1234
"sales" : 10000},
 
{ "employeeId":3232
"sales" : 30000}
]

Calculate total sales made by all the employees?
Sample data with create table syntax for the data: 

create or replace table sales as 
select 1 as batchId ,
	from_json('[{ "employeeId":1234,"sales" : 10000 },{ "employeeId":3232,"sales" : 30000 }]',
         'ARRAY<STRUCT<employeeId: BIGINT, sales: INT>>') as performance,
  current_timestamp() as insertDate
union all 
select 2 as batchId ,
  from_json('[{ "employeeId":1235,"sales" : 10500 },{ "employeeId":3233,"sales" : 32000 }]',
                'ARRAY<STRUCT<employeeId: BIGINT, sales: INT>>') as performance,
                current_timestamp() as insertDate

- select aggregate(flatten(collect_list(performance.sales)), 0, (x, y) -> x + y) 
as  total_sales from sales 

25) Which of the following statements can be used to test the functionality of code to test number of rows in the table equal to 10 in python?

row_count = spark.sql("select count(*) from table").collect()[0][0]

- assert row_count == 10, "Row count did not match"

26) How do you handle failures gracefully when writing code in Pyspark,  fill in the blanks to complete the below statement
_____
    Spark.read.table("table_name").select("column").write.mode("append").SaveAsTable("new_table_name")
_____
    print(f"query failed")

- try: except:

27) You are working on a process to query the table based on batch date, and batch date is an input parameter and expected to change every time the program runs, what is the best way to we can parameterize the query to run without manually changing the batch date?
- Create a notebook parameter for batch date and assign the value to a python variable and use a spark data frame to filter the data based on the python variable

28) Which of the following commands results in the successful creation of a view on top of the delta stream(stream on delta table)?
- Spark.readStream.format("delta").table("sales").createOrReplaceTempView("streaming_vw")

29) Which of the following techniques structured streaming uses to create an end-to-end fault tolerance?
- Checkpointing and idempotent sinks

30) Which of the following two options are supported in identifying the arrival of new files, and incremental data from Cloud object storage using Auto Loader?
- Directory listing, File notification

31) Which of the following data workloads will utilize a Bronze table as its destination?
-A job that ingests raw data from a streaming source into the Lakehouse

32) Which of the following data workloads will utilize a silver table as its source?
- A job that aggregates cleaned data to create standard summary statistics

33) Which of the following data workloads will utilize a gold table as its source?
- A job that queries aggregated data that already feeds into a dashboard

34) Which of the following data workloads will utilize a gold table as its source?
- DELTA LIVE TABLES

35) When building a DLT s pipeline you have two options to create a live tables, what is the main difference between CREATE STREAMING LIVE TABLE vs CREATE LIVE TABLE?
- CREATE STREAMING LIVE TABLE is used when working with Streaming data sources and Incremental data

36) A particular job seems to be performing slower and slower over time, the team thinks this started to happen when a recent production change was implemented, you were asked to take look at the job history and see if we can identify trends and root cause, where in the workspace UI can you perform this analysis?
- Under jobs UI select the job you are interested, under runs we can see current active runs and last 60 days historical run

37) What are the different ways you can schedule a job in Databricks workspace?
- Cron, On Demand runs

38) You have noticed that Databricks SQL queries are running slow, you are asked to look reason why queries are running slow and identify steps to improve the performance, when you looked at the issue you noticed all the queries are running in parallel and using a SQL endpoint(SQL Warehouse) with a single cluster. Which of the following steps can be taken to improve the performance/response times of the queries?
*Please note Databricks recently renamed SQL endpoint to SQL warehouse
- They can increase the maximum bound of the SQL endpoint(SQL warehouse)'s scaling range

39) You currently working with the marketing team to setup a dashboard for ad campaign analysis, since the team is not sure how often the dashboard should be refreshed they have decided to do a manual refresh on an as needed basis. Which of the following steps can be taken to reduce the overall cost of the compute when the team is not using the compute?
*Please note that Databricks recently change the name of SQL Endpoint to SQL Warehouses.
- They can turn on the Auto Stop feature for the SQL endpoint(SQL Warehouse)

40) You had worked with the Data analysts team to set up a SQL Endpoint(SQL warehouse) point so they can easily query and analyze data in the gold layer, but once they started consuming the SQL Endpoint(SQL warehouse)  you noticed that during the peak hours as the number of users increase you are seeing queries taking longer to finish, which of the following steps can be taken to resolve the issue?
*Please note Databricks recently renamed SQL endpoint to SQL warehouse.
-They can increase the maximum bound of the SQL endpoint(SQL warehouse)'s scaling range

41) The research team has put together a funnel analysis query to monitor the customer traffic on the e-commerce platform, the query takes about 30 mins to run on a small SQL endpoint cluster with max scaling set to 1 cluster. What steps can be taken to improve the performance of the query?
- They can increase the cluster size anywhere from X small to 3XL to review the performance and select the size that meets the required SLA

42) Unity catalog simplifies managing multiple workspaces, by storing and managing permissions and ACL at _______ level
- Account

43) Which of the following section in the UI can be used to manage permissions and grants to tables?
- Data Explorer

44) Which of the following is not a privilege in the Unity catalog?
- DELETE

45) A team member is leaving the team and he/she is currently the owner of the few tables, instead of transfering the ownership to a user you have decided to transfer the ownership to a group so in the future anyone in the group can manage the permissions rather than a single individual, which of the following commands help you accomplish this?
- ALTER TABLE table_name OWNER to 'group'
