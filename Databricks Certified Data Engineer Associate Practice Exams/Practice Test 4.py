# Databricks notebook source
1) Which of the statements is correct when choosing between lakehouse and Datawarehouse?
- Lakehouse replaces the current dependency on data lakes and data warehouses uses an open standard storage format and supports low latency Bi workloads

2) Where are Interactive notebook results stored in Databricks product architecture?
- Data and Control plane

3) Which of the following statements are true about a lakehouse?
- Lakehouse supports Transactions

4) Which of the following SQL command can be used to insert or update or delete rows based on a condition to check if a row(s) exists?
- MERGE INTO table_name

5) When investigating a data issue you realized that a process accidentally updated the table,  you want to query the same table with yesterday's version of the data so you can review what the prior version looks like, what is the best way to query historical data so you can do your analysis?
- SELECT * FROM table_name TIMESTAMP as of data_sub(current_date(), 1)

6) While investigating a data issue, you wanted to review yesterday's version of the table using below command, while querying the previous version of the table using time travel you realized that you are no longer able to view the historical data in the table and you could see it the table was updated yesterday based on the table history(DESCRIBE HISTORY table_name) command what could be the reason why you can not access this data?
- A command VACUUM table_name RETAIN 0 was ran on the table

7) You have accidentally deleted records from a table called transactions, what is the easiest way to restore the records deleted or the previous state of the table? Prior to deleting the version of the table is 3 and after delete the version of the table is 4.
- RESTORE TABLE transactions TO VERSION as of 3

8) Create a schema called bronze using location ‘/mnt/delta/bronze’, and check if the schema exists before creating.
- CREATE SCHEMA IF NOT EXISTS bronze
  LOCATION 'mnt/delta/bronze';

9)  How do you check the location of an existing schema in Delta Lake?
- Run SQL command DESCRIBE SCHEMA EXTENDED schema_name

10) Which of the below SQL commands create a Global temporary view?
- CREATE OR REPLACE LOCAL TEMPORARY VIEW view_name
  AS SELECT * FROM table_name

11) When you drop a managed table using SQL syntax DROP TABLE table_name how does it impact metadata, history, and data stored in the table?
- Drops table from meta store, drops metadata, history, and data in storage

12) The team has decided to take advantage of table properties to identify a business owner for each table, which of the following table DDL syntax allows you to populate a table property identifying the business owner of a table
- CREATE TABLE inventory (id INT, units FLOAT) TBLPROPERTIES (business_owner = 'supply chain')

13) Data science team has requested they are missing a column in the table called average price, this can be calculated using units sold and sales amt, which of the following SQL statements allow you to reload the data with additional column
- CREATE OR REPLACE TABLE sales
  AS SELECT *, salesAmt/unitsSold as avgPrice FROM sales

14) You are working on a process to load external CSV files into a delta table by leveraging the COPY INTO command, but after running the command for the second time no data was loaded into the table name, why is that?
- COPY INTO did ot detect new files after the last load

15) What is the main difference between the below two commands?

INSERT OVERWRITE table_name
SELECT * FROM table
CREATE OR REPLACE TABLE table_name
AS SELECT * FROM table

- INSERT OVERWRITE replaces data by default, CRAS replaces data and Schema by default

16) Which of the following functions can be used to convert JSON string to Struct data type?
- FROM_JSON (json value, schema of json)

17) You are working on a marketing team request to identify customers with the same information between two tables CUSTOMERS_2021 and CUSTOMERS_2020 each table contains 25 columns with the same schema, You are looking to identify rows that match between two tables across all columns, which of the following can be used to perform in SQL
- SELECT * FROM CUSTOMERS_2021
  INTERSECT
  SELECT * FROM CUSTOMERS_2020

 18) You are looking to process the data based on two variables, one to check if the department is supply chain and second to check if process flag is set to True
 - if department == 'supply chain' and process:

19) You were asked to create a notebook that can take department as a parameter and process the data accordingly, which is the following statements result in storing the notebook parameter into a python variable
- department = dbutils.widget.get("department")

20) Which of the following statements can successfully read the notebook widget and pass the python variable to a SQL statement in a Python notebook cell?
- order_date = dbutils.widgets.get("widget_order_date")
  spark.sql(f"SELECT * FROM sales WHERE orderDate = '{order_date}'")

21) The below spark command is looking to create a summary table based customerId and the number of times the customerId is present in the event_log delta table and write a one-time micro-batch to a summary table, fill in the blanks to complete the query.
- readStream, writeStream, once=True

22) You would like to build a spark streaming process to read from a Kafka queue and write to a Delta table every 15 minutes, what is the correct trigger option
- trigger(processingTime = "15 Minutes")

23) Which of the following scenarios is the best fit for the AUTO LOADER solution?
- Efficiently process new data incrementally from cloud object storage

24) You had AUTO LOADER to process millions of files a day and noticed slowness in load process, so you scaled up the Databricks cluster but realized the performance of the Auto loader is still not improving, what is the best way to resolve this.
- Increase the maxFilesPerTrigger option to a sufficiently high number

25) The current ELT pipeline is receiving data from the operations team once a day so you had setup an AUTO LOADER process to run once a day using trigger (Once = True) and scheduled a job to run once a day, operations team recently rolled out a new feature that allows them to send data every 1 min, what changes do you need to make to AUTO LOADER to process the data every 1 min.
- ChangeAUTO LOADER trigger to .trigger(ProcessingTime="1 minute")

26) What is the purpose of the bronze layer in a Multi-hop Medallion architecture?
- Copy of raw data, easy to query and ingest data for downstream processes

27) What is the purpose of the silver layer in a Multi hop architecture?
- Eliminates duplicate data, quarantines bad data

28) What is the purpose of gold layer in Multi hop architecture?
- Optimized query performance for business-critical data

29) The Delta Live Tables Pipeline is configured to run in Development mode using the Triggered Pipeline Mode. what is the expected outcome after clicking Start to update the pipeline?
- All datasets will be updated once and the pipeline will shut down. The compute resources will persist to allow for additional testing

30) The Delta Live Table Pipeline is configured to run in Production mode using the continuous Pipeline Mode. what is the expected outcome after clicking Start to update the pipeline?
- All datasets will be updated continuously and the pipeline will not shut down. The compute resources will persist with the pipeline

31) You are working to set up two notebooks to run on a schedule, the second notebook is dependent on the first notebook but both notebooks need different types of compute to run in an optimal fashion, what is the best way to set up these notebooks as jobs?
- Each task can use different cluster, add these two notebooks as two tasks in a single job with linear dependency and modify the cluster as needed for each of the tasks

32) You are tasked to set up a set notebook as a job for six departments and each department can run the task parallelly, the notebook takes an input parameter dept number to process the data by department, how do you go about to setup this up in job?
- A task accepts key-value pair parameters, creates six tasks pass department number as parameter foreach task with no dependency in the job as they can all run in parallel

33) You are asked to setup two tasks in a databricks job, the first task runs a notebook to download the data from a remote system, and the second task is a DLT pipeline that can process this data, how do you plan to configure this in Jobs UI
- The answer is Single job can be used to set up both notebook and DLT pipeline, use two different tasks with linear dependency

34) You are asked to set up an alert to notify in an email every time a KPI indicater increases beyond a threshold value, team also asked you to include the actual value in the alert email notification.
- Setup an alert but use the custom template to notify the message in email's subject

35) Operations team is using a centralized data quality monitoring system, a user can publish data quality metrics through a webhook, you were asked to develop a process to send messages using a webhook if there is atleast one duplicate record, which of the following approaches can be taken to integrate an alert with current data quality monitoring system
- Setup an alert with custom Webhook destination

36) You are currently working with the application team to setup a SQL Endpoint point, once the team started consuming the SQL Endpoint you noticed that during peak hours as the number of concurrent users increases you are seeing degradation in the query performance and the same queries are taking longer to run, which of the following steps can be taken to resolve the issue?
- They can increase the maximum bound of the SQL endpoint’s scaling range

37) The data engineering team is using a bunch of SQL queries to review data quality and monitor the ETL job every day, which of the following approaches can be used to set up a schedule and automate this process?
- They can schedule the query to refresh every 1 day from the query's page in Databricks SQL

38) In order to use Unity catalog features, which of the following steps needs to be taken on managed/external tables in the Databricks workspace?
- Migrate/upgrade objects in workspace managed/external tables/view to unity catalog

39) What is the top-level object in unity catalog?
- Metastore

40) One of the team members Steve who has the ability to create views, created a new view called regional_sales_vw on the existing table called sales which is owned by John, and the second team member Kevin who works with regional sales managers wanted to query the data in regional_sales_vw, so Steve granted the permission to Kevin using command
GRANT VIEW, USAGE ON regional_sales_vw to kevin@company.com but Kevin is still unable to access the view?
- Steve is not the owner of the sales table

41) Kevin is the owner of the schema sales, Steve wanted to create new table in sales schema called regional_sales so Kevin grants the create table permissions to Steve. Steve creates the new table called regional_sales in sales schema, who is the owner of the table regional_sales
- Stebe is the owner of the table

42) You were asked to setup a new all-purpose cluster, but the cluster is unable to start which of the following steps do you need to take to identify the root cause of the issue and the reason why the cluster was unable to start?
- Check the cluster event logs

43) Which of the following developer operations in CI/CD flow can be implemented in Databricks Repos?
- Commit and push code

44) You noticed that a team member started using an all-purpose cluster to develop a notebook and used the same all-purpose cluster to set up a job that can run every 30 mins so they can update underlying tables which are used in a dashboard. What would you recommend for reducing the overall cost of this approach?
- Change the clister all-purpose to job cluster when scheduling the job

45) Which of the following commands can be used to run one notebook from another notebook?
- dbutils.notebook.run('full notebook path')

