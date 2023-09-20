# Databricks notebook source

1) Which of the following commands can a data engineer use to compact small data files of a Delta table into larger ones ?
- OPTIMIZE

2) A data engineer is trying to use Delta time travel to rollback a table to a previous version, but the data engineer received an error that the data files are no longer present.
Which of the following commands was run on the table that caused deleting the data files?
 - VACUUM

3) In Delta Lake tables, which of the following is the primary format for the data files?
- Parquet

4) Which of the following locations hosts the Databricks web application ?
- Control plane

5) In Databricks Repos, which of the following operations a data engineer can use to update the local version of a repo from its remote Git repository ?
- Pull

6) According to the Databricks Lakehouse architecture, which of the following is located in the customer's cloud account?
- Cluster virtual machines

7) Which of the following best describes Databricks Lakehouse?
- Single, flexible, high-performance system that supports data, analytics, and machine learning workloads.

8) If ​​the default notebook language is SQL, which of the following options a data engineer can use to run a Python code in this SQL Notebook ?
- They can add %python at the start of a cell

9) Which of the following tasks is not supported by Databricks Repos, and must be performed in your Git provider ?
- Delete branches

10) Which of the following statements is Not true about Delta Lake ?
- Delta Lake builds upon standard data format: Parquet + XML

11) How long is the default retention period of the VACUUM command ?
- 7 days

12)The data engineering team has a Delta table called employees that contains the employees personal information including their gross salaries.
Which of the following code blocks will keep in the table only the employees having a salary greater than 3000 ?
- DELETE FROM employees WHERE salary <= 3000;

13) A data engineer wants to create a relational object by pulling data from two tables. The relational object must be used by other data engineers in other sessions on the same cluster only. In order to save on storage costs, the date engineer wants to avoid copying and storing physical data.
- Global Temporary view

14) A data engineer has developed a code block to completely reprocess data based on the following if-condition in Python:

if process_mode = "init" and not is_table_exist:
   print("Start processing ...")

This if-condition is returning an invalid syntax error.
Which of the following changes should be made to the code block to fix this error ?
-if process_mode == "init" and not is_table_exist:
   print("Start processing ...")

15) Fill in the below blank to successfully create a table in Databricks using data from an existing PostgreSQL database:

CREATE TABLE employees
  USING ____________
  OPTIONS (
    url "jdbc:postgresql:dbserver",
    dbtable "employees"
  )

- org.apache.spark.sql.jdbc

16) Which of the following commands can a data engineer use to create a new table along with a comment ?
- CREATE TABLE table_name
  COMMENT "here is a comment"
  AS query

17) A junior data engineer usually uses INSERT INTO command to write data into a Delta table. A senior data engineer suggested using another command that avoids writing of duplicate records.
Which of the following commands is the one suggested by the senior data engineer ?
- MERGE INTO

18) A data engineer is designing a Delta Live Tables pipeline. The source system generates files containing changes captured in the source data. Each change event has metadata indicating whether the specified record was inserted, updated, or deleted. In addition to a timestamp column indicating the order in which the changes happened. The data engineer needs to update a target table based on these change events.
Which of the following commands can the data engineer use to best solve this problem?
- APPLY CHANGES INTO

19) In PySpark, which of the following commands can you use to query the Delta table employees created in Spark SQL?
- spark.table("employees")

20) Which of the following code blocks can a data engineer use to create a user defined function (UDF) ?
- CREATE FUNCTION plus_one(value INTEGER)
  RETURNS INTEGER
  RETURN value + 1;

21) When dropping a Delta table, which of the following explains why only the table's metadata will be deleted, while the data files will be kept in the storage ?
- The table is external

22) Given the two tables students_course_1 and students_course_2. Which of the following commands can a data engineer use to get all the students from the above two tables without duplicate records ?
- SELECT * FROM student_course_1
  UNION
  SELECT * FROM student_course_2

23) Given the following command:
    
    CREATE DATABASE IF NOT EXISTS hr_db ;

In which of the following locations will the hr_db database be located?
- dbfs:/user/hive/warehouse

24) Fill in the below blank to get the students enrolled in less than 3 courses from array column students

SELECT
  faculty_id,
  students,
  ___________ AS few_courses_students
FROM faculties

- FILTER(student, i -> i.total_courses < 3)

25) Given the following Structured Streaming query:

(spark.table("orders")
        .withColumn("total_after_tax", col("total")+col("tax"))
    .writeStream
        .option("checkpointLocation", checkpointPath)
        .outputMode("append")
         .______________ 
        .table("new_orders")
)

Fill in the blank to make the query executes a micro-batch to process data every 2 minutes
- trigger(processingTime="2minutes")

26) Which of the following is used by Auto Loader to load data incrementally?
- Spark Structured Streaming

27) Which of the following statements best describes Auto Loader ?
- Auto loader monitors a source location, in which files accumulate, to identify and ingest only new arriving files with each command run. While the files that have already been ingested in previous runs are skipped

28) A data engineer has defined the following data quality constraint in a Delta Live Tables pipeline:

CONSTRAINT valid_id EXPECT (id IS NOT NULL) _____________

Fill in the above blank so records violating this constraint will be added to the target table, and reported in metrics

- There is no need to add ON VIOLATION clause. By default, records violating the constraint will be kept, and reported as invalid in the event log

29) The data engineer team has a DLT pipeline that updates all the tables once and then stops. The compute resources of the pipeline continue running to allow for quick testing.
Which of the following best describes the execution modes of this DLT pipeline ?
- The DLT pipeline executes in Triggered Pipeline mode under Development mode

30) Which of the following will utilize Gold tables as their source?
- Dashboards

31) Which of the following code blocks can a data engineer use to query the existing streaming table events ?
- spark.readStream.table("events")

32) In multi-hop architecture, which of the following statements best describes the Bronze layer ?
- It maintains raw data ingested from various sources

33) Given the following Structured Streaming query

(spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(ordersLocation)
     .writeStream
        .option("checkpointLocation", checkpointPath)
        .table("uncleanedOrders")
)

Which of the following best describe the purpose of this query in a multi-hop architecture?
- The query is performing raw data ingestion into a Bronze table

34) A data engineer has the following query in a Delta Live Tables pipeline:

CREATE LIVE TABLE aggregated_sales
AS
  SELECT store_id, sum(total)
  FROM cleaned_sales
  GROUP BY store_id

The pipeline is failing to start due to an error in this query
Which of the following changes should be made to this query to successfully start the DLT pipeline ?

- CREATE LIVE TABLE aggregated_sales
  AS
  SELECT store_id, sum(total)
  FROM LIVE.cleaned_sales
  GROUP BY store_id

35) A data engineer has defined the following data quality constraint in a Delta Live Tables pipeline:

CONSTRAINT valid_id EXPECT (id IS NOT NULL) _____________

Fill in the above blank so records violating this constraint will be dropped, and reported in metrics
- ON VIOLATION DROP ROW

36) Which of the following compute resources is available in Databricks SQL ?
- SQL warehouses

37) Which of the following is the benefit of using the Auto Stop feature of Databricks SQL warehouses ?
- Minimizes the total running time of the warehouse

38) Which of the following alert destinations is Not supported in Databricks SQL ?
- SMS

39) A data engineering team has a long-running multi-tasks Job. The team members need to be notified when the run of this job completes.
Which of the following approaches can be used to send emails to the team members when the job completes ?
- They can configure email notifications settings in the job page

40) A data engineer wants to increase the cluster size of an existing Databricks SQL warehouse.
Which of the following is the benefit of increasing the cluster size of Databricks SQL warehouses ?
- Improves the latency of the queries execution

41) Which of the following describes Cron syntax in Databricks Jobs ?
- It's an expression to represent complex job schedule that can be defined programmatically

42) The data engineer team has a DLT pipeline that updates all the tables at defined intervals until manually stopped. The compute resources terminate when the pipeline is stopped.
Which of the following best describes the execution modes of this DLT pipeline ?
- The DLT pipeline executes in Continuous Pipeline mode under Production mode

43) Which part of the Databricks Platform can a data engineer use to grant permissions on tables to users ?
- Data Explorer

44) Which of the following commands can a data engineer use to grant full permissions to the HR team on the table employees ?
- GRAND ALL PRIVILEGES ON TABLE employees TO hr_team

45) A data engineer uses the following SQL query:

GRANT MODIFY ON TABLE employees TO hr_team

Which of the following describes the ability given by the MODIFY privilege ?
- All the above abilities are given by the MODIFY privilege
(The MODIFY privilege gives the ability to add, delete, and modify data to or from an object)
