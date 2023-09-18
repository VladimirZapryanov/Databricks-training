# Databricks notebook source
1) A data organization leader is upset about the data analysis team’s reports being different from the data engineering team’s reports. The leader believes the siloed nature of their organization’s data engineering and data analysis architectures is to blame.
Which of the following describes how a data lakehouse could alleviate this issue?
- Both teams would use the same source of truth for their work.

2) Which of the following describes a scenario in which a data team will want to utilize cluster pools?
- An automated report needs to be refreshed as quickly as possible.

3) Which of the following is hosted completely in the control plane of the classic Databricks architecture?
- Databricks web application

4) Which of the following benefits of using the Databricks Lakehouse Platform is provided by Delta Lake?
- The ability to support batch and streaming workloads

5) Which of the following describes the storage organization of a Delta table?
- Delta tables are stored in a collection of files that contain data, history, metadata, and other attributes.

6) Which of the following code blocks will remove the rows where the value in column age is greater than 25 from the existing Delta table my_table and save the updated table?
- DELETE FROM my_table WHERE age > 25;

7) A data engineer has realized that they made a mistake when making a daily update to a table. They need to use Delta time travel to restore the table to a version that is 3 days old. However, when the data engineer attempts to time travel to the older version, they are unable to restore the data because the data files have been deleted.
Which of the following explains why the data files are no longer present?
- The VACUUM command was run on the table

8) Which of the following Git operations must be performed outside of Databricks Repos?
- Merge

9) Which of the following data lakehouse features results in improved data quality over a traditional data lake?
- A data lakehouse supports ACID-compliant transactions.

10) A data engineer needs to determine whether to use the built-in Databricks Notebooks versioning or version their project using Databricks Repos.
Which of the following is an advantage of using Databricks Repos over the Databricks Notebooks versioning?
- Databricks Repos supports the use of multiple branches

11) A data engineer has left the organization. The data team needs to transfer ownership of the data engineer’s Delta tables to a new data engineer. The new data engineer is the lead engineer on the data team.
Assuming the original data engineer no longer has access, which of the following individuals must be the one to transfer ownership of the Delta tables in Data Explorer?
- Workspace administrator

12) A data analyst has created a Delta table sales that is used by the entire data analysis team. They want help from the data engineering team to implement a series of tests to ensure the data is clean. However, the data engineering team uses Python for its tests rather than SQL.
Which of the following commands could the data engineering team use to access sales in PySpark?
- spark.table("sales")

13) Which of the following commands will return the location of database customer360?
- DESCRIBE DATABASE customer360;

14) A data engineer wants to create a new table containing the names of customers that live in France.
They have written the following command:
image1
A senior data engineer mentions that it is organization policy to include a table property indicating that the new table includes personally identifiable information (PII).
Which of the following lines of code fills in the above blank to successfully complete the task?
- COMMENT "Contains PII"

15) Which of the following benefits is provided by the array functions from Spark SQL?
- An ability to work with complex, nested data ingested from JSON files

16) Which of the following commands can be used to write data into a Delta table while avoiding the writing of duplicate records?
- MERGE

17) A data engineer needs to apply custom logic to string column city in table stores for a specific use case. In order to apply this custom logic at scale, the data engineer wants to create a SQL user-defined function (UDF).
Which of the following code blocks creates this SQL UDF?
- CREATE FUNCTION combine_nyc(city STRING)
  RETURN STRING
  RETURN CASE
    WHEN city = 'brooklyn' THEN 'new york'
    ELSE city
  END;

18) A data analyst has a series of queries in a SQL program. The data analyst wants this program to run every day. They only want the final query in the program to run on Sundays. They ask for help from the data engineering team to complete this task.
Which of the following approaches could be used by the data engineering team to complete this task?
- They could wrap the queries using PySpark and use Python’s control flow system to determine when to run the final query.

19) A data engineer runs a statement every day to copy the previous day’s sales into the table transactions. Each day’s sales are in their own file in the location "/transactions/raw".
Today, the data engineer runs the following command to complete this task:
image7
After running the command today, the data engineer notices that the number of records in table transactions has not changed.
Which of the following describes why the statement might not have copied any new records into the table?
- The previous day’s file has already been copied into the table

20) A data engineer needs to create a table in Databricks using data from their organization’s existing SQLite database.
They run the following command:
image8
Which of the following lines of code fills in the above blank to successfully complete the task?
- org.apache.spark.sql.jdbc
