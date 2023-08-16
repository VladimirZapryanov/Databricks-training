-- Databricks notebook source
-- MAGIC %md
-- MAGIC #1) Importing the data
-- MAGIC

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls dbfs:/FileStore/

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls dbfs:/FileStore/extracted_files/employee_details/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #2) Create a table
-- MAGIC

-- COMMAND ----------

CREATE TABLE employee AS
SELECT * FROM PARQUET.`${dataset/employee}/FileStore/extracted_files/employee_details/parquet_files`;

-- COMMAND ----------

SELECT * 
FROM employee;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #3) Overwrite to a table (CRAS statement)

-- COMMAND ----------

CREATE OR REPLACE TABLE employee AS
SELECT * FROM PARQUET.`${dataset/employee}/FileStore/extracted_files/employee_details/parquet_files`;

-- COMMAND ----------

SELECT * 
FROM employee;

-- COMMAND ----------

DESCRIBE HISTORY employee;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #4) Overwrite to a table (INSERT OVERWRITE statement)

-- COMMAND ----------

INSERT OVERWRITE employee
SELECT * FROM PARQUET.`${dataset/employee}/FileStore/extracted_files/employee_details/parquet_files`;

-- COMMAND ----------

DESCRIBE HISTORY employee;

-- COMMAND ----------

INSERT OVERWRITE employee
SELECT *, current_timestamp() FROM PARQUET.`${dataset/employee}/FileStore/extracted_files/employee_details/parquet_files`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #5) Appending records to tables (INSERT INTO Statement)

-- COMMAND ----------

INSERT INTO employee
SELECT * FROM PARQUET.`${dataset/employee}/FileStore/extracted_files/employee_details/parquet_files_new/employee_005.parquet`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #6) Appending records to tables (MERGE INTO Statement)
-- MAGIC

-- COMMAND ----------

CREATE TABLE employee_1 AS
SELECT * FROM PARQUET.`${dataset/employee}/FileStore/extracted_files/employee_details/parquet_files`;

-- COMMAND ----------

SELECT count(*) 
FROM employee_1;

-- COMMAND ----------

CREATE TABLE employee_2 AS
SELECT * FROM PARQUET.`${dataset/employee}/FileStore/extracted_files/employee_details/parquet_files_new/employee_005.parquet`;

-- COMMAND ----------

SELECT count(*)
FROM employee_2;

-- COMMAND ----------

MERGE INTO employee_1 t
USING employee_2 s
ON t.id = s.id
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

SELECT count(*)
FROM employee_1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 7) Insert new records & update old ones

-- COMMAND ----------

CREATE TABLE employee_3 AS
SELECT * FROM PARQUET.`${dataset/employee}/FileStore/extracted_files/employee_details/parquet_files`;

-- COMMAND ----------

SELECT count(*)
FROM employee_3;

-- COMMAND ----------

CREATE TABLE employee_4 AS
SELECT * FROM PARQUET.`${dataset/employee}/FileStore/extracted_files/employee_details/parquet_files_new/employee_006.parquet`;

-- COMMAND ----------

SELECT count(*)
FROM employee_4;

-- COMMAND ----------

SELECT *
FROM employee_4;

-- COMMAND ----------

MERGE INTO employee_3  s
USING employee_4  t
ON s.id = t.id
WHEN MATCHED THEN 
  UPDATE SET s.email = t.email
WHEN NOT MATCHED THEN 
  INSERT *;

-- COMMAND ----------

SELECT count(*) 
FROM employee_3;

-- COMMAND ----------

DESCRIBE HISTORY employee_3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #8) Delete all the tables

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DROP TABLE employee_1;
DROP TABLE employee_2;
DROP TABLE employee_3;
DROP TABLE employee_4;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------


