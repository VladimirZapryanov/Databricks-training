-- Databricks notebook source
-- MAGIC %fs
-- MAGIC ls dbfs:/FileStore/extracted_files/employee_details/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1) Json files
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC files = dbutils.fs.ls("dbfs:/FileStore/extracted_files/employee_details/json_files/")
-- MAGIC display(files)

-- COMMAND ----------

SELECT *
FROM json.`${dataset.employee}/FileStore/extracted_files/employee_details/json_files/employee_001.json`

-- COMMAND ----------

SELECT *
FROM json.`${dataset.employee}/FileStore/extracted_files/employee_details/json_files/employee_*.json`

-- COMMAND ----------

SELECT *
FROM json.`${dataset.employee}/FileStore/extracted_files/employee_details/json_files`

-- COMMAND ----------

SELECT count(*)
FROM json.`${dataset.employee}/FileStore/extracted_files/employee_details/json_files`

-- COMMAND ----------

SELECT *, input_file_name() source_file
FROM json.`${dataset.employee}/FileStore/extracted_files/employee_details/json_files`

-- COMMAND ----------

SELECT *
FROM text.`${dataset.employee}/FileStore/extracted_files/employee_details/json_files`

-- COMMAND ----------

SELECT *
FROM binaryFile.`${dataset.employee}/FileStore/extracted_files/employee_details/json_files`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2) csv files

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC files = dbutils.fs.ls("dbfs:/FileStore/extracted_files/employee_details/csv_files/")
-- MAGIC display(files)

-- COMMAND ----------

SELECT *
FROM csv.`${dataset.employee}/FileStore/extracted_files/employee_details/csv_files`

-- COMMAND ----------

SELECT count(*)
FROM csv.`${dataset.employee}/FileStore/extracted_files/employee_details/csv_files`

-- COMMAND ----------

SELECT *
FROM text.`${dataset.employee}/FileStore/extracted_files/employee_details/csv_files`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 3) Create a table & Difference between delta and non delta table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###a) Create table from csv files (Non Delta table)

-- COMMAND ----------

CREATE TABLE employee_csv(
  id STRING,
  first_name STRING,
  last_name STRING,
  email STRING,
  gender STRING,
  salary DOUBLE,
  team STRING)
  USING CSV
  OPTIONS (
    header = "true",
    delimeter = ","
  )
  LOCATION "${dataset.employee}/FileStore/extracted_files/employee_details/csv_files";



-- COMMAND ----------

SELECT *
FROM employee_csv;

-- COMMAND ----------

DESCRIBE EXTENDED employee_csv;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC files = dbutils.fs.ls("dbfs:/FileStore/extracted_files/employee_details/csv_files/")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC (spark.read.table("employee_csv").write.mode("append").format("csv").option('header', 'true').option('delimiter', ',').save("dbfs:/FileStore/extracted_files/employee_details/csv_files/"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC files = dbutils.fs.ls("dbfs:/FileStore/extracted_files/employee_details/csv_files/")
-- MAGIC display(files)

-- COMMAND ----------

SELECT count(*)
FROM employee_csv;

-- COMMAND ----------

REFRESH TABLE employee_csv;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###b) Create table from json files (Delta table)

-- COMMAND ----------

CREATE TABLE employee_json_2 AS 
SELECT *
FROM json.`${dataset.employee}/FileStore/extracted_files/employee_details/json_files`

-- COMMAND ----------

SELECT * 
FROM employee_json_2;

-- COMMAND ----------

DESCRIBE EXTENDED employee_json_2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###c) Create table from csv files (Delta table)

-- COMMAND ----------

CREATE TABLE employee_unparsed AS 
SELECT *
FROM csv.`${dataset.employee}/FileStore/extracted_files/employee_details/csv_files`;

SELECT *
FROM employee_unparsed;

-- COMMAND ----------

CREATE TEMP VIEW employee_tmp_vw(
  id STRING,
  first_name STRING,
  last_name STRING,
  email STRING,
  gender STRING,
  salary DOUBLE,
  team STRING)
  USING CSV
  OPTIONS (
    path = "${dataset.employee}/FileStore/extracted_files/employee_details/csv_files",
    header = "true",
    delimeter = ","
  );

  CREATE TABLE employee_csv_3 AS 
  SELECT * FROM employee_tmp_vw;

  SELECT *
  FROM employee_csv_3;
  

-- COMMAND ----------

DESCRIBE EXTENDED employee_csv_3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###d) Create table from parquet files (Delta table)

-- COMMAND ----------

CREATE TABLE employee_parquet AS 
SELECT *
FROM parquet.`${dataset.employee}/FileStore/extracted_files/employee_details/parquet_files`;

SELECT *
FROM employee_parquet;

-- COMMAND ----------

DESCRIBE EXTENDED employee_parquet;

-- COMMAND ----------

DROP TABLE employee_csv;
DROP TABLE employee_csv_3;
DROP TABLE employee_parquet;

-- COMMAND ----------

DROP TABLE employee_unparsed;
DROP TABLE employee_json_2;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------


