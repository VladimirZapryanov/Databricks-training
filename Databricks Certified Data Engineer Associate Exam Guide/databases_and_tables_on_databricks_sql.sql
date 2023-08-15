-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Managed Table

-- COMMAND ----------

CREATE TABLE managed_tb(
  employee_id INT,
  name STRING,
  age INT,
  regions STRING
);

-- COMMAND ----------

INSERT INTO managed_tb
VALUES 
  (1, "Oliver", 28, "West"),
  (2, "James", 30, "Norteast"),
  (3, "Amelia", 22, "Southwest"),
  (4, "Sophia", 24, "Midwest"),
  (5, "Isabella", 26, "West");

-- COMMAND ----------

SELECT *
FROM managed_tb;

-- COMMAND ----------

DESCRIBE DETAIL managed_tb;

-- COMMAND ----------

DESCRIBE HISTORY managed_tb;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/managed_tb'

-- COMMAND ----------

DESCRIBE EXTENDED managed_tb;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # External Table

-- COMMAND ----------

CREATE TABLE unmanaged_tb(
  employee_id INT,
  name STRING,
  age INT,
  regions STRING)
LOCATION 'dbfs:/mnt/demo/unmanaged_tb';

-- COMMAND ----------

INSERT INTO unmanaged_tb
VALUES 
  (1, "Oliver", 28, "West"),
  (2, "James", 30, "Norteast"),
  (3, "Amelia", 22, "Southwest"),
  (4, "Sophia", 24, "Midwest"),
  (5, "Isabella", 26, "West");

-- COMMAND ----------

DESCRIBE EXTENDED unmanaged_tb;

-- COMMAND ----------

DROP TABLE managed_tb;

-- COMMAND ----------

SELECT *
FROM managed_tb;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/managed_tb'

-- COMMAND ----------

DROP TABLE unmanaged_tb;

-- COMMAND ----------

SELECT *
FROM unmanaged_tb;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/mnt/demo/unmanaged_tb'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Apart from default database, we can also create extra databases inside hive directory

-- COMMAND ----------

CREATE SCHEMA database_one;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED database_one;

-- COMMAND ----------

USE database_one;

CREATE TABLE table_3(
  employee_id INT,
  name STRING,
  age INT,
  regions STRING
  );


-- COMMAND ----------

INSERT INTO table_3
VALUES 
  (1, "Oliver", 28, "West"),
  (2, "James", 30, "Norteast"),
  (3, "Amelia", 22, "Southwest"),
  (4, "Sophia", 24, "Midwest"),
  (5, "Isabella", 26, "West");


-- COMMAND ----------

USE database_one;

CREATE TABLE table_4(
  employee_id INT,
  name STRING,
  age INT,
  regions STRING)
LOCATION 'dbfs:/mnt/demo/table_4';

-- COMMAND ----------

INSERT INTO table_4
VALUES 
  (1, "Oliver", 28, "West"),
  (2, "James", 30, "Norteast"),
  (3, "Amelia", 22, "Southwest"),
  (4, "Sophia", 24, "Midwest"),
  (5, "Isabella", 26, "West");

-- COMMAND ----------

DESCRIBE EXTENDED table_3;

-- COMMAND ----------

DESCRIBE EXTENDED table_4;

-- COMMAND ----------

DROP TABLE table_3;
DROP TABLE table_4;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create database in custom location outside of the hive directory

-- COMMAND ----------

CREATE DATABASE database_2
LOCATION 'dbfs:/Shared/schemas/database_2.db';

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED database_2;

-- COMMAND ----------

USE database_2;

CREATE TABLE table_5(
  employee_id INT,
  name STRING,
  age INT,
  regions STRING);

-- COMMAND ----------

INSERT INTO table_5
VALUES 
  (1, "Oliver", 28, "West"),
  (2, "James", 30, "Norteast"),
  (3, "Amelia", 22, "Southwest"),
  (4, "Sophia", 24, "Midwest"),
  (5, "Isabella", 26, "West");

-- COMMAND ----------

USE database_2;

CREATE TABLE table_6(
  employee_id INT,
  name STRING,
  age INT,
  regions STRING)
LOCATION 'dbfs:/mnt/demo/table_6';

-- COMMAND ----------

INSERT INTO table_6
VALUES 
  (1, "Oliver", 28, "West"),
  (2, "James", 30, "Norteast"),
  (3, "Amelia", 22, "Southwest"),
  (4, "Sophia", 24, "Midwest"),
  (5, "Isabella", 26, "West");

-- COMMAND ----------

DESCRIBE EXTENDED table_5;

-- COMMAND ----------

DESCRIBE EXTENDED table_6;

-- COMMAND ----------

DROP TABLE table_5;
DROP TABLE table_6;

-- COMMAND ----------


