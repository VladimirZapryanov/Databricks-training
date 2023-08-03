-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

CREATE TABLE orders AS 
SELECT * FROM parquet.`${dataset.bookstore}/orders`;

-- COMMAND ----------

SELECT *
FROM orders;

-- COMMAND ----------

CREATE OR REPLACE TABLE orders AS 
SELECT * FROM parquet.`${dataset.bookstore}/orders`;

-- COMMAND ----------

DESCRIBE HISTORY orders;

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT * FROM parquet.`${dataset.bookstore}/orders`;

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------


