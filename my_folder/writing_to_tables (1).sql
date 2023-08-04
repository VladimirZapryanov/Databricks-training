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

DESCRIBE HISTORY orders;

-- COMMAND ----------

INSERT INTO orders
SELECT * FROM parquet.`${dataset.bookstore}/orders-new`;

-- COMMAND ----------

SELECT COUNT(*) FROM orders;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_updates AS
SELECT * FROM json.`${dataset.bookstore}/customers-json-new`;

MERGE INTO customers
USING customers_updates 
ON customers.customer_id = customers_updates.customer_id
WHEN MATCHED AND customers.email IS NULL AND customers_updates.email IS NOT NULL THEN
  UPDATE SET email = customers_updates.email, updated = customers_updates.updated
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------


