-- Databricks notebook source
CREATE TABLE IF NOT EXISTS smartphones(
  id INT,
  name STRING,
  brand STRING,
  year INT
);

-- COMMAND ----------

INSERT INTO smartphones
VALUES 
  (1, 'iPhone 14', 'Apple', 2022),
  (2, 'iPhone 13', 'Apple', 2021),
  (3, 'iPhone 6', 'Apple', 2014),
  (4, 'iPad Air', 'Apple', 2013),
  (5, 'Galaxy S22', 'Samsung', 2022),
  (6, 'Galaxy Z Fold', 'Samsung', 2022),
  (7, 'Galaxy S9', 'Samsung', 2016),
  (8, '12 Pro', 'Xiomi', 2022),
  (9, 'Readmi 11T Pro', 'Xiomi', 2022),
  (10, 'Readmi Note 11', 'Xiomi', 2021);

-- COMMAND ----------

SELECT *
FROM smartphones

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE VIEW view_apple_phones
AS SELECT *
FROM smartphones
WHERE brand = 'Apple';

SELECT *
FROM view_apple_phones

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE TEMP VIEW phone_brands_temp_view
AS SELECT DISTINCT brand
FROM smartphones;

-- COMMAND ----------

SELECT * 
FROM phone_brands_temp_view;

-- COMMAND ----------

CREATE GLOBAL TEMPORARY VIEW phone_years
AS SELECT *
FROM smartphones
WHERE year > 2020
ORDER BY year DESC;

-- COMMAND ----------

SELECT * 
FROM global_temp.phone_years;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

DROP TABLE smartphones;

-- COMMAND ----------

DROP VIEW view_apple_phones;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------


