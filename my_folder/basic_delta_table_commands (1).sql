-- Databricks notebook source
CREATE TABLE employee (
  id INT,
  name STRING,
  salary DOUBLE
)

-- COMMAND ----------

INSERT INTO employee
VALUES
  (1, 'Vlado', 2000),
  (2, 'Joro', 2200),
  (3, 'Georgi', 3000),
  (4, 'Ivan', 1800),
  (5, 'Maria', 2100),
  (6, 'Petur', 2800)

-- COMMAND ----------

SELECT *
FROM employee
ORDER BY salary DESC

-- COMMAND ----------

DESCRIBE DETAIL employee

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employee'

-- COMMAND ----------

UPDATE employee
SET salary = salary + 500
WHERE salary <= 2000

-- COMMAND ----------

SELECT *
FROM employee
ORDER BY salary DESC

-- COMMAND ----------

DESCRIBE HISTORY employee

-- COMMAND ----------

SELECT * 
FROM employee@v1
ORDER BY salary DESC

-- COMMAND ----------

DELETE FROM employee

-- COMMAND ----------

DESCRIBE HISTORY employee

-- COMMAND ----------

SELECT *
FROM employee

-- COMMAND ----------

RESTORE TABLE employee TO VERSION AS OF 2

-- COMMAND ----------

SELECT *
FROM employee
ORDER BY salary DESC

-- COMMAND ----------

DESCRIBE DETAIL employee

-- COMMAND ----------

OPTIMIZE employee
ZORDER BY id

-- COMMAND ----------

VACUUM employee

-- COMMAND ----------

DROP TABLE employee

-- COMMAND ----------

CREATE DATABASE test_db

-- COMMAND ----------


