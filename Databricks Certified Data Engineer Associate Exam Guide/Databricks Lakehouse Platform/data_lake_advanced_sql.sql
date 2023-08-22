-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Time Travel

-- COMMAND ----------

DESCRIBE HISTORY employee;

-- COMMAND ----------

SELECT *
FROM employee;

-- COMMAND ----------

SELECT *
FROM employee
VERSION AS OF 3;

-- COMMAND ----------

SELECT *
FROM employee@v3;

-- COMMAND ----------

DELETE FROM employee;

-- COMMAND ----------

SELECT * 
FROM employee;

-- COMMAND ----------

DESCRIBE HISTORY employee;

-- COMMAND ----------

SELECT *
FROM employee@v4;

-- COMMAND ----------

RESTORE TABLE employee TO VERSION AS OF 4;

-- COMMAND ----------

SELECT *
FROM employee;

-- COMMAND ----------

DESCRIBE HISTORY employee;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # OPTIMIZE command

-- COMMAND ----------

DESCRIBE DETAIL employee;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/employee'

-- COMMAND ----------

OPTIMIZE employee
ZORDER BY employeeId;

-- COMMAND ----------

DROP TABLE IF EXISTS people_10m;

CREATE TABLE IF NOT EXISTS people_10m
AS SELECT *
FROM delta.`/databricks-datasets/learning-spark-v2/people/people-10m.delta`;

-- COMMAND ----------

DESCRIBE HISTORY people_10m;

-- COMMAND ----------

DESCRIBE DETAIL people_10m;

-- COMMAND ----------

INSERT INTO people_10m
VALUES
(1, "Hisako", "Isabella", "Malitrott", "F", 1961, 938-80-1874, 15482);

-- COMMAND ----------

DESCRIBE DETAIL people_10m;

-- COMMAND ----------

OPTIMIZE people_10m
ZORDER BY (gender);

-- COMMAND ----------

DESCRIBE HISTORY people_10m;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #VACUUM command

-- COMMAND ----------

VACUUM people_10m;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC ls 'dbfs:/user/hive/warehouse/people_10m'

-- COMMAND ----------

VACUUM people_10m RETAIN 0 HOURS;

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

VACUUM people_10m RETAIN 0 HOURS;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC ls 'dbfs:/user/hive/warehouse/people_10m'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Delete the tables permanently
-- MAGIC

-- COMMAND ----------

DROP TABLE employee;

-- COMMAND ----------

DROP TABLE people_10m;

-- COMMAND ----------

DESCRIBE HISTORY people10m;

-- COMMAND ----------


