-- Databricks notebook source
SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

SELECT * FROM view_ford_cars;

-- COMMAND ----------

SELECT * FROM global_temp.global_temp_view_after60s_cars;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # After Cluster Restart

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

SELECT * FROM global_temp.global_temp_view_after60s_cars;

-- COMMAND ----------

DROP TABLE cars;
DROP VIEW view_ford_cars;

-- COMMAND ----------


