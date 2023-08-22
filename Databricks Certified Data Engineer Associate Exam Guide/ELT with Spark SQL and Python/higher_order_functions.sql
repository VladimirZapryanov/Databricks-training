-- Databricks notebook source
-- MAGIC %md
-- MAGIC #1) Create a table with nested data

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW nested_data AS 
SELECT id AS key,
  array(cast(rand(1) * 100 AS INT), cast(rand(2) * 100 AS INT), cast(rand(3) * 100 AS INT), cast(rand(4) * 100 AS INT), cast(rand(5) * 100 AS INT)) AS values,
  array(array(cast(rand(1) * 100 AS INT), cast(rand(2) * 100 AS INT), cast(rand(3) * 100 AS INT), cast(rand(4) * 100 AS INT), cast(rand(5) * 100 AS INT))) AS nested_values
FROM range(5);

-- COMMAND ----------

SELECT * 
FROM nested_data;

-- COMMAND ----------

SELECT key,
  values,
  TRANSFORM(values, value -> value + 1) AS value_plus_one
FROM nested_data;

-- COMMAND ----------

SELECT  key, values,
        nested_values,
        TRANSFORM(nested_values, 
          values -> TRANSFORM(values,
            value -> value + key + SIZE(values))) AS new_nested_values
FROM nested_data;

-- COMMAND ----------

SELECT key, values,
  TRANSFORM(values, value -> value + key) transformed_values
FROM nested_data;

-- COMMAND ----------

SELECT key, values,
  EXISTS(values, value -> value % 10 == 3) filtered_values
FROM nested_data;

-- COMMAND ----------

SELECT key, values,
  FILTER(values, value -> value > 50) filtered_values
FROM nested_data;

-- COMMAND ----------

SELECT key, values,
  REDUCE(values, 0, (value, acc) -> value + acc, acc -> acc) summed_values,
  REDUCE(values, 0, (value, acc) -> value + acc) summed_values_simple
FROM nested_data;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(sum([63, 53, 25, 95, 2]))

-- COMMAND ----------


