# Databricks notebook source
# MAGIC %md
# MAGIC #1) Create a simple dataset
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW foods(food) AS 
# MAGIC VALUES
# MAGIC   ('meat'),
# MAGIC   ('beans'),
# MAGIC   ('potatoes'),
# MAGIC   ('bread');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM foods;

# COMMAND ----------

# MAGIC %sql
# MAGIC meat => MEAT!!!

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION yelling(text STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN concat(upper(text), '!!!')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT yelling(food) 
# MAGIC FROM foods;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION yelling;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION EXTENDED yelling;

# COMMAND ----------

# MAGIC %md
# MAGIC #CASE/WHEN

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM foods;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *,
# MAGIC CASE 
# MAGIC   WHEN food = 'beans' THEN 'I love beans'
# MAGIC   WHEN food = 'potatoes' THEN 'My favorite vegetable is potatoes'
# MAGIC   WHEN food <> 'meat' THEN concat('Do you have any good recipies for ', food, '?')
# MAGIC   ELSE concat('I dont eat ', food)
# MAGIC END
# MAGIC FROM foods;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION food_i_like(food STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   CASE 
# MAGIC     WHEN food = 'beans' THEN 'I love beans'
# MAGIC     WHEN food = 'potatoes' THEN 'My favorite vegetable is potatoes'
# MAGIC     WHEN food <> 'meat' THEN concat('Do you have any good recipies for ', food, '?')
# MAGIC     ELSE concat('I dont eat ', food)
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT food_i_like(food)
# MAGIC FROM foods;
