# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #1) Introduction

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/deltaguide.db/customer_data

# COMMAND ----------

# MAGIC %fs ls dbfs:/fil ePath/customer_t2/_delta_log

# COMMAND ----------

# Generate Spark DataFrame
data = spark.range(0, 5)

# Write the table in parquet format
data.write.format('parquet').save('/table_pq')

# Write the table in delta format
data.write.format('delta').save('/table_delta')

# COMMAND ----------

# Read first transaction
j0 = spark.read.json('/table_delta/_delta_log/000000000000.json')

# Review Add Information
j0.select('add.path').where('add is not null').show(20, False)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Describe table history using file path
# MAGIC DESCRIBE HISTORY delta.`/table_delta`;

# COMMAND ----------

data = spark.range(6, 10)
data.write.format('delta').mode('append').save('/table_delta')

# COMMAND ----------

spark.read.format("delta").load("/table_delta").count()

# COMMAND ----------

# MAGIC %sh ls -R /dbfs/table_delta/

# COMMAND ----------

# Read version 1
j1 = spark.read.json('/table_delta/_delta_log/000000000000000001.json')

# Review Add Information
j1.select('add.path').where('add is not null').show(20, False)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Describe table history using file path
# MAGIC DESCRIBE HISTORY delta.`/table_delta`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Run this statement first as Delta will do a format check
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false
# MAGIC

# COMMAND ----------

delta_path = '/table_delta/'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delete from Delta table where id <= 2
# MAGIC DELETE FROM delta.`table_delta` WHERE id <= 2

# COMMAND ----------

spark.read.format('delta').load('/table_delta').count()

# COMMAND ----------

# Remove information
j2.select("remove.path").where("remove is not null").show(20, False)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Describe table history using file path
# MAGIC DESCRIBE HISTORY delta.`/table_delta`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Run this statement first as Delta will do a format check
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

# MAGIC %python
# MAGIC delta_path = "/table_delta/"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize our Delta table
# MAGIC OPTIMIZE delta.`/table_delta/`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #2) Time Travel

# COMMAND ----------


