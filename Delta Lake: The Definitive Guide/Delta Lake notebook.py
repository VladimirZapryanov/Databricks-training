# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # 1) Creating your first Delta table.

# COMMAND ----------

data = spark.range(0,5)
data.write.format("delta").save("/delta")


# COMMAND ----------

spark.read.format("delta").load("/delta").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM delta.`/delta`

# COMMAND ----------

data.write.format("delta").saveAsTable("myTable")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE id (
# MAGIC  date DATE,
# MAGIC  id INTEGER)
# MAGIC USING DELTA
# MAGIC LOCATION "/delta"
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls(PARQUET_PATH))

# COMMAND ----------

display(dbutils.fs.ls(DELTALAKE_PATH))

# COMMAND ----------


