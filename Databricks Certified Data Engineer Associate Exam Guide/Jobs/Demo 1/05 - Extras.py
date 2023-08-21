# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE my_table

# COMMAND ----------

dbutils.fs.unmount('/mnt/bronze')

# COMMAND ----------

dbutils.fs.unmount('/mnt/silver')

# COMMAND ----------

dbutils.fs.unmount('/mnt/gold')

# COMMAND ----------

# MAGIC %sql
# MAGIC Show tables;

# COMMAND ----------

dbutils.fs.unmount('/mnt/retail-org')

# COMMAND ----------


