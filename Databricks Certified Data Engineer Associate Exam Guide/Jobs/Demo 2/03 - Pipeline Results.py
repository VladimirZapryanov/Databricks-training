# Databricks notebook source
files = dbutils.fs.ls("/mnt/retail-org/delta_db")
display(files)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/retail-org/delta_db/system/

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/mnt/retail-org/delta_db/system/events/")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`dbfs:/mnt/retail-org/delta_db/system/events/`

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/mnt/retail-org/delta_db/tables/")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta_db.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta_db.sales_orders_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta_db.sales_orders_cleaned

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta_db.sales_order_in_chicago

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta_db.sales_order_in_la

# COMMAND ----------


