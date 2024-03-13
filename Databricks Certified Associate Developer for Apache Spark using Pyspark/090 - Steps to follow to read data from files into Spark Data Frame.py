# Databricks notebook source
# MAGIC %fs ls /FileStore/public/retail_db/orders

# COMMAND ----------

spark.read.text('/FileStore/public/retail_db/orders').show(truncate=False)

# COMMAND ----------

# MAGIC %fs ls /FileStore/public/retail_db_json/orders

# COMMAND ----------

spark.read.json('/FileStore/public/retail_db_json/orders').show()

# COMMAND ----------

# MAGIC %fs ls /user/root/retail_db_parquet/orders

# COMMAND ----------


