# Databricks notebook source
# MAGIC %fs ls FileStore/public/retail_db_json

# COMMAND ----------

orders = spark.read.json('/FileStore/public/retail_db_json/orders')

# COMMAND ----------

orders.show()

# COMMAND ----------

orders.printSchema()

# COMMAND ----------

order_items = spark.read.json('/FileStore/public/retail_db_json/order_items')

# COMMAND ----------

order_items.show()

# COMMAND ----------

order_items.printSchema()

# COMMAND ----------


