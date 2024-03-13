# Databricks notebook source
order_items = spark.read.json('/FileStore/public/retail_db_json/order_items')

# COMMAND ----------

help(order_items.groupBy)

# COMMAND ----------

order_items.count()

# COMMAND ----------

order_items.show()

# COMMAND ----------

order_items.groupBy().min().show()

# COMMAND ----------

orders = spark.read.json('/FileStore/public/retail_db_json/orders')

# COMMAND ----------

orders.groupBy().min().show()

# COMMAND ----------

orders.dtypes

# COMMAND ----------

order_items_grouped = order_items.groupBy()

# COMMAND ----------

type(order_items_grouped)

# COMMAND ----------

help(order_items_grouped.count)

# COMMAND ----------

help(order_items_grouped.sum)

# COMMAND ----------


