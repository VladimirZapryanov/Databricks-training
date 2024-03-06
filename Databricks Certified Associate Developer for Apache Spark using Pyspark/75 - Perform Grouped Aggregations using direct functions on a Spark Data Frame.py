# Databricks notebook source
order_items = spark.read.json('/FileStore/public/retail_db_json/order_items')

# COMMAND ----------

order_items_grouped = order_items. \
    groupBy('order_item_order_id')

# COMMAND ----------

type(order_items_grouped)

# COMMAND ----------


