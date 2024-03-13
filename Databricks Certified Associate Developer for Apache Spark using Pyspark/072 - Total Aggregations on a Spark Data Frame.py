# Databricks notebook source
from pyspark.sql.functions import sum, count

# COMMAND ----------

order_items = spark.read.json('/FileStore/public/retail_db_json/order_items')

# COMMAND ----------

order_items.dtypes

# COMMAND ----------

order_items.printSchema()

# COMMAND ----------

order_items.show()

# COMMAND ----------

order_items.filter('order_item_order_id == 2').select(sum('order_item_subtotal').alias('order_revenue')).show()

# COMMAND ----------

order_items. \
    filter('order_item_order_id == 2'). \
    select(
        count('order_item_quantity').alias('order_item_count'),
        sum('order_item_quantity').alias('order_quantity'),
        sum('order_item_subtotal').alias('order_revenue')). \
    show()

# COMMAND ----------


