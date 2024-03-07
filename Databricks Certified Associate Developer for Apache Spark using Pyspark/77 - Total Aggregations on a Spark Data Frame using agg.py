# Databricks notebook source
from pyspark.sql.functions import sum, count

# COMMAND ----------

order_items = spark.read.json('/FileStore/public/retail_db_json/order_items')

# COMMAND ----------

order_items.dtypes

# COMMAND ----------

order_items.filter('order_item_order_id = 2')

# COMMAND ----------

order_items.filter('order_item_order_id = 2').show()

# COMMAND ----------

help(sum)

# COMMAND ----------

help(order_items.agg)

# COMMAND ----------

help(order_items.groupBy('order_item_order_id').agg)

# COMMAND ----------

order_items.filter('order_item_order_id = 2').agg(sum('order_item_subtotal')).show()

# COMMAND ----------

order_items.filter('order_item_order_id = 2').agg(sum('order_item_subtotal').alias('order_revenue')).show()

# COMMAND ----------

order_items. \
    filter('order_item_order_id = 2'). \
    show()

# COMMAND ----------

order_items. \
    filter('order_item_order_id = 2'). \
    agg(
        count('order_item_quantity').alias('order_item_count'),
        sum('order_item_quantity').alias('order_quantity'),
        sum('order_item_subtotal').alias('order_revenue')
    ). \
    show()

# COMMAND ----------

# We can only perform one aggregation per one column using this approach
order_items. \
    filter('order_item_order_id = 2'). \
    agg(
        {'order_item_quantity': 'count', 'order_item_subtotal': 'sum'}
    ). \
    toDF('order_revenue', 'order_item_count'). \
    show()

# COMMAND ----------


