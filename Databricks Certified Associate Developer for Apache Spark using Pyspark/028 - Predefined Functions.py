# Databricks notebook source
from pyspark.sql.functions import date_format

# COMMAND ----------

orders = spark.read.csv(
    '/FileStore/public/retail_db/orders',
    schema='order_id INT, order_date STRING, order_customer_id INT, order_status STRING'
)

# COMMAND ----------

orders.show()

# COMMAND ----------

orders.printSchema()

# COMMAND ----------

help(date_format)

# COMMAND ----------

orders.select('*', date_format('order_date', 'yyyy-MM').alias('order_month')).show()

# COMMAND ----------

orders.withColumn('order_month', date_format('order_date', 'yyyy-MM')).show()

# COMMAND ----------

orders. \
    filter(date_format('order_date', 'yyyyMM') == 201401). \
    show()

# COMMAND ----------

orders. \
    groupBy(date_format('order_date', 'yyyyMM').alias('order_month')). \
    count(). \
    show()

# COMMAND ----------


