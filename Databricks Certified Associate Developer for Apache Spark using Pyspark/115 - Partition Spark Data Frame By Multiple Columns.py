# Databricks notebook source
import getpass
from pyspark.sql.functions import date_format

# COMMAND ----------

orders = spark.read.json('/FileStore/public/retail_db_json/orders')

# COMMAND ----------

username = getpass.getuser()

# COMMAND ----------

orders. \
    withColumn('year', date_format('order_date', 'yyyy')). \
    withColumn('month', date_format('order_date', 'MM')). \
    withColumn('day_of_month', date_format('order_date', 'dd')). \
    show()

# COMMAND ----------

dbutils.fs.rm(f'/user/{username}/retail_db/orders_partitioned', recurse=True)

# COMMAND ----------

orders. \
    withColumn('year', date_format('order_date', 'yyyy')). \
    withColumn('month', date_format('order_date', 'MM')). \
    withColumn('day_of_month', date_format('order_date', 'dd')). \
    coalesce(1). \
    write. \
    partitionBy('year', 'month', 'day_of_month'). \
    parquet(f'/user/{username}/retail_db/orders_partitioned')

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/retail_db/orders_partitioned')

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/retail_db/orders_partitioned/year=2014/month=01/day_of_month=09')

# COMMAND ----------

orders.count()

# COMMAND ----------

spark.read.parquet(f'/user/{username}/retail_db/orders_partitioned').dtypes

# COMMAND ----------

spark.read.parquet(f'/user/{username}/retail_db/orders_partitioned').show()

# COMMAND ----------

spark.read.parquet(f'/user/{username}/retail_db/orders_partitioned').count()

# COMMAND ----------


