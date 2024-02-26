# Databricks notebook source
from pyspark.sql.functions import year, month, weekofyear, dayofmonth, dayofyear, dayofweek, current_date, current_timestamp, hour, minute, second

# COMMAND ----------

l = [('X',)]

# COMMAND ----------

df = spark.createDataFrame(l).toDF("dummy")

# COMMAND ----------

df.show()

# COMMAND ----------

df.select(
    current_date().alias('current_date'), 
    year(current_date()).alias('year'),
    month(current_date()).alias('month'),
    weekofyear(current_date()).alias('weekofyear'),
    dayofyear(current_date()).alias('dayofyear'),
    dayofmonth(current_date()).alias('dayofmonth'),
    dayofweek(current_date()).alias('dayofweek')
).show() #yyyy-MM-dd

# COMMAND ----------

df.select(
    current_timestamp().alias('current_timestamp'), 
    year(current_timestamp()).alias('year'),
    month(current_timestamp()).alias('month'),
    dayofmonth(current_timestamp()).alias('dayofmonth'),
    hour(current_timestamp()).alias('hour'),
    minute(current_timestamp()).alias('minute'),
    second(current_timestamp()).alias('second')
).show(truncate=False) #yyyy-MM-dd HH:mm:ss.SSS

# COMMAND ----------


