# Databricks notebook source
from pyspark.sql.functions import current_date, current_timestamp, lit, to_date, to_timestamp

# COMMAND ----------

l = [('X', )]

# COMMAND ----------

df = spark.createDataFrame(l).toDF('dummy')

# COMMAND ----------

df.show()

# COMMAND ----------

help(current_date)

# COMMAND ----------

df.select(current_date()).show()

# COMMAND ----------

help(current_timestamp)

# COMMAND ----------

df.select(current_timestamp()).show(truncate=False)

# COMMAND ----------

help(to_date)

# COMMAND ----------

df.select(to_date(lit('20210228'), 'yyyyMMdd').alias('to_date')).show()

# COMMAND ----------

help(to_timestamp)

# COMMAND ----------

df.select(to_timestamp(lit('20210228 1725'), 'yyyyMMdd HHmm').alias('to_timestamp')).show()

# COMMAND ----------


