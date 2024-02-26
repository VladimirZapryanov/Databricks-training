# Databricks notebook source
from pyspark.sql.functions import trunc, date_trunc

# COMMAND ----------

help(trunc)

# COMMAND ----------

help(date_trunc)

# COMMAND ----------

datetimes = [("2014-02-28", "2014-02-28 10:00:00.123"),
                     ("2016-02-29", "2016-02-29 08:08:08.999"),
                     ("2017-10-31", "2017-12-31 11:59:59.123"),
                     ("2019-11-30", "2019-08-31 00:00:00.000")
                ]

# COMMAND ----------

datetimesDF = spark.createDataFrame(datetimes, schema="date STRING, time STRING")

# COMMAND ----------

datetimesDF.show(truncate=False)

# COMMAND ----------

datetimesDF. \
    withColumn('date_trunc', trunc('date', 'MM')). \
    withColumn('time_trunc', trunc('time', 'yy')). \
    show(truncate=False)

# COMMAND ----------

datetimesDF. \
    withColumn('date_trunc', date_trunc('MM', 'date')). \
    withColumn('time_trunc', date_trunc('yy', 'time')). \
    show(truncate=False)

# COMMAND ----------

datetimesDF. \
    withColumn("date_dt", date_trunc("HOUR", "date")). \
    withColumn("time_dt", date_trunc("HOUR", "time")). \
    withColumn("time_dt1", date_trunc("dd", "time")). \
    show(truncate=False)

# COMMAND ----------


