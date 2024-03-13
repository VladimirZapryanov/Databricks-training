# Databricks notebook source
from pyspark.sql.functions import date_format

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

help(date_format)

# COMMAND ----------

datetimesDF. \
    withColumn("date_ym", date_format("date", "yyyyMM")). \
    withColumn("time_ym", date_format("time", "yyyyMM")). \
    show(truncate=False)

# yyyy
# MM
# dd
# DD
# HH
# hh
# mm
# ss
# SSS

# COMMAND ----------

datetimesDF. \
    withColumn("date_ym", date_format("date", "yyyyMM")). \
    withColumn("time_ym", date_format("time", "yyyyMM")). \
    printSchema()

# COMMAND ----------

datetimesDF. \
    withColumn("date_ym", date_format("date", "yyyyMM").cast('int')). \
    withColumn("time_ym", date_format("time", "yyyyMM").cast('int')). \
    printSchema()

# COMMAND ----------

datetimesDF. \
    withColumn("date_ym", date_format("date", "yyyyMM").cast('int')). \
    withColumn("time_ym", date_format("time", "yyyyMM").cast('int')). \
    show()

# COMMAND ----------

datetimesDF. \
    withColumn("date_dt", date_format("date", "yyyyMMddHHmmss")). \
    withColumn("date_ts", date_format("time", "yyyyMMddHHmmss")). \
    show(truncate=False)

# COMMAND ----------

datetimesDF. \
    withColumn("date_dt", date_format("date", "yyyyMMddHHmmss").cast('long')). \
    withColumn("date_ts", date_format("time", "yyyyMMddHHmmss").cast('long')). \
    show(truncate=False)

# COMMAND ----------

datetimesDF. \
    withColumn("date_yd", date_format("date", "yyyyDDD").cast('int')). \
    withColumn("time_yd", date_format("time", "yyyyDDD").cast('int')). \
    show(truncate=False)

# COMMAND ----------

datetimesDF. \
    withColumn("date_desc", date_format("date", "MMMM d, yyyy")). \
    show(truncate=False)

# COMMAND ----------

datetimesDF. \
    withColumn("day_name_abbr", date_format("date", "EE")). \
    show(truncate=False)

# COMMAND ----------

datetimesDF. \
    withColumn("day_name_full", date_format("date", "EEEE")). \
    show(truncate=False)

# COMMAND ----------


