# Databricks notebook source
from pyspark.sql.functions import date_add, date_sub, current_date, current_timestamp, datediff, months_between, add_months, round

# COMMAND ----------

datetimes = [("2014-02-28", "2014-02-28 10:00:00.123"),
                     ("2016-02-29", "2016-02-29 08:08:08.999"),
                     ("2017-10-31", "2017-12-31 11:59:59.123"),
                     ("2019-11-30", "2019-08-31 00:00:00.000")
                ]

# COMMAND ----------

datetimesDF = spark.createDataFrame(datetimes, schema='date STRING, time STRING')

# COMMAND ----------

datetimesDF.show(truncate=False)

# COMMAND ----------

datetimesDF2 = spark.createDataFrame(datetimes).toDF('date', 'time')

# COMMAND ----------

datetimesDF2.show(truncate=False)

# COMMAND ----------

datetimesDF.printSchema()

# COMMAND ----------

datetimesDF2.printSchema()

# COMMAND ----------

help(date_add)

# COMMAND ----------

help(date_sub)

# COMMAND ----------

datetimesDF. \
    withColumn('date_add_date', date_add('date', 10)). \
    withColumn('date_add_time', date_add('time', 10)). \
    withColumn('date_sub_date', date_sub('date', 10)). \
    withColumn('date_sub_time', date_sub('time', 10)). \
show(truncate=False)

# COMMAND ----------

help(datediff)

# COMMAND ----------

datetimesDF. \
    withColumn('datediff_date', datediff(current_date(), 'date')). \
    withColumn('datediff_time', datediff(current_date(), 'time')). \
    show()

# COMMAND ----------

help(months_between)

# COMMAND ----------

help(add_months)

# COMMAND ----------

help(round)

# COMMAND ----------

datetimesDF. \
    withColumn("months_between_date", round(months_between(current_date(), "date"), 2)). \
    withColumn("months_between_time", round(months_between(current_timestamp(), "time"), 2)). \
    withColumn("add_months_date", add_months("date", 3)). \
    withColumn("add_months_time", add_months("time", 3)). \
    show(truncate=False)

# COMMAND ----------


