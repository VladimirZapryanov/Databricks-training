# Databricks notebook source
from pyspark.sql.functions import lit, to_date, to_timestamp, col

# COMMAND ----------

datetimes = [(20140228, "28-Feb-2014 10:00:00.123"),
                     (20160229, "20-Feb-2016 08:08:08.999"),
                     (20171031, "31-Dec-2017 11:59:59.123"),
                     (20191130, "31-Aug-2019 00:00:00.000")
                ]

# COMMAND ----------

datetimeDF = spark.createDataFrame(datetimes, schema='date BIGINT, time STRING')

# COMMAND ----------

datetimeDF.show(truncate=False)

# COMMAND ----------

l = [('x',)]

# COMMAND ----------

df = spark.createDataFrame(l).toDF('dummy')

# COMMAND ----------

df.show()

# COMMAND ----------

help(to_date)

# COMMAND ----------

df.select(to_date(lit('20210302'), 'yyyyMMdd').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('2021061'), 'yyyyDDD').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('02/03/2021'), 'dd/MM/yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('02-03-2021'), 'dd-MM-yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('02-Mar-2021'), 'dd-MMM-yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('02-March-2021'), 'dd-MMMM-yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('March 2, 2021'), 'MMMM d, yyyy').alias('to_date')).show()

# COMMAND ----------

help(to_timestamp)

# COMMAND ----------

df.select(to_timestamp(lit('02-Mar-2021'), 'dd-MMM-yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(to_timestamp(lit('02-Mar-2021 17:30:15'), 'dd-MMM-yyyy HH:mm:ss').alias('to_date')).show()

# COMMAND ----------

datetimeDF.printSchema()

# COMMAND ----------

datetimeDF.show(truncate=False)

# COMMAND ----------

datetimeDF. \
    withColumn('to_date', to_date(col('date').cast('string'), 'yyyyMMdd')). \
    withColumn('to_timestamp', to_timestamp(col('time'), 'dd-MMM-yyyy HH:mm:ss.SSS')). \
    show(truncate=False)

# COMMAND ----------


