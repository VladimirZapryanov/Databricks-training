# Databricks notebook source
ages = [21, 23, 18, 41, 32]

# COMMAND ----------

type(ages)

# COMMAND ----------

spark

# COMMAND ----------

help(spark.createDataFrame)

# COMMAND ----------

spark.createDataFrame(ages, 'int') 

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType

# COMMAND ----------

spark.createDataFrame(ages, IntegerType()) 

# COMMAND ----------

names = ['Scott', 'Donald', 'Mickey']

# COMMAND ----------

spark.createDataFrame(names, 'string')

# COMMAND ----------

spark.createDataFrame(names, StringType())

# COMMAND ----------


