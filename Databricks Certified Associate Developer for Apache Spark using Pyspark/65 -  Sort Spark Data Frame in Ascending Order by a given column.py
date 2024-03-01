# Databricks notebook source
from pyspark.sql.functions import size

# COMMAND ----------

# MAGIC %run "./64 - Creating Spark Data Frame for Sorting the Data"

# COMMAND ----------

help(users_df.sort)

# COMMAND ----------

help(users_df.orderBy)

# COMMAND ----------

users_df. \
    sort('first_name'). \
    show()

# COMMAND ----------

users_df. \
    sort('customer_from'). \
    show()

# COMMAND ----------

users_df. \
    withColumn('no_of_courses', size('courses')). \
    sort(size('courses')). \
    show()

# COMMAND ----------


