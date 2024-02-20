# Databricks notebook source
from pyspark.sql.functions import concat, lit, col, size

# COMMAND ----------

# MAGIC %run "./17 - Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

users_df.select('id', 'first_name', 'last_name').show()

# COMMAND ----------

users_df. \
    select('id', 'first_name', 'last_name',
           concat('first_name', lit(', '), 'last_name').alias('full_name')).show()

# COMMAND ----------

users_df. \
    select('id', 'first_name', 'last_name'). \
    withColumn('full_name', concat('first_name', lit(', '), 'last_name')). \
        show()

# COMMAND ----------

users_df. \
    select('id', 'first_name', 'last_name'). \
    withColumn('fn', col('first_name')). \
        show()

# COMMAND ----------

users_df. \
    select('id', 'first_name', 'last_name'). \
    withColumn('fn', users_df['first_name']). \
        show()

# COMMAND ----------

users_df.select('id', 'courses').show()

# COMMAND ----------

users_df.select('id', 'courses').dtypes

# COMMAND ----------

users_df.select('id', 'courses'). \
    withColumn('course_count', size('courses')). \
    show()

# COMMAND ----------


