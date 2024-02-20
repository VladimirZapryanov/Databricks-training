# Databricks notebook source
from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

# MAGIC %run "./17 - Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

users_df. \
    select('id', 'first_name', 'last_name'). \
    withColumnRenamed('id', 'user_id'). \
    withColumnRenamed('first_name', 'user_first_name'). \
    withColumnRenamed('last_name', 'user_last_name'). \
    withColumn('user_full_name', concat('user_first_name', lit(', '), 'user_last_name')). \
    show()

# COMMAND ----------

users_df. \
    select(
        col('id').alias('user_id'),
        col('first_name').alias('user_first_name'),
        col('last_name').alias('user_last_name'),
        concat(col('first_name'), lit(', '), col('last_name')).alias('user_full_name')
        ). \
show()

# COMMAND ----------

users_df. \
    select(
        users_df['id'].alias('user_id'),
        users_df['first_name'].alias('user_first_name'),
        users_df['last_name'].alias('user_last_name'),
        concat(users_df['first_name'], lit(', '), users_df['last_name']).alias('user_full_name')
        ). \
show()

# COMMAND ----------


