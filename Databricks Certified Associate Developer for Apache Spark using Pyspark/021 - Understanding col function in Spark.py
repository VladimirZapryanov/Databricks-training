# Databricks notebook source
from pyspark.sql.functions import col, date_format

# COMMAND ----------

# MAGIC %run "./17 - Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

users_df['id']

# COMMAND ----------

col('id')

# COMMAND ----------

users_df.select('id', 'first_name', 'last_name').show()

# COMMAND ----------

cols = ['id', 'first_name', 'last_name']
users_df.select(*cols).show()

# COMMAND ----------

help(col)

# COMMAND ----------

user_id = col('id')

# COMMAND ----------

users_df.select(user_id).show()

# COMMAND ----------

users_df.select('id', 'customer_from').show()

# COMMAND ----------

users_df.select('id', 'customer_from').dtypes

# COMMAND ----------

users_df.select(
    col('id'),
    date_format('customer_from', 'yyyyMMdd').cast('int').alias('customer_from')
).show()

# COMMAND ----------

cols = [col('id'), date_format('customer_from', 'yyyyMMdd').cast('int').alias('customer_from')]
users_df.select(*cols).show()

# COMMAND ----------


