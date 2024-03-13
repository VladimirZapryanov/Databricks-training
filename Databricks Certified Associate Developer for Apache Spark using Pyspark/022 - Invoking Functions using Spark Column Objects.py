# Databricks notebook source
from pyspark.sql.functions import col, concat, lit, date_format

# COMMAND ----------

# MAGIC %run "./17 - Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

users_df.select('id', 'first_name', 'last_name', concat(col('first_name'), lit(', '), col('last_name')).alias('full_name')).show()

# COMMAND ----------

users_df.select('id', 'customer_from').show()

# COMMAND ----------

customer_from_alias = date_format('customer_from', 'yyyyMMdd').cast('int').alias('customer_from')

# COMMAND ----------

users_df.select('id', customer_from_alias).show()

# COMMAND ----------


