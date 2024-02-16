# Databricks notebook source
# MAGIC %run "./17 - Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

users_df.select('*').show()

# COMMAND ----------

users_df.select('id', 'first_name', 'last_name').show()

# COMMAND ----------

users_df.select(['id', 'first_name', 'last_name']).show()

# COMMAND ----------

users_df.alias('u').select('u.*').show()

# COMMAND ----------

users_df.alias('u').select('u.id', 'u.first_name', 'u.last_name').show()

# COMMAND ----------

users_df.select(col('id'), 'first_name', 'last_name').show()

# COMMAND ----------

users_df.select(
    col('id'),
    'first_name',
    'last_name',
    concat(col('first_name'), lit(', '), col('last_name')).alias('full_name')
).show()

# COMMAND ----------


