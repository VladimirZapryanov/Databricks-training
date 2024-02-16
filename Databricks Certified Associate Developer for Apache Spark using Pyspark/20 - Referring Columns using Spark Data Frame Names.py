# Databricks notebook source
from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

# MAGIC %run "./17 - Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

users_df['id']

# COMMAND ----------

type(users_df['id'])

# COMMAND ----------

users_df.select('id', col('first_name'), 'last_name').show()

# COMMAND ----------

users_df.select(users_df['id'], col('first_name'), 'last_name').show()

# COMMAND ----------

users_df. \
    select(
        'id', 'first_name', 'last_name', 
        concat(users_df['first_name'], lit(', '), col('last_name')).alias('full_name')
    ). \
    show()

# COMMAND ----------

users_df.alias('u').selectExpr('id', 'first_name', 'last_name', "concat(u.first_name, ', ', u.last_name) AS full_name").show()

# COMMAND ----------

users_df.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql("""
    SELECT id, first_name, last_name,
        concat(u.first_name, ', ', u.last_name) AS full_name
    FROM users AS u
"""). \
    show()

# COMMAND ----------


