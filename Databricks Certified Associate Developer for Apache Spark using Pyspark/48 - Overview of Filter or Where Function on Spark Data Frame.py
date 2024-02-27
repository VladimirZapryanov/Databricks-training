# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %run "./47 - Creating Spark Data Frame for Filtering"

# COMMAND ----------

help(users_df.filter)

# COMMAND ----------

users_df.where?

# COMMAND ----------

users_df.filter(col('id') == 1).show()

# COMMAND ----------

users_df.where(col('id') == 1).show()

# COMMAND ----------

users_df.filter(users_df['id'] == 1).show()

# COMMAND ----------

users_df.where(users_df['id'] == 1).show()

# COMMAND ----------

users_df.filter('id = 1').show()

# COMMAND ----------

users_df.where('id = 1').show()

# COMMAND ----------

users_df.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql(""" 
          SELECT *
          FROM users
          WHERE id = 1
          """).show()

# COMMAND ----------


