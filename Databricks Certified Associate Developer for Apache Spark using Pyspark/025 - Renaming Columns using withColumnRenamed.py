# Databricks notebook source
# MAGIC %run "./17 - Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

help(users_df.withColumnRenamed)

# COMMAND ----------

users_df.select('id', 'first_name', 'last_name').show()

# COMMAND ----------

users_df. \
    select('id', 'first_name', 'last_name'). \
    withColumnRenamed('id', 'user_id'). \
    withColumnRenamed('first_name', 'user_first_name'). \
    withColumnRenamed('last_name', 'user_last_name'). \
show()

# COMMAND ----------


