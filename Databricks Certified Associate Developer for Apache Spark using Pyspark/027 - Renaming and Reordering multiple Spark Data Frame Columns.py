# Databricks notebook source
# MAGIC %run "./17 - Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

# required columns from original list
required_columns = ['id', 'first_name', 'last_name', 'email', 'phone_numbers', 'courses']

# new column name list
target_column_names = ['user_id', 'user_first_name', 'user_last_name', 'user_email', 'user_phone_numbers', 'enrolled_courses']

# COMMAND ----------

help(users_df.toDF)

# COMMAND ----------

users_df. \
    select(required_columns). \
    show()

# COMMAND ----------

users_df. \
    select(required_columns). \
    toDF(*target_column_names). \
    show()

# COMMAND ----------

def myDF(*cols):
    print(type(cols))
    print(cols)

# COMMAND ----------

myDF(*['f1', 'f2'])

# COMMAND ----------


