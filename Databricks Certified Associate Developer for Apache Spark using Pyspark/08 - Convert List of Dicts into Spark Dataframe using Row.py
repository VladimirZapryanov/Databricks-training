# Databricks notebook source
from pyspark.sql import Row

# COMMAND ----------

users_list = [
    {'user_id': 1, 'user_first_name': 'Scott'},
    {'user_id': 2, 'user_first_name': 'Donald'},
    {'user_id': 3, 'user_first_name': 'Mickey'},
    {'user_id': 4, 'user_first_name': 'Elvis'}
]

# COMMAND ----------

spark.createDataFrame(users_list)

# COMMAND ----------

help(Row)

# COMMAND ----------

user_details = users_list[1]

# COMMAND ----------

user_details

# COMMAND ----------

user_details.values()

# COMMAND ----------

Row(*user_details.values())

# COMMAND ----------

users_rows = [Row(*user.values()) for user in users_list]

# COMMAND ----------

users_rows

# COMMAND ----------

spark.createDataFrame(users_rows, "user_id int, user_first_name string")

# COMMAND ----------

users_rows = [Row(**user) for user in users_list]

# COMMAND ----------

users_rows

# COMMAND ----------

spark.createDataFrame(users_rows)

# COMMAND ----------

def dummy(**kwargs):
    print(kwargs)
    print(len(kwargs))

# COMMAND ----------

user_details = {'user_id': 1, 'user_first_name': 'Scott'}

# COMMAND ----------

dummy(user_details=user_details)

# COMMAND ----------

dummy(user_id=1, user_first_name='Scott')

# COMMAND ----------

dummy(**user_details)

# COMMAND ----------


