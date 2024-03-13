# Databricks notebook source
ages_list = [(21, ), (23, ), (41, ), (32, )]

# COMMAND ----------

type(ages_list)

# COMMAND ----------

type(ages_list[2])

# COMMAND ----------

spark.createDataFrame(ages_list)

# COMMAND ----------

df_age = spark.createDataFrame(ages_list, 'age int')
display(df_age)

# COMMAND ----------

users_list = [(1, 'Scott', 'ivanow'), (2, 'Donald', 'ivanow'), (3, 'Mickey', 'ivanow'), (4, 'Elvis', 'ivanow')]

# COMMAND ----------

spark.createDataFrame(users_list)

# COMMAND ----------

df_users = spark.createDataFrame(users_list, 'user_id int, user_first_name string, user_last_name string')
display(df_users)

# COMMAND ----------


