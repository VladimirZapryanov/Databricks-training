# Databricks notebook source
# MAGIC %run "./17 - Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

help(users_df.selectExpr)

# COMMAND ----------

users_df.selectExpr('*').show()

# COMMAND ----------

users_df.alias('u').selectExpr('u.*').show()

# COMMAND ----------

users_df.selectExpr('id', 'first_name', 'last_name').show()

# COMMAND ----------

users_df.selectExpr(['id', 'first_name', 'last_name']).show()

# COMMAND ----------

users_df.selectExpr('id', 'first_name', 'last_name', "concat(first_name, ', ', last_name) AS full_name").show()

# COMMAND ----------

users_df.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql("""
          SELECT id, first_name, last_name,
            concat(first_name, ', ', last_name) AS full_name
          FROM users
""").show()

# COMMAND ----------


