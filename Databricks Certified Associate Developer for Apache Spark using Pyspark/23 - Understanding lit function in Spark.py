# Databricks notebook source
from pyspark.sql.functions import lit, col

# COMMAND ----------

# MAGIC %run "./17 - Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

users_df.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql("""
          SELECT id, (amount_paid + 25) AS amount_paid
          FROM users
          """).show()

# COMMAND ----------

users_df.selectExpr('id', '(amount_paid + 25) AS amaount_paid').show()

# COMMAND ----------

users_df.select('id', (col('amount_paid') + lit(25.0)).alias('amount_paid')).show()

# COMMAND ----------

lit(25)

# COMMAND ----------


