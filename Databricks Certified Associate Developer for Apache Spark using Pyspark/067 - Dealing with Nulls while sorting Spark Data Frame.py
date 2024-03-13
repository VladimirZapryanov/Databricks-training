# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %run "./64 - Creating Spark Data Frame for Sorting the Data"

# COMMAND ----------

users_df.sort(col('customer_from').asc_nulls_last()).show()

# COMMAND ----------

users_df.sort(users_df['customer_from'].asc_nulls_last()).show()

# COMMAND ----------

users_df.sort(col('customer_from').desc()).show()

# COMMAND ----------

users_df.sort(users_df['customer_from'].desc_nulls_first()).show()

# COMMAND ----------


