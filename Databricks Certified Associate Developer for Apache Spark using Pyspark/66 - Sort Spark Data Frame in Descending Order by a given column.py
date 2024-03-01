# Databricks notebook source
from pyspark.sql.functions import desc, size

# COMMAND ----------

# MAGIC %run "./64 - Creating Spark Data Frame for Sorting the Data"

# COMMAND ----------

users_df.sort(desc('first_name')).show()

# COMMAND ----------

users_df.sort('first_name', ascending=False).show()

# COMMAND ----------

users_df.sort(users_df['first_name'], ascending=False).show()

# COMMAND ----------

users_df.sort(users_df['first_name'].desc()).show()

# COMMAND ----------

users_df.sort(desc('customer_from')).show()

# COMMAND ----------

users_df.sort(users_df['customer_from'].desc_nulls_first()).show()

# COMMAND ----------

users_df.sort(desc(size('courses'))).show()

# COMMAND ----------


