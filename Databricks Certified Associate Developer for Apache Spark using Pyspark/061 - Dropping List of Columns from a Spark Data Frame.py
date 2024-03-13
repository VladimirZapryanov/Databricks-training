# Databricks notebook source
# MAGIC %run "./58 - Creating Spark Data Frame for Dropping Columns"

# COMMAND ----------

pii_columns = ['first_name', 'last_name', 'email', 'phone_numbers']

# COMMAND ----------

users_df_nopii = users_df.drop(*pii_columns)

# COMMAND ----------

users_df_nopii.printSchema()

# COMMAND ----------

users_df_nopii.show()

# COMMAND ----------

pii_columns = ['first_name', 'last_name', 'email', 'phone_numbers', 'street']

# COMMAND ----------

users_df_nopii = users_df.drop(*pii_columns)

# COMMAND ----------

users_df_nopii.show()

# COMMAND ----------


