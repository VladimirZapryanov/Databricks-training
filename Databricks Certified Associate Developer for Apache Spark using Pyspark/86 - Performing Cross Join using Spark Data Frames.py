# Databricks notebook source
# MAGIC %run "./78 - Setup Data Sets to perform joins"

# COMMAND ----------

help(courses_df.crossJoin)

# COMMAND ----------

users_df.crossJoin(courses_df).show()

# COMMAND ----------

users_df.crossJoin(courses_df).count()

# COMMAND ----------

users_df.join(courses_df).show()

# COMMAND ----------

users_df.join(courses_df).count()

# COMMAND ----------

users_df.join(courses_df, how='cross').show()

# COMMAND ----------

users_df.join(courses_df, how='cross').count()

# COMMAND ----------


