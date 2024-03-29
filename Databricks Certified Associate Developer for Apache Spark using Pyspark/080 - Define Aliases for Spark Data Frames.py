# Databricks notebook source
# MAGIC %run "./78 - Setup Data Sets to perform joins"

# COMMAND ----------

help(courses_df.alias)

# COMMAND ----------

type(courses_df.alias('c'))

# COMMAND ----------

courses_df.alias('c').select('c.course_id').show()

# COMMAND ----------

courses_df.alias('c').select('c.*').show()

# COMMAND ----------


