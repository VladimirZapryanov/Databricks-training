# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %run "./47 - Creating Spark Data Frame for Filtering"

# COMMAND ----------

users_df. \
    select('*'). \
    filter((col('current_city') == '') | (col('current_city').isNull())). \
    show()

# COMMAND ----------

users_df. \
    select('*'). \
    filter('current_city == "" OR current_city IS NULL'). \
    show()

# COMMAND ----------

users_df. \
    select('*'). \
    filter((col('current_city') == 'Houston') | (col('current_city') == 'Dallas')). \
    show()

# COMMAND ----------

users_df. \
    select('*'). \
    filter('current_city == "Houston" OR current_city == "Dallas"'). \
    show()

# COMMAND ----------

users_df. \
    select('*'). \
    filter(col('current_city').isin('Houston', 'Dallas')). \
    show()

# COMMAND ----------

users_df. \
    select('*'). \
    filter("current_city IN ('Houston', 'Dallas')"). \
    show()

# COMMAND ----------

users_df. \
    select('*'). \
    filter(col('current_city').isin('Houston', 'Dallas', '')). \
    show()

# COMMAND ----------

users_df. \
    select('*'). \
    filter("current_city IN ('Houston', 'Dallas', '')"). \
    show()

# COMMAND ----------

users_df. \
    select('*'). \
    filter(col('current_city').isin('Houston', 'Dallas', '') | (col('current_city').isNull())). \
    show()

# COMMAND ----------

users_df. \
    select('*'). \
    filter("current_city IN ('Houston', 'Dallas', '') OR current_city IS NULL"). \
    show()

# COMMAND ----------


