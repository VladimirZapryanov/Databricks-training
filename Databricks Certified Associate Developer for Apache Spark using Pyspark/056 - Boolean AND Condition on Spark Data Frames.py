# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %run "./47 - Creating Spark Data Frame for Filtering"

# COMMAND ----------

users_df. \
    select('*'). \
    filter((col('gender') == 'male') & (col('is_customer') == 'True')). \
    show()

# COMMAND ----------

users_df. \
    select('*'). \
    filter('gender == "male" AND is_customer == True'). \
    show()

# COMMAND ----------

users_df. \
    select('*'). \
    filter((col('customer_from') >= '2021-01-20') & (col('customer_from')<= '2021-02-15')). \
    show()

# COMMAND ----------

users_df. \
    select('*'). \
    filter(col('customer_from').between('2021-01-20', '2021-02-15')). \
    show()

# COMMAND ----------

users_df. \
    select('*'). \
    filter('customer_from >= "2021-01-20" AND customer_from <= "2021-02-15"'). \
    show()

# COMMAND ----------

users_df. \
    select('*'). \
    filter('customer_from BETWEEN "2021-01-20" AND "2021-02-15"'). \
    show()

# COMMAND ----------


