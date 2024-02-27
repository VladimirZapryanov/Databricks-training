# Databricks notebook source
from pyspark.sql.functions import col, isnan

# COMMAND ----------

# MAGIC %run "./47 - Creating Spark Data Frame for Filtering"

# COMMAND ----------

users_df.filter(col('is_customer') == True).show()

# COMMAND ----------

users_df.filter(col('is_customer') == 'true').show()

# COMMAND ----------

users_df.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql('''
    SELECT * FROM users
    WHERE is_customer = "true"
'''). \
    show()

# COMMAND ----------

users_df.filter(col('is_customer') == False).show()

# COMMAND ----------

users_df.filter(col('is_customer') == 'false').show()

# COMMAND ----------

spark.sql('''
    SELECT * FROM users
    WHERE is_customer = FALSE
'''). \
    show()

# COMMAND ----------

users_df.filter(col('current_city') == 'Dallas').show()

# COMMAND ----------

users_df.filter('current_city == "Dallas"').show()

# COMMAND ----------

users_df.filter(col('amount_paid') == 900.0).show()

# COMMAND ----------

users_df.filter('amount_paid == 900.0').show()

# COMMAND ----------

users_df.select('amount_paid', isnan('amount_paid')).show()

# COMMAND ----------

users_df.filter(isnan('amount_paid') == True).show()

# COMMAND ----------

users_df.filter('isnan(amount_paid) == True').show()

# COMMAND ----------


