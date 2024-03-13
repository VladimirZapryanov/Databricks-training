# Databricks notebook source
from pyspark.sql.functions import ltrim, rtrim, trim, col, expr

# COMMAND ----------

l = [('    Hello.   ',)]

# COMMAND ----------

df = spark.createDataFrame(l).toDF('dummy')

# COMMAND ----------

df.show()

# COMMAND ----------

help(ltrim)

# COMMAND ----------

help(rtrim)

# COMMAND ----------

help(trim)

# COMMAND ----------

df.withColumn("ltrim", ltrim(col("dummy"))). \
  withColumn("rtrim", rtrim(col("dummy"))). \
  withColumn("trim", trim("dummy")). \
  show()

# COMMAND ----------

spark.sql('DESCRIBE FUNCTION rtrim').show(truncate=False)

# COMMAND ----------

df.withColumn("ltrim", expr("ltrim(dummy)")). \
  withColumn("rtrim", expr("rtrim('.', rtrim(dummy))")). \
  withColumn("trim", trim(col("dummy"))). \
  show()

# COMMAND ----------

spark.sql('DESCRIBE FUNCTION trim').show(truncate=False)

# COMMAND ----------

df.withColumn("ltrim", expr("trim(LEADING ' ' FROM dummy)")). \
  withColumn("rtrim", expr("trim(TRAILING '.' FROM rtrim(dummy))")). \
  withColumn("trim", expr("trim(BOTH ' ' FROM dummy)")). \
  show()

# COMMAND ----------


