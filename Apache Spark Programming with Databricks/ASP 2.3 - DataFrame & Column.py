# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture
# MAGIC

# COMMAND ----------

# MAGIC %run "/Apache Spark/apache-spark-programming-with-databricks-english/Includes/Classroom-Setup"

# COMMAND ----------

events_df = spark.read.format("delta").load(DA.paths.events)
display(events_df)

# COMMAND ----------

from pyspark.sql.functions import col

print(events_df.device)
print(events_df['device'])
print(col('device'))

# COMMAND ----------

# MAGIC %scala
# MAGIC $"device"

# COMMAND ----------


