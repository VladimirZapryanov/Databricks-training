# Databricks notebook source
bank_data = spark.read.parquet('/mnt/gold/bank_data')

# COMMAND ----------

bank_data.display()

# COMMAND ----------

bank_data.write.format("delta").mode("overwrite").saveAsTable("my_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM my_table

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED my_table
