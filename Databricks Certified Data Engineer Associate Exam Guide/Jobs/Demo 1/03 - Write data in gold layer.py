# Databricks notebook source
# MAGIC %md
# MAGIC # 1) Read data from silver layer

# COMMAND ----------

bank_data = spark.read.parquet('/mnt/silver/bank_data')

# COMMAND ----------

bank_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 1) Write data to gold layer

# COMMAND ----------

bank_data =bank_data[bank_data['Balance'] !=0]

# COMMAND ----------

bank_data.display()

# COMMAND ----------

bank_data.write.parquet('/mnt/gold/bank_data')

# COMMAND ----------

bank_data = spark.read.parquet('/mnt/gold/bank_data')

# COMMAND ----------

bank_data.display()
