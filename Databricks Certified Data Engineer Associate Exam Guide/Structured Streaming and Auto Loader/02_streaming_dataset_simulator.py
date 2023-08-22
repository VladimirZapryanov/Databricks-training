# Databricks notebook source
bankdata_full_path = '/mnt/streaming-demo/full_dataset/bank_data.csv'

bankdata_full = spark.read.csv(bankdata_full_path, header=True)

# COMMAND ----------

bankdata_full.display()

# COMMAND ----------

bankdata_full.filter(bankdata_full['CustomerId'] == 1).display()

# COMMAND ----------

bankdata_streaming_path = '/mnt/streaming-demo/streaming_dataset/bankdata_streaming.csv'

# COMMAND ----------

bankdata_1 = bankdata_full.filter(bankdata_full['CustomerId'] == 1)
bankdata_1.write.options(header=True).mode('append').csv(bankdata_streaming_path)

# COMMAND ----------

bankdata_streaming = spark.read.csv(bankdata_streaming_path, header=True)
display(bankdata_streaming)

# COMMAND ----------

bankdata_2 = bankdata_full.filter(bankdata_full['CustomerId'] == 2)
bankdata_2.write.options(header=True).mode('append').csv(bankdata_streaming_path)

# COMMAND ----------

bankdata_3 = bankdata_full.filter(bankdata_full['CustomerId'] == 3)
bankdata_3.write.options(header=True).mode('append').csv(bankdata_streaming_path)


# COMMAND ----------

bankdata_4_5 = bankdata_full.filter((bankdata_full['CustomerId'] == 4) | (bankdata_full['CustomerId'] == 5))
bankdata_4_5.write.options(header=True).mode('append').csv(bankdata_streaming_path)

# COMMAND ----------

bankdata_6_7 = bankdata_full.filter((bankdata_full['CustomerId'] == 6) | (bankdata_full['CustomerId'] == 7))
bankdata_6_7.write.options(header=True).mode('append').csv(bankdata_streaming_path)

# COMMAND ----------

bankdata_streaming = spark.read.csv(bankdata_streaming_path, header=True)
display(bankdata_streaming)

# COMMAND ----------

dbutils.fs.rm(bankdata_streaming_path, recurse=True)

# COMMAND ----------


