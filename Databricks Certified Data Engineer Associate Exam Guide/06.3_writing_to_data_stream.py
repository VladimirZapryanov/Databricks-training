# Databricks notebook source
# MAGIC %md
# MAGIC #1) Writing a datastream

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType

bankdata_streaming_path = '/mnt/streaming-demo/streaming_dataset/bankdata_streaming.csv'

bankdata_schema = StructType([
                    StructField("CustomerId", IntegerType()),
                    StructField("Surname", StringType()),
                    StructField("CreditScore", IntegerType()),
                    StructField("Geography", StringType()),
                    StructField("Gender", StringType()),
                    StructField("Age", IntegerType()),
                    StructField("Tenure", IntegerType()),
                    StructField("Balance", DoubleType()),
                    StructField("NumOfProducts", IntegerType()),
                    StructField("HasCrCard", IntegerType()),
                    StructField("IsActiveMember", IntegerType()),
                    StructField("EstimatedSalary", DoubleType()),
                    StructField("Exited", IntegerType())
                    ]
                    )

# COMMAND ----------

bankdata_sdf= spark.readStream.csv(bankdata_streaming_path, bankdata_schema, header=True)

# COMMAND ----------

bankdata_sdf.display()

# COMMAND ----------

streamQuery = bankdata_sdf.writeStream.format('delta').\
    option('checkpointLocation', '/mnt/streaming-demo/streaming_dataset/bankdata_stream_sink/_checkpointLocation').\
    start('/mnt/streaming-demo/streaming_dataset/bankdata_stream_sink')

# COMMAND ----------

streamQuery.isActive

# COMMAND ----------

streamQuery.recentProgress

# COMMAND ----------

spark.read.format('delta').load('/mnt/streaming-demo/streaming_dataset/bankdata_stream_sink').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #2) Write datastream into a table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE streaming_db

# COMMAND ----------

streamQuery = bankdata_sdf.writeStream.format('delta').\
    option('checkpointLocation', '/mnt/streaming-demo/streaming_dataset/streaming_db/managed/_checkpointLocation').\
    toTable('streaming_db.bankdata_m')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM streaming_db.bankdata_m;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM streaming_db.bankdata_m;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE streaming_db.bankdata_m;

# COMMAND ----------

#cansel the stream
for s in spark.streams.active:
    print(f'Stoping: {s.id}')
    s.stop()
    s.awaitTermination()

# COMMAND ----------


