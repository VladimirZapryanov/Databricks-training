# Databricks notebook source
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

bankdata_sdf.isStreaming

# COMMAND ----------

display(bankdata_sdf)

# COMMAND ----------

print('Hello!')

# COMMAND ----------

#cansel the stream
for s in spark.streams.active:
    print(f'Stoping: {s.id}')
    s.stop()
    s.awaitTermination()

# COMMAND ----------


