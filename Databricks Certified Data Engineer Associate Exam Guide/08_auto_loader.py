# Databricks notebook source
# import the relevant data types
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType
 
# For a streaming source DataFrame we must define the schema
# Please update with your specific file path and assign it to the variable orders_full_path
bankdata_streaming_path = "/mnt/streaming-demo/streaming_dataset/bankdata_streaming.csv"
 
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

bankdata_sdf = spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").schema(bankdata_schema).load(bankdata_streaming_path, header=True)

# COMMAND ----------

bankdata_sdf.createTempView("bank_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM bank_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC #3) Creating table from streaming dataframe

# COMMAND ----------

bankdata_sdf.writeStream.option("checkpointLocation", "/mnt/streaming-demo/streaming_dataset/table/_checkpointLocation").table("bankdata_t")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bankdata_t;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED bankdata_t;

# COMMAND ----------

#cansel the stream
for s in spark.streams.active:
    print(f'Stoping: {s.id}')
    s.stop()
    s.awaitTermination()

# COMMAND ----------


