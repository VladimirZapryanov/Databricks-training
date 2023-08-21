# Databricks notebook source
# MAGIC %md
# MAGIC # 1) Read data from bronze layer

# COMMAND ----------

bank_data_path = '/mnt/bronze/bank_data.csv'
 
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType
bank_data_schema = StructType([
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
 
bank_data=spark.read.csv(path=bank_data_path, header=True, schema=bank_data_schema)

# COMMAND ----------

bank_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Drom 3 columns from the dataframe countries

# COMMAND ----------

bank_data = bank_data.drop("Surname", "Geography", "Gender")

# COMMAND ----------

bank_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2) Write data in silver layer

# COMMAND ----------

# MAGIC %md
# MAGIC We will write above dataframe as a parquet file.
# MAGIC
# MAGIC Because the parquet is better than CSV, it can retain the data types that we have assigned

# COMMAND ----------

# MAGIC %md
# MAGIC **Note that:** the file here is the DBFS file path but it's an alias for the URI that's actually pointing to the storage container

# COMMAND ----------

bank_data.write.parquet('/mnt/silver/bank_data')

# COMMAND ----------

# MAGIC %md
# MAGIC - Go to azure portal - storage account - silver container - file shouls be there
# MAGIC - U can see the same in DBFS mount
