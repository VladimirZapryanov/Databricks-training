# Databricks notebook source
# MAGIC %fs ls /FileStore/public/retail_db

# COMMAND ----------

# MAGIC %fs ls /FileStore/public/retail_db/orders/

# COMMAND ----------

schema = """
    order_id INT,
    order_date TIMESTAMP,
    order_customer_id INT,
    order_status STRING
"""

# COMMAND ----------

orders = spark.read.schema(schema).csv('/FileStore/public/retail_db/orders/')

# COMMAND ----------

orders.show()

# COMMAND ----------

orders.printSchema()

# COMMAND ----------

orders.dtypes

# COMMAND ----------

# MAGIC %fs ls /FileStore/public/retail_db_json

# COMMAND ----------

# MAGIC %fs ls /FileStore/public/retail_db_json/orders

# COMMAND ----------

orders = spark.read.json('/FileStore/public/retail_db_json/orders')

# COMMAND ----------

orders.show()

# COMMAND ----------

orders.printSchema()

# COMMAND ----------


