# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

(spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .load(f"{dataset_bookstore}/orders-raw")
      .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .table("orders_updates")
);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM orders_updates;

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY orders_updates;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE orders_updates;

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/demo/orders_checkpoint", True)

# COMMAND ----------


