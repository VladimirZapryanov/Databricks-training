# Databricks notebook source
# MAGIC %run "/Apache Spark/apache-spark-programming-with-databricks-english/Includes/Classroom-Setup"

# COMMAND ----------

from pyspark.sql.functions import col, size

# COMMAND ----------

sales_df = spark.read.parquet(f"{DA.paths.datasets}/ecommerce/sales/sales.parquet")
delta_sales_path = f"{DA.paths.working_dir}/delta-sales"

# COMMAND ----------

sales_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_sales_path)

# COMMAND ----------

assert len(dbutils.fs.ls(delta_sales_path)) > 0
print("All test pass")

# COMMAND ----------

display(sales_df)

# COMMAND ----------

updated_sales_df = sales_df.withColumn("items", size(col("items")))
                   
display(updated_sales_df)

# COMMAND ----------

from pyspark.sql.types import IntegerType

assert updated_sales_df.schema[6].dataType == IntegerType()
print("All test pass")

# COMMAND ----------

updated_sales_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_sales_path)

# COMMAND ----------

assert spark.read.format("delta").load(delta_sales_path).schema[6].dataType == IntegerType()
print("All test pass")

# COMMAND ----------

spark.sql("""DROP TABLE IF EXISTS sales_delta""")
spark.sql("""CREATE TABLE sales_delta USING DELTA LOCATION '{}'""".format(delta_sales_path))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY sales_delta;

# COMMAND ----------

sales_delta_df = spark.sql("SELECT * FROM sales_delta")
assert sales_delta_df.count() == 210370
assert sales_delta_df.schema[6].dataType == IntegerType()
print("All test pass")

# COMMAND ----------

old_sales_df = spark.read.format("delta").option("versionAsOf", 0).load(delta_sales_path) 
display(old_sales_df)

# COMMAND ----------

assert old_sales_df.select(size(col("items"))).first()[0] == 1
print("All test pass")

# COMMAND ----------

DA.cleanup()

# COMMAND ----------


