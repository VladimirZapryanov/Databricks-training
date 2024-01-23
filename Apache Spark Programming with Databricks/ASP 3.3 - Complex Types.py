# Databricks notebook source
# MAGIC %md 
# MAGIC #Lecture
# MAGIC

# COMMAND ----------

# MAGIC %run "/Apache Spark/apache-spark-programming-with-databricks-english/Includes/Classroom-Setup"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.format("delta").load(DA.paths.sales)

display(df)

# COMMAND ----------

details_df = (df
              .withColumn("items", explode("items"))
              .select("email", "items.item_name")
              .withColumn("details", split(col("item_name"), " "))
             )
             
display(details_df)

# COMMAND ----------

display(df.select(split(df.email, "@", 0).alias("email_handle")))

# COMMAND ----------

mattress_df = (details_df
               .filter(array_contains(col("details"), "Mattress"))
               .withColumn("size", element_at(col("details"), 2))
               )

display(mattress_df)

# COMMAND ----------

size_df = mattress_df.groupBy("email").agg(collect_set("size").alias("size options"))

display(size_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab

# COMMAND ----------

details_df = (df
              .withColumn("items", explode("items"))
              .select("email", "items.item_name")
              .withColumn("details", split(col("item_name"), " "))
             )
             
display(details_df)

# COMMAND ----------

assert details_df.count() == 235911

# COMMAND ----------

mattress_df = (details_df
               .filter(array_contains(col("details"), "Mattress"))
               .withColumn("size", element_at(col("details"), 2))
               .withColumn("quality", element_at(col("details"), 1))
              )

display(mattress_df)

# COMMAND ----------

assert mattress_df.count() == 208384

# COMMAND ----------

pillow_df = (details_df
             .filter(array_contains(col("details"), "Pillow"))
             .withColumn("size", element_at(col("details"), 1))
             .withColumn("quality", element_at(col("details"), 2))
            )
            
display(pillow_df)

# COMMAND ----------

assert pillow_df.count() == 27527

# COMMAND ----------

union_df = mattress_df.union(pillow_df).drop("details")
display(union_df)

# COMMAND ----------

assert union_df.count() == 235911

# COMMAND ----------

options_df = (union_df
              .groupBy("email")
              .agg(collect_set("size").alias("size options"),
                   collect_set("quality").alias("quality options"))
             )

display(options_df)

# COMMAND ----------

assert options_df.count() == 210370

# COMMAND ----------

DA.cleanup()

# COMMAND ----------


