# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture
# MAGIC

# COMMAND ----------

# MAGIC %run "/Apache Spark/apache-spark-programming-with-databricks-english/Includes/Classroom-Setup"

# COMMAND ----------

df = spark.read.format("delta").load(DA.paths.events)
display(df)

# COMMAND ----------

df.groupBy("event_name")

# COMMAND ----------

df.groupBy("geo.state", "geo.city")

# COMMAND ----------

event_counts_df = df.groupBy("event_name").count()
display(event_counts_df)

# COMMAND ----------

avg_state_purchases_df = df.groupBy("geo.state").avg("ecommerce.purchase_revenue_in_usd")
display(avg_state_purchases_df)

# COMMAND ----------

city_purchase_quantities_df = df.groupBy("geo.state", "geo.city").sum("ecommerce.total_item_quantity", "ecommerce.purchase_revenue_in_usd")
display(city_purchase_quantities_df)

# COMMAND ----------

from pyspark.sql.functions import sum

state_purchases_df = df.groupBy("geo.state").agg(sum("ecommerce.total_item_quantity").alias("total_purchases"))
display(state_purchases_df)

# COMMAND ----------

from pyspark.sql.functions import avg, approx_count_distinct

state_aggregates_df = (df
                       .groupBy("geo.state")
                       .agg(avg("ecommerce.total_item_quantity").alias("avg_quantity"),
                            approx_count_distinct("user_id").alias("distinct_users"))
                      )

display(state_aggregates_df)

# COMMAND ----------

from pyspark.sql.functions import cos, sqrt

display(spark.range(10)
        .withColumn("sqrt", sqrt("id"))
        .withColumn("cos", cos("id"))
        )

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab

# COMMAND ----------

from pyspark.sql.functions import col

# Purchase events logged on the BedBricks website
df = (spark.read.format("delta").load(DA.paths.events)
      .withColumn("revenue", col("ecommerce.purchase_revenue_in_usd"))
      .filter(col("revenue").isNotNull())
      .drop("event_name")
     )

display(df)

# COMMAND ----------

from pyspark.sql.functions import avg, sum

traffic_df = (df
             .groupBy("traffic_source")
             .agg(sum("revenue").alias("total_rev")),
             avg("revenue").alias("avg_rev")       

)
display(traffic_df)

# avg_state_purchases_df = df.groupBy("geo.state").avg("ecommerce.purchase_revenue_in_usd")
# display(avg_state_purchases_df)

# COMMAND ----------


