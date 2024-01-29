# Databricks notebook source
# MAGIC %md
# MAGIC #Lecture

# COMMAND ----------

# MAGIC %run "/Apache Spark/apache-spark-programming-with-databricks-english/Includes/Classroom-Setup"

# COMMAND ----------

from pyspark.sql.functions import col, approx_count_distinct, count
import time

# COMMAND ----------

df = (spark
      .readStream
      .option("maxFilesPerTrigger", 1)
      .format("delta")
      .load(DA.paths.events)
      )

df.isStreaming

# COMMAND ----------

email_traffic_df = (df
                    .filter(col("traffic_source") == "email")
                    .withColumn("mobile", col("device").isin(["iOS", "Android"]))
                    .select("user_id", "event_timestamp", "mobile")
                    )

email_traffic_df.isStreaming

# COMMAND ----------

checkpoint_path = f"{DA.paths.checkpoints}/email_traffic"
output_path = f"{DA.paths.working_dir}/email_traffic/output"

devices_query = (email_traffic_df
                 .writeStream
                 .outputMode("append")
                 .format("delta")
                 .queryName("email_traffic")
                 .trigger(processingTime="1 second")
                 .option("checkpointLocation", checkpoint_path)
                 .start(output_path)
                )


# COMMAND ----------

devices_query.id

# COMMAND ----------

devices_query.status

# COMMAND ----------

devices_query.lastProgress

# COMMAND ----------

time.sleep(10)
devices_query.stop()

# COMMAND ----------

devices_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC #Lab

# COMMAND ----------

# MAGIC %md
# MAGIC ##5.1aL - Coupon Sales

# COMMAND ----------

# MAGIC %run "/Apache Spark/apache-spark-programming-with-databricks-english/Includes/Classroom-Setup-5.1a"

# COMMAND ----------

from pyspark.sql.functions import col, explode

# COMMAND ----------

df = (spark
      .readStream
      .option("maxFilesPerTrigger", 1)
      .format("delta")
      .load(DA.paths.sales)
)
display(df)
df.isStreaming

# COMMAND ----------

DA.tests.validate_1_1(df)

# COMMAND ----------

coupon_sales_df = (df
                   .withColumn("items", explode(col("items")))
                   .filter(col("items.coupon").isNotNull())
                   )

display(coupon_sales_df)

# COMMAND ----------

DA.tests.validate_2_1(coupon_sales_df.schema)

# COMMAND ----------

coupons_checkpoint_path = f"{DA.paths.checkpoints}/coupon-sales"
coupons_output_path = f"{DA.paths.working_dir}/coupon-sales/output"

coupon_sales_query = (coupon_sales_df
                        .writeStream
                        .queryName("coupon_sales")
                        .format("delta")
                        .outputMode("append")
                        .trigger(processingTime="1 second")
                        .option("checkpointLocation", coupons_checkpoint_path)
                        .start(coupons_output_path)
                     )

DA.block_until_stream_is_ready(coupon_sales_query)

# COMMAND ----------

DA.tests.validate_3_1(coupon_sales_query)

# COMMAND ----------

query_id = coupon_sales_query.id

# COMMAND ----------

query_status = coupon_sales_query.status
display(query_status)

# COMMAND ----------

DA.tests.validate_4_1(query_id, query_status)

# COMMAND ----------

coupon_sales_query.stop()

# COMMAND ----------

DA.tests.validate_5_1(coupon_sales_query)

# COMMAND ----------

display(spark.read.format("delta").load(coupons_output_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ##5.1bL - Hourly Activity by Traffic

# COMMAND ----------

# MAGIC %run "/Apache Spark/apache-spark-programming-with-databricks-english/Includes/Classroom-Setup-5.1b"

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

schema = "device STRING, ecommerce STRUCT<purchase_revenue_in_usd: DOUBLE, total_item_quantity: BIGINT, unique_items: BIGINT>, event_name STRING, event_previous_timestamp BIGINT, event_timestamp BIGINT, geo STRUCT<city: STRING, state: STRING>, items ARRAY<STRUCT<coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>, traffic_source STRING, user_first_touch_timestamp BIGINT, user_id STRING"

# Directory of hourly events logged from the BedBricks website on July 3, 2020
hourly_events_path = f"{DA.paths.datasets}/ecommerce/events/events-2020-07-03.json"

df = (spark.readStream
           .schema(schema)
           .option("maxFilesPerTrigger", 1)
           .json(hourly_events_path))

display(df)

# COMMAND ----------

events_df = (df
             .withColumn("createdAt", (col("event_timestamp") / 1e6).cast("timestamp"))
             .withWatermark("createdAt", "2 hours")
            )

# COMMAND ----------

DA.tests.validate_1_1(events_df.schema)

# COMMAND ----------


