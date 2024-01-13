# Databricks notebook source
# MAGIC %md 
# MAGIC #Lecture

# COMMAND ----------

# MAGIC %run "/Apache Spark/apache-spark-programming-with-databricks-english/Includes/Classroom-Setup"

# COMMAND ----------

from pyspark.sql.functions import col, date_format, year, month, dayofweek, minute, second, to_date, date_add, approx_count_distinct, avg

# COMMAND ----------

df = spark.read.format("delta").load(DA.paths.events).select("user_id", col("event_timestamp").alias("timestamp"))
display(df)

# COMMAND ----------

timestamp_df = df.withColumn("timestamp", (col("timestamp") / 1e6).cast("timestamp"))
display(timestamp_df)


# COMMAND ----------

formatted_df = (timestamp_df
                .withColumn("date string", date_format("timestamp", "MMMM dd, yyyy"))
                .withColumn("time string", date_format("timestamp", "HH:mm:ss.SSSSSS"))
                )

display(formatted_df)

# COMMAND ----------

datetime_df = (timestamp_df
                .withColumn("year", year(col("timestamp")))
                .withColumn("month", month(col("timestamp")))
                .withColumn("dayofweek", dayofweek(col("timestamp")))
                .withColumn("minute", minute(col("timestamp")))
                .withColumn("second", second(col("timestamp")))
)

display(datetime_df)

# COMMAND ----------

date_df = timestamp_df.withColumn("date", to_date(col("timestamp")))
display(date_df)

# COMMAND ----------

plus_2_df = timestamp_df.withColumn("plus_two_days", date_add(col("timestamp"), 2))
display(plus_2_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Lab

# COMMAND ----------

df = (spark
      .read
      .format("delta")
      .load(DA.paths.events)
      .select("user_id", col("event_timestamp").alias("ts"))
     )

display(df)

# COMMAND ----------

datetime_df = (df
               .withColumn("ts", (col("ts") / 1e6).cast("timestamp"))
               .withColumn("date", to_date(col("ts")))
)

display(datetime_df)

# COMMAND ----------

from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

expected1a = StructType([StructField("user_id", StringType(), True),
                         StructField("ts", TimestampType(), True),
                         StructField("date", DateType(), True)])

result1a = datetime_df.schema

assert expected1a == result1a, "datetime_df does not have the expected schema"
print("All test pass")

# COMMAND ----------

active_users_df = (datetime_df
                   .groupBy("date")
                   .agg(approx_count_distinct("user_id").alias('active_users'))
                   .sort("date")
                  )   

display(active_users_df)

# COMMAND ----------

from pyspark.sql.types import LongType

expected2a = StructType([StructField("date", DateType(), True),
                         StructField("active_users", LongType(), False)])

result2a = active_users_df.schema

assert expected2a == result2a, "active_users_df does not have the expected schema"
print("All test pass")

# COMMAND ----------

active_dow_df = (active_users_df
                 .withColumn("day", date_format("date", "E"))
                 .groupBy(col("day"))
                 .agg(avg("active_users").alias("avg_users"))
)

display(active_dow_df)

# COMMAND ----------

expected3b = [("Fri", 247180.66666666666), ("Mon", 238195.5), ("Sat", 278482.0), ("Sun", 282905.5), ("Thu", 264620.0), ("Tue", 260942.5), ("Wed", 227214.0)]

result3b = [(row.day, row.avg_users) for row in active_dow_df.sort("day").collect()]

assert expected3b == result3b, "active_dow_df does not have the expected values"
print("All test pass")

# COMMAND ----------

DA.cleanup()

# COMMAND ----------


