# Databricks notebook source
# MAGIC %md
# MAGIC #1) Delta as a Stream Source

# COMMAND ----------

spark.readStream.format('delta').load('/mnt/delta/events')

import io.delta.implicits._
spark.readStream.delta('/mnt/delta/events')

# COMMAND ----------

spark.readStream.format("delta")
    .option("ignoreDeletes", "true")
    .load("/mnt/delta/user_events")

# COMMAND ----------

spark.readStream.format("delta")
    .option("ignoreChanges", "true")
    .load("/mnt/delta/user_events")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #2) Specify initial position

# COMMAND ----------

# Via version
spark.readStream.format("delta")
    .option("startingVersion", "5")
    .load("/mnt/delta/user_events")

# Via timestamp
spark.readStream.format("delta")
    .option("startingTimestamp", "2018-10-18")
    .load("/mnt/delta/user_events")


# COMMAND ----------

# MAGIC %md
# MAGIC #3) Delta Table as a Sink
# MAGIC

# COMMAND ----------

# Append mode with Path method
events.writeStream
 .format("delta")
 .outputMode("append")
 .option("checkpointLocation", "/delta/events/_checkpoints/etl-from-json")
 .start("/delta/events")

# Append mode with table method
events.writeStream
 .format("delta")
 .outputMode("append")
 .option("checkpointLocation", "/delta/events/_checkpoints/etl-from-json")
 .table("events")

# Complete mode
spark.readStream
 .format("delta")
 .load("/mnt/delta/events")
 .groupBy("customerId")
 .count()
 .writeStream
 .format("delta")
 .outputMode("complete")
 .option("checkpointLocation", "/mnt/delta/eventsByCustomer/_checkpoints/streamingagg")
 .start("/mnt/delta/eventsByCustomer")
