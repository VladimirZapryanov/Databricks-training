# Databricks notebook source
import getpass

# COMMAND ----------

df = spark.read.csv('dbfs:/databricks-datasets/asa/airlines', header=True)

# COMMAND ----------

help(df.coalesce)

# COMMAND ----------

help(df.repartition)

# COMMAND ----------

df = spark.read.csv('dbfs:/databricks-datasets/asa/airlines', header=True, inferSchema=True)

# COMMAND ----------

dbutils.fs.ls('dbfs:/databricks-datasets/asa/airlines')

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# coalescing the Dataframe to 16
df.coalesce(16).rdd.getNumPartitions()

# COMMAND ----------

# not effective as coalesce can be used to reduce the number of partitioning.
# Faster as no shuffling is involved
df.coalesce(186).rdd.getNumPartitions()

# COMMAND ----------

# incurs shuffling
# Watch the execution time and compare with coalesce
df.repartition(16).rdd.getNumPartitions()

# COMMAND ----------

# repartitioned to higher number of partitions
df.repartition(186, 'Year', 'Month').rdd.getNumPartitions()

# COMMAND ----------

username = getpass.getuser()

# COMMAND ----------


dbutils.fs.rm(f'/user/{username}/airlines', recurse=True)

# COMMAND ----------

df.write.mode('overwrite').csv(f'/user/{username}/airlines', header=True, compression='gzip')

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/airlines')

# COMMAND ----------

df.repartition(16).write.mode('overwrite').csv(f'/user/{username}/airlines', header=True, compression='gzip')

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/airlines')

# COMMAND ----------

# If you use repartition it will take longer time than this.
df.coalesce(16).write.mode('overwrite').csv(f'/user/{username}/airlines', header=True, compression='gzip')

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/airlines')

# COMMAND ----------


