# Databricks notebook source
# MAGIC %run "./Includes/Classroom-Setup" 

# COMMAND ----------

import pandas as pd

# COMMAND ----------

spark_df = spark.read.csv(f"{datasets-dir}/sf-airbnb/sf-airbnb.csv", header="true", inferSchema="true", multiline="true", escape='"')
display(spark_df)

# COMMAND ----------

df = read_csv(f"{datasets-dir}/sf-airbnb/sf-airbnb.csv", inferSchema="true", multiline="true", escape='"')
df.head()

# COMMAND ----------

ps.set_option("compute.default_index_type", "distributed-sequence")
df_dist_sequence = ps.read_csv(f"{datasets-dir}/sf-airbnb/sf-airbnb.csv", inferSchema="true", multiline="true", escape='"')
df_dist_sequence.head()

# COMMAND ----------

df = ps.DataFrame(spark_df)
display(df)

# COMMAND ----------

df = spark_df.to_pandas_on_spark()
display(df)

# COMMAND ----------

display(df.to_spark())

# COMMAND ----------

display(spark_df.groupby("property_type").count().orderBy("count", ascending=False))

# COMMAND ----------

df['property_type'].value_counts()

# COMMAND ----------

ps.sql("SELECT distinct(property_type) FROM {df}")
