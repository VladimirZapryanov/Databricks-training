# Databricks notebook source
from pyspark.sql.functions import broadcast

# COMMAND ----------

# Default size is 10 MB.
spark.conf.get('spark.sql.autoBroadcastJoinThreshold')

# COMMAND ----------

# We can disable broadcast join using this approach
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '0')

# COMMAND ----------

# Resetting to original value
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '10485760b')

# COMMAND ----------

# 1+ GB Data Set
clickstream = spark.read.csv('dbfs:/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed/', sep='\t', header=True)

# COMMAND ----------

# 10+ GB Data Set
articles = spark.read.parquet('dbfs:/databricks-datasets/wikipedia-datasets/data-001/en_wikipedia/articles-only-parquet/')

# COMMAND ----------

# MAGIC %%time
# MAGIC
# MAGIC # Default will be reduce side join as the size of smaller data set is more than 10 MB (default broadcast size)
# MAGIC clickstream.join(articles, articles.id == clickstream.curr_id).count()

# COMMAND ----------

# MAGIC %%time
# MAGIC # We can use broadcast function to override existing broadcast join threshold
# MAGIC # We can also override by using this code spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '1500m')
# MAGIC broadcast(clickstream).join(articles, articles.id == clickstream.curr_id).count()
