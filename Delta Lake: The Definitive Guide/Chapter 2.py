# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #1) Introduction

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/deltaguide.db/customer_data

# COMMAND ----------

# MAGIC %fs ls dbfs:/fil ePath/customer_t2/_delta_log

# COMMAND ----------

# Generate Spark DataFrame
data = spark.range(0, 5)

# Write the table in parquet format
data.write.format('parquet').save('/table_pq')

# Write the table in delta format
data.write.format('delta').save('/table_delta')

# COMMAND ----------

# Read first transaction
j0 = spark.read.json('/table_delta/_delta_log/000000000000.json')

# Review Add Information
j0.select('add.path').where('add is not null').show(20, False)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Describe table history using file path
# MAGIC DESCRIBE HISTORY delta.`/table_delta`;

# COMMAND ----------

data = spark.range(6, 10)
data.write.format('delta').mode('append').save('/table_delta')

# COMMAND ----------

spark.read.format("delta").load("/table_delta").count()

# COMMAND ----------

# MAGIC %sh ls -R /dbfs/table_delta/

# COMMAND ----------

# Read version 1
j1 = spark.read.json('/table_delta/_delta_log/000000000000000001.json')

# Review Add Information
j1.select('add.path').where('add is not null').show(20, False)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Describe table history using file path
# MAGIC DESCRIBE HISTORY delta.`/table_delta`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Run this statement first as Delta will do a format check
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false
# MAGIC

# COMMAND ----------

delta_path = '/table_delta/'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delete from Delta table where id <= 2
# MAGIC DELETE FROM delta.`table_delta` WHERE id <= 2

# COMMAND ----------

spark.read.format('delta').load('/table_delta').count()

# COMMAND ----------

# Remove information
j2.select("remove.path").where("remove is not null").show(20, False)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Describe table history using file path
# MAGIC DESCRIBE HISTORY delta.`/table_delta`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Run this statement first as Delta will do a format check
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

# MAGIC %python
# MAGIC delta_path = "/table_delta/"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize our Delta table
# MAGIC OPTIMIZE delta.`/table_delta/`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #2) Time Travel

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##2.1) Using a timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Query metastore-defined Delta table by timestamp
# MAGIC SELECT * FROM my_table TIMESTAMP AS OF "2019-01-01";
# MAGIC SELECT * FROM my_table TIMESTAMP AS OF date_sub(current_date(), 1);
# MAGIC SELECT * FROM my_table TIMESTAMP AS OF "2019-01-01 01:30:00.000";
# MAGIC
# MAGIC -- Query Delta table by file path by timestamp
# MAGIC SELECT * FROM delta.`<path-to-delta>` TIMESTAMP AS OF "2019-01-01";
# MAGIC SELECT * FROM delta.`<path-to-delta>` TIMESTAMP AS OF date_sub(current_date(), 1);
# MAGIC SELECT * FROM delta.`<path-to-delta>` TIMESTAMP AS OF "2019-01-01 01:30:00.000";
# MAGIC

# COMMAND ----------

# Load into Spark DataFrame from Delta table by timestamp
(df = spark.read
    .format('delta')
    .option('timestampAsOf', '2019-01-01')
    .load('/path/to/my/table')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Row count as of timestamp t1
# MAGIC SELECT COUNT(1) FROM customer_1 TIMESTAMP AS OF "2020-10-30T18:15:03.000+0000";
# MAGIC
# MAGIC -- Row count as of timestamp t2
# MAGIC SELECT COUNT(1) FROM customer_1 TIMESTAMP AS OF "2020-10-30T18:10:47.000+0000";

# COMMAND ----------

# timestamps
t1 = '2020-10-30T18:15:03.000+0000'
t2 = '2020-10-30T18:10:47.000+0000'
DELTA_PATH = '/demo/customer_t1'

# Row count as of timestamp t1
(spark.read
    .format('delta')
    .option('timestampAsOf', t1)
    .load(DELTA_PATH)
    .count()
)

# Row count as of timestamp t2
(spark.read
    .format('delta')
    .option('timestampAsOf', t2)
    .load(DELTA_PATH)
    .count()
)

# COMMAND ----------

spark.read.format("delta").load(<path-to-delta>@yyyyMMddHHmmssSSS)

# Delta table base path
BASE_PATH="/demo/customer_t1"

# Include timestamp in yyyyMMddHHmmssSSS format
DELTA_PATH=BASE_PATH + "@20201030181503000"

# Get row count of the Delta table by timestamp using @ parameter
spark.read.format("delta").load(DELTA_PATH).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##2.2) Using a version number

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query metastore-defined Delta table by version
# MAGIC SELECT COUNT(*) FROM my_table VERSION AS OF 5238;
# MAGIC SELECT COUNT(*) FROM my_table@v5238;
# MAGIC
# MAGIC -- Query Delta table by file path by version
# MAGIC SELECT count(*) FROM delta.`/path/to/my/table@v5238`;

# COMMAND ----------

# Query Delta table by version using versionAsOf
(df = spark.read
    .format('delta')
    .option('versionAsOf', '5238')
    .load('/path/to/my/table')
 )

 # Query Delta table by version using @ parameter
 (df = spark.read
    .format('delta')
    .load('/path/to/my/table@v5238')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Query metastore-defined Delta table by version 19
# MAGIC SELECT COUNT(1) FROM customer_t1 VERSION AS OF 19
# MAGIC SELECT COUNT(1) FROM customer_t1@v19
# MAGIC
# MAGIC -- Query metastore-defined Delta table by version 18
# MAGIC SELECT COUNT(1) FROM customer_t1 VERSION AS OF 18
# MAGIC SELECT COUNT(1) FROM customer_t1@v18

# COMMAND ----------

    # Delta table base path
DELTA_PATH="/demo/customer_t1"

# Row count of Delta table by version 19
(spark.read
    .format("delta")
    .option("versionAsOf", 19)
    .load(DELTA_PATH)
    .count()
)

# Row count of Delta table by version 18
(spark.read
    .format("delta")
    .option("versionAsOf", 18)
    .load(DELTA_PATH)
    .count()
 )
 
# Row count of Delta table by @ parameter for version 18
(spark.read
    .format("delta")
    .load(DELTA_PATH + "@v18")
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3) Time travel use cases

# COMMAND ----------

# MAGIC %md
# MAGIC ###2.3.1) Use Case: Governance and Auditing

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customer_delete_keys

# COMMAND ----------

# Query table using metastore defined table
sql("SELECT * FROM customer_delete_keys").show()

# Query table from file path
spark.read.format(“delta”).load("/customer_delta_keys").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Identify the records
# MAGIC SELECT COUNT(*)
# MAGIC FROM customer_t1
# MAGIC WHERE c_customer_id IN (
# MAGIC   SELECT c_customer_id
# MAGIC   FROM customer_delete_keys
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Delete identified records
# MAGIC DELETE
# MAGIC FROM customer_t1
# MAGIC WHERE c_customer_id IN (
# MAGIC   SELECT c_customer_id
# MAGIC   FROM customer_delete_keys
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###2.3.2) Use Case: Rollbacks

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY customer_t1

# COMMAND ----------

# Restore customer_t1 table to Version 17
# Load Version 17 data using df
df = spark.read.format('delta').option('versionAsOf', '17').load('/customer_t1')

# Overwrite Version 17 as the current version
df.write.format('delta').mode('overwrite').save('/customer_t1')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Restore table as of version 17
# MAGIC RESTORE customer_t1 VERSION AS OF 17
