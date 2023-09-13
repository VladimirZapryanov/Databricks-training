# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # 1) Creating your first Delta table.

# COMMAND ----------

data = spark.range(0,5)
data.write.format("delta").save("/delta")


# COMMAND ----------

spark.read.format("delta").load("/delta").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM delta.`/delta`

# COMMAND ----------

data.write.format("delta").saveAsTable("myTable")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE id (
# MAGIC  date DATE,
# MAGIC  id INTEGER)
# MAGIC USING DELTA
# MAGIC LOCATION "/delta"
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls(PARQUET_PATH))

# COMMAND ----------

display(dbutils.fs.ls(DELTALAKE_PATH))

# COMMAND ----------

# MAGIC %md   
# MAGIC
# MAGIC # 2) Use DESCRIBE and VACUUM commands

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Review history by Delta table file path
# MAGIC DESCRIBE HISTORY delta.`/ml/loan_by_state`;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Review history by Delta table defined in the metastore as `loan_by_state`
# MAGIC DESCRIBE HISTORY loan_by_state;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Review last 5 operations by Delta table defined in the metastore as `loan_by_state`
# MAGIC DESCRIBE HISTORY loan_by_state
# MAGIC LIMIT 5;

# COMMAND ----------

from delta.tables import *
deltaTable = DeltaTable.forPath(spark, "/ml/loan_by_state")

# COMMAND ----------

fullHistoryDF = deltaTable.history()

# COMMAND ----------

last5OperationsDF = deltaTable.history(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- vacuum files in a path-based table by default retention threshold
# MAGIC VACUUM delta.`/data/events`

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- vacuum files by metastore defined table by default retention threshold
# MAGIC VACUUM eventsTable

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- vacuum files by metastore defined table that are no longer required older than 100 hours old
# MAGIC VACUUM eventsTable RETAIN 100 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- dry run: get the list of files to be deleted
# MAGIC VACUUM eventsTable DRY RUN

# COMMAND ----------

from delta.tables import *

# vacuum files in path-based tables
deltaTable = DeltaTable.forPath(spark, pathToTable)

# vacuum files in metastore-based tables
deltaTable = DeltaTable.forName(spark, tableName)

# vacuum files in path-based table by default retention threshold
deltaTable.vacuum()

# vacuum files not required by versions more than 100 hours old
deltaTable.vacuum(100)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Describe detail using Delta file path
# MAGIC DESCRIBE DETAIL delta.`/ml/loan_by_state`;
# MAGIC
# MAGIC -- Describe detail using metastore-defined Delta table
# MAGIC DESCRIBE DETAIL loan_by_state;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE TABLE table_name;
# MAGIC
# MAGIC DESCRIBE TABLE EXTENDED table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #3) Generate manifest file

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC GENERATE symkink_format_manifest FOR TABLE delta.`<path-to-delta-table>`;

# COMMAND ----------

deltaTable = DeltaTable.forPath(<path-to-delta-table>)
deltaTable.generate("symkink_format_manifest")

# COMMAND ----------

# MAGIC %md
# MAGIC #4) Convert a Parquet table to a Delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Convert non partitioned parquet table at path '<path-to-table>'
# MAGIC CONVERT TO DELTA parquet.`<path-to-table>`;
# MAGIC
# MAGIC -- Convert partitioned Parquet table at path '<path-to-table>' and partitioned by integer columns named 'part' and 'part2'
# MAGIC CONVERT TO DELTA parquet.`<path-to-table>` PARTITIOned BY (part int, part2 int);

# COMMAND ----------

from delta.tables import *

# Convert non partitioned parquet table at path '<path-to-table>'
deltaTable = DeltaTable.convertToDelta(spark, "parquet.`<path-to-table>`")

# Convert partitioned parquet table at path '<path-to-table>' and partitioned by integer column named 'part'
partitionedDeltaTable = DeltaTable.convertToDelta(spark, "parquet.`<path-to-table>`", "part int")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 5) Convert a Delta table to a Parquet table

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC In case you need to convert a Delta table to parquet you can follow the following
# MAGIC steps:
# MAGIC 1. If you have performed Delta Lake operations that can change the data files (for
# MAGIC example, delete or merge), run VACUUM with a retention of 0 hours to delete all
# MAGIC data files that do not belong to the latest version of the table.
# MAGIC 2. Delete the _delta_log directory in the table directory.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #6) Restore a table version
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- restore version 9 of metastore defined Delta table
# MAGIC INSERT OVERWRITE INTO loan_by_state
# MAGIC SELECT * FROM loan_by_state VERSION AS OF 9;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC RESTORE TABLE loan_by_state TO VERSION AS OF 9;
# MAGIC RESTORE TABLE delta.`/ml/loan_by_state` TO TIMESTAMP AS OF 9;

# COMMAND ----------

from delta.tables import *

# path-based Delta tables
deltaTable = DeltaTable.forPath(spark, `/ml/loan_by_state/`)

# metastore-based tables
deltaTable = DeltaTable.forName(spark, `loan_by_state`)

# restore table to version 9
deltaTable.restoreToVersion(9)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #7) CLONE Command

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Shallow clone table
# MAGIC CREATE TABLE delta.`/some/test/location` SHALLOW CLONE prod.events

# COMMAND ----------

DeltaTable.forName("spark", "prod.events").clone("/some/test/location", isShallow=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create a shallow clone
# MAGIC CREATE TABLE temp.staged_changes SHALLOW CLONE prod.events;
# MAGIC
# MAGIC -- Test out deleting table from shallow clone table
# MAGIC DELETE FROM temp.staged_changes WHERE event_id IS NULL;
# MAGIC
# MAGIC -- Update shallow clone table
# MAGIC UPDATE temp.staged_changes SET change_date = current_date() WHERE change_date IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- If no changes have been made to the source
# MAGIC REPLACE TABLE prod.events CLONE temp.staged_changes;
# MAGIC
# MAGIC -- If the source table has changed
# MAGIC MERGE INTO prod.events USING temp.staged_changes
# MAGIC ON events.event_id <=> staged_changes.event_id
# MAGIC WHEN MATCHED THEN UPDATE SET;
# MAGIC
# MAGIC -- Drop the staged table
# MAGIC DROP TABLE temp.staged_changes;

# COMMAND ----------

# Run your ML workloads using Python and then
DeltaTable.forName(spark, "feature_store").cloneAtVersion(128, "feature_store_bf2020")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Data migration using deep clone
# MAGIC CREATE TABLE delta.`zz://my-new-bucket/events` CLONE prod.events;
# MAGIC ALTER TABLE prod.events SET LOCATION 'zz://my-new-bucket/events';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- The following code can be scheduled to run at your convenience for data sharing
# MAGIC CREATE OR REPLACE TABLE data_science.events CLONE prod.events;
# MAGIC
# MAGIC -- The following code can be scheduled to run at your convenience for data archiving
# MAGIC CREATE OR REPLACE TABLE archive.events CLONE prod.events;
