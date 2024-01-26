# Databricks notebook source
# MAGIC %run "/Apache Spark/apache-spark-programming-with-databricks-english/Includes/Classroom-Setup"

# COMMAND ----------

df = spark.read.format("delta").load(DA.paths.events)
df.rdd.getNumPartitions()

# COMMAND ----------

print(spark.sparkContext.defaultParallelism)

# COMMAND ----------

print(sc.defaultParallelism)

# COMMAND ----------

repartitioned_df = df.repartition(8)

# COMMAND ----------

repartitioned_df.rdd.getNumPartitions()

# COMMAND ----------

coalesce_df = df.coalesce(2)

# COMMAND ----------

coalesce_df.rdd.getNumPartitions()

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 4)
print(spark.conf.get("spark.sql.shuffle.partitions"))

# COMMAND ----------

spark.conf.get("spark.sql.adaptive.enabled")

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab

# COMMAND ----------

dbutils.fs.head(f"{DA.paths.datasets}/people/people-with-dups.txt")

# COMMAND ----------

source_file = f"{DA.paths.datasets}/people/people-with-dups.txt"
delta_dest_dir = f"{DA.paths.working_dir}/people"

# In case it already exists
dbutils.fs.rm(delta_dest_dir, True)

spark.conf.set("spark.sql.shuffle.partitions", 8)

df = (spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ":")
      .csv(source_file)
     )



# COMMAND ----------

from pyspark.sql.functions import col, lower, translate

deduped_df = (df
             .select(col("*"),
                     lower(col("firstName")).alias("lcFirstName"),
                     lower(col("lastName")).alias("lcLastName"),
                     lower(col("middleName")).alias("lcMiddleName"),
                     translate(col("ssn"), "-", "").alias("ssnNums")
                     # regexp_replace(col("ssn"), "-", "").alias("ssnNums")  # An alternate function to strip the hyphens
                     # regexp_replace(col("ssn"), """^(\d{3})(\d{2})(\d{4})$""", "$1-$2-$3").alias("ssnNums")  # An alternate that adds hyphens if missing
                    )
             .dropDuplicates(["lcFirstName", "lcMiddleName", "lcLastName", "ssnNums", "gender", "birthDate", "salary"])
             .drop("lcFirstName", "lcMiddleName", "lcLastName", "ssnNums")
            )

display(deduped_df)

# COMMAND ----------

# Now, write the results in Delta format as a single file. We'll also display the Delta files to make sure they were written as expected.

(deduped_df
 .repartition(1)
 .write
 .mode("overwrite")
 .format("delta")
 .save(delta_dest_dir)
)

display(dbutils.fs.ls(delta_dest_dir))

# COMMAND ----------

verify_files = dbutils.fs.ls(delta_dest_dir)
verify_delta_format = False
verify_num_data_files = 0
for f in verify_files:
    if f.name == "_delta_log/":
        verify_delta_format = True
    elif f.name.endswith(".parquet"):
        verify_num_data_files += 1

assert verify_delta_format, "Data not written in Delta format"
assert verify_num_data_files == 1, "Expected 1 data file written"

verify_record_count = spark.read.format("delta").load(delta_dest_dir).count()
assert verify_record_count == 100000, "Expected 100000 records in final result"

del verify_files, verify_delta_format, verify_num_data_files, verify_record_count
print("All test pass")

# COMMAND ----------

DA.cleanup()
