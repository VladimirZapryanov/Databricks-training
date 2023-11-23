# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture

# COMMAND ----------

# MAGIC %run "/Apache Spark/apache-spark-programming-with-databricks-english/Includes/Classroom-Setup"

# COMMAND ----------

users_csv_path = f"{DA.paths.datasets}/ecommerce/users/users-500k.csv"

users_df = (spark
           .read
           .option("sep", "\t")
           .option("header", True)
           .option("inferSchema", True)
           .csv(users_csv_path)
          )

users_df.printSchema()

# COMMAND ----------

users_df = (spark
           .read
           .csv(users_csv_path, sep="\t", header=True, inferSchema=True)
          )

users_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import LongType, StringType, StructType, StructField

user_defined_schema = StructType([
    StructField('user_id', StringType(), True),
    StructField('user_first_touch_timestamp', LongType(), True),
    StructField('email', StringType(), True)
])

# COMMAND ----------

user_df = (spark
            .read   
            .option('sep', '\t')
            .option('header', True)
            .schema(user_defined_schema)
            .csv(users_csv_path)
          )

# COMMAND ----------

ddl_schema = 'user_id string, user_first_touch_timestamp long, email string'

users_df = (spark
            .read   
            .option('sep', '\t')
            .option('header', True)
            .schema(ddl_schema)
            .csv(users_csv_path)
          )

# COMMAND ----------

events_json_path = f"{DA.paths.datasets}/ecommerce/events/events-500k.json"

events_df = (spark
            .read
            .option("inferSchema", True)
            .json(events_json_path)
           )

events_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import ArrayType, DoubleType, IntegerType, LongType, StringType, StructType, StructField

user_defined_schema = StructType([
    StructField("device", StringType(), True),
    StructField("ecommerce", StructType([
        StructField("purchaseRevenue", DoubleType(), True),
        StructField("total_item_quantity", LongType(), True),
        StructField("unique_items", LongType(), True)
    ]), True),
    StructField("event_name", StringType(), True),
    StructField("event_previous_timestamp", LongType(), True),
    StructField("event_timestamp", LongType(), True),
    StructField("geo", StructType([
        StructField("city", StringType(), True),
        StructField("state", StringType(), True)
    ]), True),
    StructField("items", ArrayType(
        StructType([
            StructField("coupon", StringType(), True),
            StructField("item_id", StringType(), True),
            StructField("item_name", StringType(), True),
            StructField("item_revenue_in_usd", DoubleType(), True),
            StructField("price_in_usd", DoubleType(), True),
            StructField("quantity", LongType(), True)
        ])
    ), True),
    StructField("traffic_source", StringType(), True),
    StructField("user_first_touch_timestamp", LongType(), True),
    StructField("user_id", StringType(), True)
])

events_df = (spark
            .read
            .schema(user_defined_schema)
            .json(events_json_path)
           )

# COMMAND ----------

# Step 1 - use this trick to transfer a value (the dataset path) between Python and Scala using the shared spark-config
spark.conf.set("whatever_your_scope.events", events_json_path)

# COMMAND ----------

# MAGIC %scala
# MAGIC // Step 2 - pull the value from the config (or copy & paste it)
# MAGIC val eventsJsonPath = spark.conf.get("whatever_your_scope.events")
# MAGIC
# MAGIC // Step 3 - Read in the JSON, but let it infer the schema
# MAGIC val eventsSchema = spark.read
# MAGIC                         .option("inferSchema", true)
# MAGIC                         .json(eventsJsonPath)
# MAGIC                         .schema.toDDL
# MAGIC
# MAGIC // Step 4 - print the schema, select it, and copy it.
# MAGIC println("="*80)
# MAGIC println(eventsSchema)
# MAGIC println("="*80)

# COMMAND ----------

# Step 5 - paste the schema from above and assign it to a variable as seen here
events_schema = "`device` STRING,`ecommerce` STRUCT<`purchase_revenue_in_usd`: DOUBLE, `total_item_quantity`: BIGINT, `unique_items`: BIGINT>,`event_name` STRING,`event_previous_timestamp` BIGINT,`event_timestamp` BIGINT,`geo` STRUCT<`city`: STRING, `state`: STRING>,`items` ARRAY<STRUCT<`coupon`: STRING, `item_id`: STRING, `item_name`: STRING, `item_revenue_in_usd`: DOUBLE, `price_in_usd`: DOUBLE, `quantity`: BIGINT>>,`traffic_source` STRING,`user_first_touch_timestamp` BIGINT,`user_id` STRING"

# Step 6 - Read in the JSON data using our new DDL formatted string
events_df = (spark.read
                 .schema(events_schema)
                 .json(events_json_path))

display(events_df)

# COMMAND ----------

users_output_dir = f"{DA.paths.working_dir}/users.parquet"

(users_df
 .write
 .option("compression", "snappy")
 .mode("overwrite")
 .parquet(users_output_dir)
)

# COMMAND ----------

display(
    dbutils.fs.ls(users_output_dir)
)

# COMMAND ----------

(users_df
 .write
 .parquet(users_output_dir, compression="snappy", mode="overwrite")
)

# COMMAND ----------

events_df.write.mode('overwrite').saveAsTable('events')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM events

# COMMAND ----------

print(f"Database Name: {DA.schema_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN ${DA.schema_name}

# COMMAND ----------

events_output_path = f"{DA.paths.working_dir}/delta/events"

(events_df
 .write
 .format("delta")
 .mode("overwrite")
 .save(events_output_path)
)

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab

# COMMAND ----------

single_product_csv_file_path = f"{DA.paths.datasets}/products/products.csv/part-00000-tid-1663954264736839188-daf30e86-5967-4173-b9ae-d1481d3506db-2367-1-c000.csv"
print(dbutils.fs.head(single_product_csv_file_path))

products_csv_path = f"{DA.paths.datasets}/products/products.csv"
products_df = (spark
           .read
           .csv(products_csv_path, header=True, inferSchema=True)
          )

products_df.printSchema()

# COMMAND ----------

assert(products_df.count() == 12)
print("All test pass")

# COMMAND ----------

from pyspark.sql.types import DoubleType, StringType, StructType, StructField

user_defined_schema = StructType([
    StructField('item_id', StringType(), True),
    StructField('name', StringType(), True),
    StructField('price', DoubleType(), True)
])

products_df2 = (spark
                .read
                .option('header', True)
                .schema(user_defined_schema)
                .csv(products_csv_path)
                )

# COMMAND ----------

assert(user_defined_schema.fieldNames() == ["item_id", "name", "price"])
print("All test pass")

# COMMAND ----------

from pyspark.sql import Row

expected1 = Row(item_id="M_STAN_Q", name="Standard Queen Mattress", price=1045.0)
result1 = products_df2.first()

assert(expected1 == result1)
print("All test pass")

# COMMAND ----------

ddl_schema = 'item_id string, name string, price double'

products_df3 = (spark
                .read
                .option('header', True)
                .schema(ddl_schema)
                .csv(products_csv_path)
                )

# COMMAND ----------

assert(products_df3.count() == 12)
print("All test pass")


# COMMAND ----------

products_output_path = f"{DA.paths.working_dir}/delta/products"
(products_df
        .write
        .format("delta")
        .mode("overwrite")
        .save(products_output_path)
)

# COMMAND ----------

verify_files = dbutils.fs.ls(products_output_path)
verify_delta_format = False
verify_num_data_files = 0
for f in verify_files:
    if f.name == "_delta_log/":
        verify_delta_format = True
    elif f.name.endswith(".parquet"):
        verify_num_data_files += 1

assert verify_delta_format, "Data not written in Delta format"
assert verify_num_data_files > 0, "No data written"
del verify_files, verify_delta_format, verify_num_data_files
print("All test pass")

# COMMAND ----------

DA.cleanup()

# COMMAND ----------


