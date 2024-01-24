# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture

# COMMAND ----------

# MAGIC %run "/Apache Spark/apache-spark-programming-with-databricks-english/Includes/Classroom-Setup"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

sales_df = spark.read.format("delta").load(DA.paths.sales)
display(sales_df)

# COMMAND ----------

no_pii_df = sales_df.drop("email")
display(no_pii_df)

# COMMAND ----------

gmail_accounts = sales_df.filter(col("email").contains("gmail"))
display(gmail_accounts)

# COMMAND ----------

# MAGIC %md
# MAGIC #Lab

# COMMAND ----------

display(sales_df)

# COMMAND ----------

users_df = spark.read.format("delta").load(DA.paths.users)
display(users_df)

# COMMAND ----------

events_df = spark.read.format("delta").load(DA.paths.events)
display(events_df)

# COMMAND ----------

converted_users_df = (sales_df
                      .select("email")
                      .distinct()
                      .withColumn("converted", lit(True))
                     )

display(converted_users_df)

# COMMAND ----------

expected_columns = ["email", "converted"]

expected_count = 210370

assert converted_users_df.columns == expected_columns, "converted_users_df does not have the correct columns"

assert converted_users_df.count() == expected_count, "converted_users_df does not have the correct number of rows"

assert converted_users_df.select(col("converted")).first()[0] == True, "converted column not correct"
print("All test pass")

# COMMAND ----------

conversions_df = (users_df
                  .join(converted_users_df, "email", "outer")
                  .filter(col("email").isNotNull())
                  .na.fill(False)
                 )
                 
display(conversions_df)

# COMMAND ----------

expected_columns = ["email", "user_id", "user_first_touch_timestamp", "converted"]
expected_count = 782749
expected_false_count = 572379

assert conversions_df.columns == expected_columns, "Columns are not correct"
assert conversions_df.filter(col("email").isNull()).count() == 0, "Email column contains null"
assert conversions_df.count() == expected_count, "There is an incorrect number of rows"
assert conversions_df.filter(col("converted") == False).count() == expected_false_count, "There is an incorrect number of false entries in converted column"

print("All test pass")

# COMMAND ----------

carts_df = (events_df
            .withColumn("items", explode("items"))
            .groupBy("user_id")
            .agg(collect_set("items.item_id").alias("cart"))
)

display(carts_df)

# COMMAND ----------

expected_columns = ["user_id", "cart"]
expected_count = 488403

assert carts_df.columns == expected_columns, "Incorrect columns"
assert carts_df.count() == expected_count, "Incorrect number of rows"
assert carts_df.select(col("user_id")).drop_duplicates().count() == expected_count, "Duplicate user_ids present"

print("All test pass")

# COMMAND ----------


email_carts_df = conversions_df.join(carts_df, "user_id", "left_outer")
display(email_carts_df)

# COMMAND ----------

expected_columns = ["user_id", "email", "user_first_touch_timestamp", "converted", "cart"]
expected_count = 782749
expected_cart_null_count = 397799

assert email_carts_df.columns == expected_columns, "Columns do not match"
assert email_carts_df.count() == expected_count, "Counts do not match"
assert email_carts_df.filter(col("cart").isNull()).count() == expected_cart_null_count, "Cart null counts incorrect from join"

print("All test pass")

# COMMAND ----------

abandoned_carts_df = (email_carts_df
                      .filter(col("converted").contains(False))
                      .filter(col("cart").isNotNull())
)

display(abandoned_carts_df)

# COMMAND ----------

expected_columns = ["user_id", "email", "user_first_touch_timestamp", "converted", "cart"]
expected_count = 204272

assert abandoned_carts_df.columns == expected_columns, "Columns do not match"
assert abandoned_carts_df.count() == expected_count, "Counts do not match"

print("All test pass")

# COMMAND ----------

abandoned_items_df = (abandoned_carts_df
                      .withColumn("items", explode("cart"))
                      .groupBy("items")
                      .count()
                      .sort("items")
                     )
display(abandoned_items_df)

# COMMAND ----------

expected_columns = ["items", "count"]
expected_count = 12

assert abandoned_items_df.count() == expected_count, "Counts do not match"
assert abandoned_items_df.columns == expected_columns, "Columns do not match"

print("All test pass")

# COMMAND ----------

DA.cleanup()

# COMMAND ----------


