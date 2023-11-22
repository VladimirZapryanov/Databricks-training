# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture
# MAGIC

# COMMAND ----------

# MAGIC %run "/Apache Spark/apache-spark-programming-with-databricks-english/Includes/Classroom-Setup-SQL"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT name, price
# MAGIC FROM products
# MAGIC WHERE price < 200
# MAGIC ORDER BY price ASC;

# COMMAND ----------

 
display(spark
        .table('products')
        .select('name', 'price')
        .where('price < 200')
        .orderBy('price')
       )

# COMMAND ----------

products_df = spark.table('products')                                

# COMMAND ----------

result_df = spark.sql("""SELECT name, price
FROM products
WHERE price < 200
ORDER BY price
""")

display(result_df)

# COMMAND ----------

budget_df = (spark
             .table('products')
             .select('name', 'price')
             .where('price < 200')
             .orderBy('price')
            )

# COMMAND ----------

display(budget_df)

# COMMAND ----------

budget_df.schema

# COMMAND ----------

budget_df.printSchema()

# COMMAND ----------

(budget_df
 .select('name', 'price')
 .where('price < 200')
 .orderBy('price')
 .display()
 )

# COMMAND ----------

budget_df.count()

# COMMAND ----------

budget_df.collect()

# COMMAND ----------

budget_df.createOrReplaceTempView('budget')

# COMMAND ----------

display(spark.sql("SELECT * FROM budget"))

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md 
# MAGIC # Lab

# COMMAND ----------

events_df = (spark
             .table('events')
            )

# COMMAND ----------

display(events_df)

# COMMAND ----------

events_df.schema

# COMMAND ----------

events_df.printSchema()

# COMMAND ----------

events_macOS_df = (events_df
                   .where("device == 'macOS'")
                   .sort('event_timestamp')
                  ) 

display(events_macOS_df)

# COMMAND ----------

num_rows = events_macOS_df.count()
rows = events_macOS_df.take(5)

# COMMAND ----------

from pyspark.sql import Row

assert(num_rows == 1938215)
assert(len(rows) == 5)
assert(type(rows[0]) == Row)
print('All test pass')

# COMMAND ----------

sql_df = spark.sql("SELECT * FROM events")

display(sql_df)

# COMMAND ----------

mac_sql_df = spark.sql("""
SELECT * 
FROM events 
WHERE device = 'macOS' 
ORDER BY event_timestamp
""")

display(mac_sql_df)

# COMMAND ----------

verify_rows = mac_sql_df.take(5)
assert (mac_sql_df.select("device").distinct().count() == 1 and len(verify_rows) == 5 and verify_rows[0]['device'] == "macOS"), "Incorrect filter condition"
assert (verify_rows[4]['event_timestamp'] == 1592539226602157), "Incorrect sorting"
del verify_rows
print("All test pass")

# COMMAND ----------

DA.cleanup()

# COMMAND ----------


