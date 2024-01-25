# Databricks notebook source
# MAGIC %md
# MAGIC #Lecture

# COMMAND ----------

# MAGIC %run "/Apache Spark/apache-spark-programming-with-databricks-english/Includes/Classroom-Setup"

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

sales_df = spark.read.format("delta").load(DA.paths.sales)
display(sales_df)

# COMMAND ----------

def first_letter_function(email):
    return email[0]

first_letter_function("annagray@kaufman.com")

# COMMAND ----------

first_letter_udf = udf(first_letter_function)

# COMMAND ----------

display(sales_df.select(first_letter_udf(col("email"))))

# COMMAND ----------

sales_df.createOrReplaceTempView("sales")

first_letter_udf = spark.udf.register("sql_udf", first_letter_function)

# COMMAND ----------

display(sales_df.select(first_letter_udf(col("email"))))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT sql_udf(email) AS first_letter 
# MAGIC FROM sales

# COMMAND ----------

@udf("string")
def first_letter_udf(email: str) -> str:
    return email[0]

# COMMAND ----------

sales_df = spark.read.format("delta").load(DA.paths.sales)
display(sales_df.select(first_letter_udf(col("email"))))

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf("string")
def vectorized_udf(email: pd.Series) -> pd.Series:
    return email.str[0]

# Alternatively
# def vectorized_udf(email: pd.Series) -> pd.Series:
#     return email.str[0]
# vectorized_udf = pandas_udf(vectorized_udf, "string")

# COMMAND ----------

display(sales_df.select(vectorized_udf(col("email"))))

# COMMAND ----------

spark.udf.register("sql_vectorized_udf", vectorized_udf)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use the Pandas UDF from SQL
# MAGIC SELECT sql_vectorized_udf(email) AS firstLetter FROM sales

# COMMAND ----------

# MAGIC %md
# MAGIC #Lab

# COMMAND ----------

from pyspark.sql.functions import approx_count_distinct, avg, col, date_format, to_date

df = (spark
      .read
      .format("delta")
      .load(DA.paths.events)
      .withColumn("ts", (col("event_timestamp") / 1e6).cast("timestamp"))
      .withColumn("date", to_date("ts"))
      .groupBy("date").agg(approx_count_distinct("user_id").alias("active_users"))
      .withColumn("day", date_format(col("date"), "E"))
      .groupBy("day").agg(avg(col("active_users")).alias("avg_users"))
     )

display(df)

# COMMAND ----------

def label_day_of_week(day: str) -> str:
    dow = {"Mon": "1", "Tue": "2", "Wed": "3", "Thu": "4",
           "Fri": "5", "Sat": "6", "Sun": "7"}
    return dow.get(day) + "-" + day

# COMMAND ----------

label_dow_udf = spark.udf.register("label_dow", label_day_of_week)

# COMMAND ----------

final_df = (df
            .withColumn("day", label_dow_udf(col("day")))
            .sort("day")
           )
           
display(final_df)

# COMMAND ----------

DA.cleanup()

# COMMAND ----------


