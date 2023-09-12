-- Databricks notebook source
-- MAGIC %run "/Users/vladimir.zapryanov@dxc.com/Data Engineering with Databricks - v3.1.6/DE 2 - ETL with Spark/Includes/Classroom-Setup-02.6L"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def check_table_results(table_name, num_rows, column_names):
-- MAGIC     assert spark.table(table_name), f"Table named **`{table_name}`** does not exist"
-- MAGIC     assert set(spark.table(table_name).columns) == set(column_names), "Please name the columns as shown in the schema above"
-- MAGIC     assert spark.table(table_name).count() == num_rows, f"The table should have {num_rows} records"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("events_pivot", 204586, ['user', 'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium'])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("clickpaths", 9085, ['user', 'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium', 'user_id', 'order_id', 'transaction_timestamp', 'total_item_quantity', 'purchase_revenue_in_usd', 'unique_items', 'P_FOAM_K', 'M_STAN_Q', 'P_FOAM_S', 'M_PREM_Q', 'M_STAN_F', 'M_STAN_T', 'M_PREM_K', 'M_PREM_F', 'M_STAN_K', 'M_PREM_T', 'P_DOWN_S', 'P_DOWN_K'])

-- COMMAND ----------

-- MAGIC %run "/Users/vladimir.zapryanov@dxc.com/Data Engineering with Databricks - v3.1.6/DE 2 - ETL with Spark/Includes/Classroom-Setup-02.7A"

-- COMMAND ----------

CREATE OR REPLACE FUNCTION sale_announcement(item_name STRING, item_price INT)
RETURNS STRING
RETURN concat("The ", item_name, " is on sale for $", round(item_price * 0.8, 0));

SELECT *, sale_announcement(name, price) AS message FROM item_lookup

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED sale_announcement

-- COMMAND ----------

CREATE OR REPLACE FUNCTION item_preference(name STRING, price INT)
RETURNS STRING
RETURN CASE 
  WHEN name = "Standard Queen Mattress" THEN "This is my default mattress"
  WHEN name = "Premium Queen Mattress" THEN "This is my favorite mattress"
  WHEN price > 100 THEN concat("I'd wait until the ", name, " is on sale for $", round(price * 0.8, 0))
  ELSE concat("I don't need a ", name)
END;

SELECT *, item_preference(name, price) FROM item_lookup

-- COMMAND ----------

-- MAGIC %run "/Users/vladimir.zapryanov@dxc.com/Data Engineering with Databricks - v3.1.6/DE 2 - ETL with Spark/Includes/Classroom-Setup-02.7B"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sales_df = spark.table("sales")
-- MAGIC display(sales_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def first_letter_function(email):
-- MAGIC     return email[0]
-- MAGIC
-- MAGIC first_letter_function("annagray@kaufman.com")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC first_letter_udf = udf(first_letter_function)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC display(sales_df.select(first_letter_udf(col("email"))))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Our input/output is a string
-- MAGIC @udf("string")
-- MAGIC def first_letter_udf(email: str) -> str:
-- MAGIC     return email[0]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC sales_df = spark.table("sales")
-- MAGIC display(sales_df.select(first_letter_udf(col("email"))))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC from pyspark.sql.functions import pandas_udf
-- MAGIC
-- MAGIC # We have a string input/output
-- MAGIC @pandas_udf("string")
-- MAGIC def vectorized_udf(email: pd.Series) -> pd.Series:
-- MAGIC     return email.str[0]
-- MAGIC
-- MAGIC # Alternatively
-- MAGIC # def vectorized_udf(email: pd.Series) -> pd.Series:
-- MAGIC #     return email.str[0]
-- MAGIC # vectorized_udf = pandas_udf(vectorized_udf, "string")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(sales_df.select(vectorized_udf(col("email"))))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.udf.register("sql_vectorized_udf", vectorized_udf)

-- COMMAND ----------

-- Use the Pandas UDF from SQL
SELECT sql_vectorized_udf(email) AS firstLetter FROM sales

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------


