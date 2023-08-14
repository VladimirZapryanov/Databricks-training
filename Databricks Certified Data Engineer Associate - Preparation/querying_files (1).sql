-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC files = dbutils.fs.ls("dbfs:/mnt/demo-datasets/bookstore/customers-json")
-- MAGIC display(files);

-- COMMAND ----------

SELECT * FROM json.`dbfs:/mnt/demo-datasets/bookstore/customers-json`;

-- COMMAND ----------

SELECT *,
  input_file_name() source_file
  FROM json.`dbfs:/mnt/demo-datasets/bookstore/customers-json`;

-- COMMAND ----------

SELECT * FROM text.`dbfs:/mnt/demo-datasets/bookstore/customers-json`;

-- COMMAND ----------

SELECT *
FROM binaryFile.`dbfs:/mnt/demo-datasets/bookstore/customers-json`;

-- COMMAND ----------

SELECT *
FROM csv.`dbfs:/mnt/demo-datasets/bookstore/books-csv`;

-- COMMAND ----------

CREATE TABLE books_csv (
  book_id STRING,
  title STRING,
  author STRING,
  category STRING,
  price DOUBLE
)
USING CSV
OPTIONS (
  header = "true",
  delimiter = ";"
)
LOCATION "dbfs:/mnt/demo-datasets/bookstore/books-csv";

-- COMMAND ----------

SELECT * 
FROM books_csv;


-- COMMAND ----------

DESCRIBE EXTENDED books_csv;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC files = dbutils.fs.ls("dbfs:/mnt/demo-datasets/bookstore/books-csv")
-- MAGIC display(files);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC (spark.read
-- MAGIC  .table("books_csv")
-- MAGIC  .write
-- MAGIC  .mode("append")
-- MAGIC  .format("csv")
-- MAGIC  .option('header', 'true')
-- MAGIC  .option('delimiter', ';')
-- MAGIC  .save("dbfs:/mnt/demo-datasets/bookstore/books-csv"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC files = dbutils.fs.ls("dbfs:/mnt/demo-datasets/bookstore/books-csv")
-- MAGIC display(files);

-- COMMAND ----------

REFRESH TABLE books_csv;

-- COMMAND ----------

SELECT COUNT(*) FROM books_csv;

-- COMMAND ----------

CREATE TABLE customers AS SELECT * 
FROM json.`dbfs:/mnt/demo-datasets/bookstore/customers-json`;

DESCRIBE EXTENDED customers;

-- COMMAND ----------


