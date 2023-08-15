-- Databricks notebook source
CREATE TABLE IF NOT EXISTS cars(
  car_id INT,
  name STRING,
  brand STRING,
  launch_year INT
);

-- COMMAND ----------

INSERT INTO cars
VALUES
  (1, "Model T", "Ford", 1908),
  (2, "Mustang", "Ford", 1964),
  (3, "Thunderbirtd", "Ford", 1955),
  (4, "Falcon", "Ford", 1960),
  (5, "Corvette", "Chervolet", 1953),
  (6, "Camaro", "Chervolet", 1966),
  (7, "Impala", "Chervolet", 1958),
  (8, "S-Class", "Mercedes", 1954),
  (9, "E-Class", "Mercedes", 1984),
  (10, "C-Class", "Mercedes", 1993);

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DROP TABLE author_counts;
DROP TABLE books_csv;
DROP TABLE customers;
DROP TABLE daily_customer_books;
DROP TABLE orders;
DROP TABLE orders_bronze;
DROP TABLE orders_silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create a stored view

-- COMMAND ----------

CREATE VIEW view_ford_cars
AS SELECT *
FROM cars
WHERE brand = 'Ford';

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT * 
FROM view_ford_cars;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create a temporary view

-- COMMAND ----------

CREATE TEMP VIEW temp_view_car_brands
AS SELECT DISTINCT brand
FROM cars;

-- COMMAND ----------

SELECT * 
FROM temp_view_car_brands;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE TEMP VIEW view_mercedes_cars
AS SELECT *
FROM cars
WHERE brand = 'Mercedes';

-- COMMAND ----------

SELECT *
FROM view_mercedes_cars;

-- COMMAND ----------

SELECT *
FROM view_mercedes_cars
WHERE name = "E-Class";

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create a global view

-- COMMAND ----------

CREATE GLOBAL TEMPORARY VIEW global_temp_view_after60s_cars
AS SELECT * FROM cars
WHERE launch_year > 1960
ORDER BY launch_year DESC;

-- COMMAND ----------

SELECT *
FROM global_temp.global_temp_view_after60s_cars;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------


