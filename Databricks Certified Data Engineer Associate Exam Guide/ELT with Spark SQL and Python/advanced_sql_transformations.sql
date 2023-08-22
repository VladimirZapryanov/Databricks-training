-- Databricks notebook source
-- MAGIC %md
-- MAGIC #1) Importing the data

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls dbfs:/FileStore/

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls dbfs:/FileStore/client_details_extracted/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import zipfile
-- MAGIC
-- MAGIC zip_path = "/dbfs/FileStore/client_details.zip"
-- MAGIC
-- MAGIC with zipfile.ZipFile(zip_path, "r") as zip_ref:
-- MAGIC     zip_ref.extractall("/dbfs/FileStore/client_details_extracted/")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #2) Create a table

-- COMMAND ----------

CREATE TABLE client_details AS
SELECT * FROM JSON.`${dataset.client_details}/FileStore/client_details_extracted/client_details/client_details.json`;

-- COMMAND ----------

SELECT *
FROM client_details;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #3) Use ':' syntax to query nested data

-- COMMAND ----------

SELECT client_id, details:first_name, details:address:city
FROM client_details

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #4) Struct types

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_client AS 
SELECT client_id, from_json(details, schema_of_json('{"first_name":"Chauncey","last_name":"Airey","gender":"Male","address":{"street":"039 Daystar Terrace","city":"Kumba","country":"Cameroon"}}')) details_struct
FROM client_details;


-- COMMAND ----------

SELECT *
FROM parsed_client;

-- COMMAND ----------

DESCRIBE EXTENDED parsed_client;

-- COMMAND ----------

SELECT client_id, details_struct.first_name, details_struct.address.city
FROM parsed_client;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW client_new AS
SELECT client_id, details_struct.*
FROM parsed_client;

-- COMMAND ----------

SELECT * 
FROM client_new;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW client_new_2 AS
SELECT client_id, address.*
FROM client_new;

-- COMMAND ----------

SELECT *
FROM client_new_2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #5) Working with Arrays

-- COMMAND ----------

CREATE TABLE my_table(
  id INT,
  my_array ARRAY<STRUCT<name: STRING, age: INT>>
)

-- COMMAND ----------

INSERT INTO my_table
VALUES
  (1, array(named_struct('name', 'John', 'age', 30))),
  (2, array(named_struct('name', 'Bob', 'age', 45), named_struct('name', 'Alice', 'age', 50))),
  (3, array(named_struct('name', 'Tom', 'age', 35), named_struct('name', 'Sue', 'age', 28), named_struct('name', 'Jane', 'age', 25)))

-- COMMAND ----------

SELECT *
FROM my_table;

-- COMMAND ----------

SELECT id, my_array
FROm my_table
WHERE size(my_array) > 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #6) Explode function

-- COMMAND ----------

SELECT *
FROM my_table;

-- COMMAND ----------

SELECT id, explode(my_array) as new_array 
FROM my_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #7) Collect Array
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###a) collect_set function

-- COMMAND ----------

CREATE TABLE grocery_items (
  item_id INT,
  item_name STRING,
  category STRING
);

-- COMMAND ----------

INSERT INTO grocery_items
VALUES
  (1, 'Bananas', 'Fruits'),
  (2, 'Apples', 'Fruits'),
  (3, 'Oranges', 'Fruits'),
  (4, 'Carrots', 'Vegetables'),
  (5, 'Broccoli', 'Vegetables'),
  (6, 'Spinach', 'Vegetables'),
  (7, 'Lamb', 'Meat'),
  (8, 'Chicken', 'Meat'),
  (9, 'Salmon', 'Seafood'),
  (10, 'Shrimp', 'Seafood');

-- COMMAND ----------

SELECT *
FROM grocery_items;

-- COMMAND ----------

SELECT category, collect_set(item_name) as item_name_new
FROM grocery_items
GROUP BY category;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###b) flatten function

-- COMMAND ----------

CREATE TABLE people (
  name STRING,
  age INT,
  addresses ARRAY<ARRAY<STRING>>
);

-- COMMAND ----------

INSERT INTO people 
VALUES
  ('Alice', 25, array(array('123 Main st', 'San Francisko', 'CA', '12345'), array('456 Pine st', 'San Jose', 'CA', '67890'))),
  ('Bob', 30, array(array('789 Oak st', 'Los Angeles', 'CA', '45678')))

-- COMMAND ----------

SELECT * 
FROM people;

-- COMMAND ----------

SELECT name, age, flatten(addresses) as address_flat
FROM people;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###c) array_distinct function

-- COMMAND ----------

CREATE TABLE cars (
  name STRING,
  age INT,
  brand ARRAY<ARRAY<STRING>>
);

INSERT INTO cars 
VALUES
  ('Alice', 25, ARRAY(ARRAY('Ford', 'BMW'), ARRAY('Tesla', 'Ford'))),
  ('Bob', 30, ARRAY(ARRAY('BMW', 'Mercedes-Benz'), ARRAY('Audi', 'Porshe'))),
  ('Carol', 35, ARRAY(ARRAY('Audi', 'Porshe'), ARRAY('Ford', 'Audi'))),
  ('David', 40, ARRAY(ARRAY('BMW', 'Mercedes'), ARRAY('Audi', 'Porshe')));

-- COMMAND ----------

SELECT *
FROM cars;

-- COMMAND ----------

SELECT name, age, array_distinct(flatten(brand)) AS distinct_brands
FROM cars;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #8) Join operations

-- COMMAND ----------

CREATE TEMP VIEW employee(id, name, deptno) AS
VALUES
  (105, 'Chloe', 5),
  (103, 'Paul', 3),
  (101, 'John', 1),
  (102, 'Lisa', 2),
  (104, 'Evan', 4),
  (106, 'Amy', 6);

-- COMMAND ----------

SELECT * 
FROM employee;

-- COMMAND ----------

CREATE TEMP VIEW department(deptno, deptname) AS 
VALUES
  (3, 'Engineering'),
  (2, 'Sales'),
  (1, 'Marketing');

-- COMMAND ----------

SELECT *
FROM department;

-- COMMAND ----------

SELECT id, name, employee.deptno, deptname
FROM employee
INNER JOIN department 
ON employee.deptno = department.deptno;

-- COMMAND ----------

SELECT id, name, employee.deptno, deptname
FROM employee
LEFT JOIN department 
ON employee.deptno = department.deptno;

-- COMMAND ----------

SELECT id, name, employee.deptno, deptname
FROM employee
RIGHT JOIN department 
ON employee.deptno = department.deptno;

-- COMMAND ----------

SELECT id, name, employee.deptno, deptname
FROM employee
FULL JOIN department 
ON employee.deptno = department.deptno;

-- COMMAND ----------

SELECT id, name, employee.deptno, deptname
FROM employee
CROSS JOIN department;

-- COMMAND ----------

SELECT *
FROM employee
SEMI JOIN department 
ON employee.deptno = department.deptno;

-- COMMAND ----------

SELECT *
FROM employee
ANTI JOIN department 
ON employee.deptno = department.deptno;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #9) Set operators

-- COMMAND ----------

CREATE TEMP VIEW number1(c) AS 
VALUES
  (1),
  (2),
  (2),
  (3),
  (3),
  (4),
  (5),
  (6),
  (7),
  (8),
  (9),
  (10);

-- COMMAND ----------

CREATE TEMP VIEW number2(c) AS 
VALUES
  (1),
  (2),
  (3),
  (4),
  (4),
  (5),
  (5);

-- COMMAND ----------

SELECT *
FROM number1;

-- COMMAND ----------

SELECT *
FROM number2;

-- COMMAND ----------

SELECT c FROM number1 EXCEPT SELECT c FROM number2;

-- COMMAND ----------

SELECT c FROM number1 MINUS SELECT c FROM number2;

-- COMMAND ----------

SELECT c FROM number1 EXCEPT ALL (SELECT c FROM number2);

-- COMMAND ----------

SELECT c FROM number1 INTERSECT SELECT c FROM number2;

-- COMMAND ----------

SELECT c FROM number1 INTERSECT DISTINCT SELECT c FROM number2;

-- COMMAND ----------

SELECT c FROM number1 INTERSECT ALL SELECT c FROM number2;

-- COMMAND ----------

SELECT c FROM number1 UNION SELECT c FROM number2;

-- COMMAND ----------

SELECT c FROM number1 UNION ALL SELECT c FROM number2;

-- COMMAND ----------

SELECT c FROM number1 UNION DISTINCT SELECT c FROM number2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 10) Pivot Clause

-- COMMAND ----------

CREATE TEMP VIEW sales(year, quarter, region, sales) AS 
VALUES
  (2018, 1, 'east', 100),
  (2018, 2, 'east', 20),
  (2018, 3, 'east', 40),
  (2018, 4, 'east', 40),
  (2019, 1, 'east', 120),
  (2019, 2, 'east', 110),
  (2019, 3, 'east', 80),
  (2019, 4, 'east', 60),
  (2018, 1, 'west', 105),
  (2018, 2, 'west', 25),
  (2018, 3, 'west', 45),
  (2018, 4, 'west', 45),
  (2019, 1, 'west', 125),
  (2019, 2, 'west', 115),
  (2019, 3, 'west', 85),
  (2019, 4, 'west', 65);

-- COMMAND ----------

SELECT *
FROM sales;

-- COMMAND ----------

SELECT year, region, q1, q2, q3, q4
FROM sales 
PIVOT (sum(sales) AS sales
FOR quarter
IN (1 AS q1, 2 AS q2, 3 AS q3, 4 AS q4));

-- COMMAND ----------


