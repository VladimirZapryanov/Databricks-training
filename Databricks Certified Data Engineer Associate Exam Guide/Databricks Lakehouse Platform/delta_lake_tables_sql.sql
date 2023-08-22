-- Databricks notebook source
CREATE TABLE Employee(
  employeeId INT,
  name STRING,
  title String,
  salary DOUBLE
);

-- COMMAND ----------

INSERT INTO employee
VALUES
  (1, "Emily", "Manager", 90000.00),
  (2, "John", "Salesman", 80000.00),
  (3, "Vinny", "Business Analyst", 120000.00),
  (4, "Tom", "Trainee", 25000.00),
  (5, "Emma", "HR Associate", 50000.00),
  (6, "James", "Trainee", 25000.00);

-- COMMAND ----------

SELECT *
FROM employee;

-- COMMAND ----------

DESCRIBE DETAIL employee;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/employee'

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/employee/_delta_log/'

-- COMMAND ----------

 UPDATE employee
 SET salary = salary + 5000
 WHERE title = "Trainee";

-- COMMAND ----------

SELECT *
FROM employee;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/employee'

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/employee/_delta_log/'

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC head dbfs:/user/hive/warehouse/employee/_delta_log/00000000000000000004.json

-- COMMAND ----------

DESCRIBE DETAIL employee;

-- COMMAND ----------

DESCRIBE HISTORY employee;
