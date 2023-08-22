# Databricks notebook source
SELECT * 
FROM samples.nyctaxi.trips;

SELECT pickup_zip Zip_Code, sum(fare_amount) Fare_Amount
FROM samples.nyctaxi.trips
GROUP BY pickup_zip;


CREATE DATABASE IF NOT EXISTS hr_db
LOCATION 'dbfs:/user/hive/warehouse/hr_db.db';

USE hr_db;

CREATE TABLE employee (
  employee_id INT,
  name STRING,
  salary DOUBLE,
  city STRING
  );

INSERT INTO employee
VALUES
  (1, 'Ema', 2500, 'Miami'),
  (2, 'Ethan', 3000, 'Seattle'),
  (3, 'Olivia', 3500, 'Miami'),
  (4, 'Liam', 2000, 'Miami'),
  (5, 'Charlotte', 2500, 'Seattle'),
  (6, 'Noah', 3500, 'Seattle'),
  (7, 'Amelia', 3000, 'Miami');

CREATE VIEW hive_metastore.hr_db.miami_employee_vw
AS SELECT * FROM employee WHERE city = 'Miami';
