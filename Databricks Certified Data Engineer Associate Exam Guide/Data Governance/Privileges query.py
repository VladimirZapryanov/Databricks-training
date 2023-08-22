# Databricks notebook source
CREATE GROUP hr_team;

CREATE GROUP fn_team;


GRANT SELECT, MODIFY, READ_METADATA, CREATE ON SCHEMA hr_db TO hr_team;

GRANT USAGE ON SCHEMA hr_db TO hr_team;

GRANT SELECT ON VIEW hive_metastore.hr_db.miami_employee_vw TO fn_team;

SHOW GRANTS ON SCHEMA hr_db;

SHOW GRANTS ON VIEW hive_metastore.hr_db.miami_employee_vw;
