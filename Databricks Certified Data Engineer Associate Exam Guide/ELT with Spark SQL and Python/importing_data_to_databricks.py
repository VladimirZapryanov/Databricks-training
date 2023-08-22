# Databricks notebook source
# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 'dbfs:/FileStore/'

# COMMAND ----------

import zipfile

zip_path = "/dbfs/FileStore/employee_details.zip"

with zipfile.ZipFile(zip_path, "r") as zip_ref:
    zip_ref.extractall("/dbfs/FileStore/extracted_files/")


# COMMAND ----------

# MAGIC %fs
# MAGIC ls "dbfs:/FileStore"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/extracted_files/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/extracted_files/employee_details/
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/extracted_files/employee_details/csv_files/

# COMMAND ----------


