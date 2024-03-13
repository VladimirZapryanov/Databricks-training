# Databricks notebook source
import getpass

# COMMAND ----------

username = getpass.getuser()

# COMMAND ----------

input_dir = '/FileStore/public/retail_db_json'
output_dir = f'/user/{username}/retail_db_parquet'

# COMMAND ----------

for file_details in dbutils.fs.ls(input_dir):
    if not ('.git' in file_details.path or file_details.path.endswith('.sql')):
        print(f'Converting data in {file_details.path} folder from json to parquet')

        data_set_dir = file_details.path.split('/')[-2]
        df = spark.read.json(file_details.path)
        df.coalesce(1).write.parquet(f'{output_dir}/{data_set_dir}', mode='overwrite')


# COMMAND ----------

dbutils.fs.ls(f'{output_dir}/orders')

# COMMAND ----------

orders = spark.read.parquet(f'{output_dir}/orders')

# COMMAND ----------

orders.dtypes

# COMMAND ----------

orders.show()

# COMMAND ----------


