# Databricks notebook source
import getpass

# COMMAND ----------

username = getpass.getuser()

input_dir = '/FileStore/public/retail_db'
output_dir = f'/user/{username}/retail_db_pipe'

# COMMAND ----------

dbutils.fs.ls('/FileStore/public/retail_db')

# COMMAND ----------

# Generate CSV files with pipe delimiter
for file_details in dbutils.fs.ls(input_dir):
    if 'git' not in file_details.path and 'sql' not in file_details.path and 'md' not in file_details.path:
        print(f'Converting data in {file_details.path} folder from comma separated to pipe separated')
        df = spark.read.csv(file_details.path)
        folder_name = file_details.path.split('/')[-2]
        df.coalesce(1).write.mode('overwrite').csv(f'{output_dir}/{folder_name}', sep='|')

# COMMAND ----------

schema = """
    order_id INT,
    order_date TIMESTAMP,
    order_customer_id INT,
    order_status STRING
"""

# COMMAND ----------

orders = spark.read.schema(schema).csv(f'{output_dir}/orders')

# COMMAND ----------

orders.show()

# COMMAND ----------

orders = spark.read.schema(schema).csv(f'{output_dir}/orders', sep='|')

# COMMAND ----------

orders.show()

# COMMAND ----------


