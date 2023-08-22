# Databricks notebook source
# MAGIC %md
# MAGIC #1) Mount 3 containers

# COMMAND ----------

# Must use this one but do not have access !!!
# application_id = dbutils.secrets.get(scope='databricks-secrets', key='application-id')
# tenant_id = dbutils.secrets.get(scope='databricks-secrets', key='tenant-id')
# secret = dbutils.secrets.get(scope='databricks-secrets', key='secret')

application_id = 'ac8ce520-9037-43b5-a2de-030dc038a043'
tenant_id = '93f33571-550f-43cf-b09f-cd331338d086'
secret = 'PEG8Q~~TGJaErZQbjserlnPuNRtYfhea_VvCFcDc'

# COMMAND ----------

container_name = 'bronze'
account_name = 'gen2storage1488'
mount_point = '/mnt/bronze'

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f'{application_id}',
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)

# COMMAND ----------

container_name = 'silver'
account_name = 'gen2storage1488'
mount_point = '/mnt/silver'

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f'{application_id}',
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)

# COMMAND ----------

container_name = 'gold'
account_name = 'gen2storage1488'
mount_point = '/mnt/gold'

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f'{application_id}',
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC #2) Read the files from bronze container

# COMMAND ----------

bank_data_path = '/mnt/bronze/bank_data.csv'

from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType
bank_data_schema = StructType([
                    StructField("CustomerId", IntegerType()),
                    StructField("Surname", StringType()),
                    StructField("CreditScore", IntegerType()),
                    StructField("Geography", StringType()),
                    StructField("Gender", StringType()),
                    StructField("Age", IntegerType()),
                    StructField("Tenure", IntegerType()),
                    StructField("Balance", DoubleType()),
                    StructField("NumOfProducts", IntegerType()),
                    StructField("HasCrCard", IntegerType()),
                    StructField("IsActiveMember", IntegerType()),
                    StructField("EstimatedSalary", DoubleType()),
                    StructField("Exited", IntegerType())
                    ]
                    )

bank_data=spark.read.csv(path=bank_data_path, header=True, schema=bank_data_schema)

# COMMAND ----------

display(bank_data)

# COMMAND ----------

bank_data = bank_data.drop('Surname', 'Geography', 'Gender')

# COMMAND ----------

display(bank_data)

# COMMAND ----------

# MAGIC %md
# MAGIC #3) Write data in silver layer

# COMMAND ----------

bank_data.write.parquet('/mnt/silver/bank_data')

# COMMAND ----------

# MAGIC %md
# MAGIC #4) Write data in gold container

# COMMAND ----------

bank_data = spark.read.parquet('/mnt/silver/bank_data')

# COMMAND ----------

display(bank_data)

# COMMAND ----------

bank_data = bank_data[bank_data['Balance'] != 0]

# COMMAND ----------

display(bank_data)

# COMMAND ----------

bank_data.write.parquet('/mnt/gold/bank_data')

# COMMAND ----------

# MAGIC %md
# MAGIC #5) Unmaunt the 3 containers

# COMMAND ----------

dbutils.fs.unmount('/mnt/bronze')
dbutils.fs.unmount('/mnt/silver')
dbutils.fs.unmount('/mnt/gold')

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


