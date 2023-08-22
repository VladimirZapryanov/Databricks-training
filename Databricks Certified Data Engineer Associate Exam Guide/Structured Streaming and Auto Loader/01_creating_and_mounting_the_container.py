# Databricks notebook source
# MAGIC %md
# MAGIC #1)Mount container

# COMMAND ----------

# Must use this one but do not have access !!!
# application_id = dbutils.secrets.get(scope='databricks-secrets', key='application-id')
# tenant_id = dbutils.secrets.get(scope='databricks-secrets', key='tenant-id')
# secret = dbutils.secrets.get(scope='databricks-secrets', key='secret')

application_id = 'ac8ce520-9037-43b5-a2de-030dc038a043'
tenant_id = '93f33571-550f-43cf-b09f-cd331338d086'
secret = 'PEG8Q~~TGJaErZQbjserlnPuNRtYfhea_VvCFcDc'

# COMMAND ----------

container_name = 'streaming-demo'
account_name = 'gen2storage1488'
mount_point = '/mnt/streaming-demo'

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

display(spark.read.csv('/mnt/streaming-demo/full_dataset/bank_data.csv', header=True))

# COMMAND ----------


