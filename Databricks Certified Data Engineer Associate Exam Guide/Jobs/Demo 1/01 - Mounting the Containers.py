# Databricks notebook source
# Must use this one but do not have access !!!
# application_id = dbutils.secrets.get(scope='databricks-secrets', key='application-id')
# tenant_id = dbutils.secrets.get(scope='databricks-secrets', key='tenant-id')
# secret = dbutils.secrets.get(scope='databricks-secrets', key='secret')

application_id = 'ac8ce520-9037-43b5-a2de-030dc038a043'
tenant_id = '93f33571-550f-43cf-b09f-cd331338d086'
secret = 'PEG8Q~~TGJaErZQbjserlnPuNRtYfhea_VvCFcDc'

# COMMAND ----------

container_name = "bronze"
account_name = "gen2storage1488"
mount_point = "/mnt/bronze"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
 
dbutils.fs.mount(
  source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)

# COMMAND ----------

container_name = "silver"
mount_point = "/mnt/silver"

# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)

# COMMAND ----------

container_name = "gold"
mount_point = "/mnt/gold"

# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)

# COMMAND ----------

# Display mounts
display(dbutils.fs.mounts())
