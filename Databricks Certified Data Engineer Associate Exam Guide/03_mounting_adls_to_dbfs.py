# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "<application-id>",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="<scope-name>",key="<service-credential-key-name>"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.unmount("/mnt/<mount-name>")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC #1) App registrations details
# MAGIC

# COMMAND ----------

application_id = 'ac8ce520-9037-43b5-a2de-030dc038a043'
tenant_id = '93f33571-550f-43cf-b09f-cd331338d086'
secret = 'PEG8Q~~TGJaErZQbjserlnPuNRtYfhea_VvCFcDc'

# COMMAND ----------

# MAGIC %md
# MAGIC #2) Service app permission

# COMMAND ----------

# MAGIC %md
# MAGIC #3) Mounting ADLS to DBFS

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": 'ac8ce520-9037-43b5-a2de-030dc038a043',
          "fs.azure.account.oauth2.client.secret": 'PEG8Q~~TGJaErZQbjserlnPuNRtYfhea_VvCFcDc',
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/93f33571-550f-43cf-b09f-cd331338d086/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://bronze@gen2storage1488.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

bank_data = spark.read.csv('/mnt/bronze/bank_data.csv', header= True)

# COMMAND ----------

display(bank_data)

# COMMAND ----------

dbutils.fs.unmount("/mnt/bronze")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


