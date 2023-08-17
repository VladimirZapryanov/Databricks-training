# Databricks notebook source
token = 'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-08-19T21:25:34Z&st=2023-08-17T13:25:34Z&spr=https&sig=TOQam%2Bn99LOVDGhhmHn8cMnl9PuhbqF%2BIv%2F4fD%2BBaJ0%3D'

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.gen2storage1488.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.gen2storage1488.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.gen2storage1488.dfs.core.windows.net", token)

# COMMAND ----------

path = 'abfss://bronze@gen2storage1488.dfs.core.windows.net/bank_data.csv'
bank_data = spark.read.csv(path, header= True)
display(bank_data)

# COMMAND ----------

 
