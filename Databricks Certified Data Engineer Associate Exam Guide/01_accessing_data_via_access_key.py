# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.gen2storage1488.dfs.core.windows.net",
    "Sn6mmI7HN8wvVZBGWpqwYapxGhxUZqrbUgIbZr7trDhtJVni/ZGT/8AbYTn7u0Q6FQXUJgm5hdNZ+AStYhNh7g==")

path = 'abfss://bronze@gen2storage1488.dfs.core.windows.net/bank_data.csv'

bank_data = spark.read.csv(path, header= True)

display(bank_data)

# COMMAND ----------


