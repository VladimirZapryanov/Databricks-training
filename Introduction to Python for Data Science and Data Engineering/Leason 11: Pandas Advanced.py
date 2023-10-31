# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture

# COMMAND ----------

# MAGIC %run "/Apache Spark/introduction-to-python-for-data-science-and-data-engineering-v1.1.3-notebooks/Includes/Classroom-Setup"

# COMMAND ----------

import pandas as pd

# COMMAND ----------

file_path = f"{datasets_dir}/sf-airbnb/sf-airbnb.csv".replace("dbfs", "/dbfs")
df = pd.read_csv(file_path)

# COMMAND ----------

df.head(3)

# COMMAND ----------

df.tail(3)

# COMMAND ----------

df = df.rename(columns={"neighbourhood": "neighborhood"})
df.head(3)

# COMMAND ----------

filtered_df = df[df["host_is_superhost"] == "t"]
filtered_df.head(3)

# COMMAND ----------

filtered_df = df[(df["host_is_superhost"] == "t") & (df["number_of_reviews"] >= 150)]
filtered_df.head(3)

# COMMAND ----------

print(df["number_of_reviews"].mean())
print(df["number_of_reviews"].min())
print(df["number_of_reviews"].max())

# COMMAND ----------

df["number_of_reviews"].describe()

# COMMAND ----------

df[["number_of_reviews", "host_listings_count", "bedrooms"]].describe()

# COMMAND ----------

df[["number_of_reviews", "host_listings_count", "bedrooms"]].describe().round(2)

# COMMAND ----------

df.groupby(["neighborhood"]).mean().head(10)[["bedrooms"]]

# COMMAND ----------

grouped_df = df.groupby(["neighborhood"]) [["bedrooms"]].mean().head(10)
grouped_df

# COMMAND ----------

reset_df = grouped_df.reset_index()
reset_df

# COMMAND ----------

df.sort_values(["bedrooms"]).head(3)

# COMMAND ----------

df['bedrooms'].sort_values()

# COMMAND ----------

df['bedrooms'].sort_values(ascending=False)

# COMMAND ----------

nan_df = df[['security_deposit', 'notes']]

# COMMAND ----------

nan_df.isna().sum()

# COMMAND ----------

nan_df.dropna()

# COMMAND ----------

nan_df.fillna("missing")

# COMMAND ----------

nan_df.fillna({'security_deposit': '$0.00', 'notes': 'Missing'}, inplace=False)

# COMMAND ----------

write_path = working_dir + '/example.csv'
dbutils.fs.rm(write_path)
df.to_csv(write_path.replace('dbfs:', '/dbfs'), index=False)

# COMMAND ----------

load_df = pd.read_csv(write_path.replace('dbfs:', '/dbfs'))
load_df.head()
