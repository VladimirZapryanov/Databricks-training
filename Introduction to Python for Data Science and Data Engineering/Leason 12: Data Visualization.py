# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture

# COMMAND ----------

# MAGIC %run "/Apache Spark/introduction-to-python-for-data-science-and-data-engineering-v1.1.3-notebooks/Includes/Classroom-Setup"

# COMMAND ----------

import pandas as pd

# COMMAND ----------

file_path = f"{datasets_dir}/sf-airbnb/sf-airbnb.csv".replace("dbfs:", "/dbfs")
df = pd.read_csv(file_path)
df.head(3)

# COMMAND ----------

display(df)

# COMMAND ----------

df["bedrooms"].hist()

# COMMAND ----------

df["bedrooms"].hist(bins=20)

# COMMAND ----------

df.boxplot(["bedrooms", "bathrooms"])

# COMMAND ----------

import seaborn as sns

# COMMAND ----------

sns.scatterplot(data=df, x="bedrooms", y="bathrooms")

# COMMAND ----------

sns.regplot(data=df, x="bedrooms", y="bathrooms")
