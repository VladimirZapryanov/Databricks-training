# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture

# COMMAND ----------

import pandas as pd

# COMMAND ----------

data = [["John", 30, "Journalist"], ["Marry", 30, "Programmer"], ["Abe", 40, "Chef"]]

df = pd.DataFrame(data=data)
df

# COMMAND ----------

cols = ["Name", "Age", "Job"]
df = pd.DataFrame(data=data, columns=cols)
df

# COMMAND ----------

df['Age']

# COMMAND ----------

df.Age

# COMMAND ----------

df.dtypes

# COMMAND ----------

df['Age'] + df['Age']

# COMMAND ----------

df['Age'] * 3 - 1

# COMMAND ----------

df['Age'][0]

# COMMAND ----------

df[['Name', 'Age']]

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab

# COMMAND ----------

data = [["Buddy", 3, "Australian Shepherd"], ["Harley", 10, "Labrador"], ["Luna", 2, "Golden Retriever"], ["Bailey", 8, "Chihuahua"]]
column_name = ['Name', 'Age', 'Breed']

df = pd.DataFrame(data=data, columns=column_name)
df


# COMMAND ----------

assert (df.columns == ["Name", "Age", "Breed"]).all(), "The columns are named incorrectly"
assert [df.iloc[0][x] for x in ["Name", "Age", "Breed"]] == ["Buddy", 3, "Australian Shepherd"], "First row defined incorrectly"
assert [df.iloc[1][x] for x in ["Name", "Age", "Breed"]] == ["Harley", 10, "Labrador"], "Second row defined incorrectly"
assert [df.iloc[2][x] for x in ["Name", "Age", "Breed"]] == ["Luna", 2, "Golden Retriever"], "Third row defined incorrectly"
assert [df.iloc[3][x] for x in ["Name", "Age", "Breed"]] == ["Bailey", 8, "Chihuahua"], "Fourth row defined incorrectly"
print("Test passed!")

# COMMAND ----------

print(df.dtypes)

# COMMAND ----------

name_age_df = df[['Name', 'Age']]
name_age_df

# COMMAND ----------

assert (name_age_df.columns == ["Name", "Age"]).all(), "The columns are named incorrectly"
assert name_age_df.shape == (4, 2), "There are not the right number of rows or columns"
assert [name_age_df.iloc[0][x] for x in ["Name", "Age"]] == ["Buddy", 3], "First row defined incorrectly"
assert [name_age_df.iloc[1][x] for x in ["Name", "Age"]] == ["Harley", 10], "Second row defined incorrectly"
assert [name_age_df.iloc[2][x] for x in ["Name", "Age"]] == ["Luna", 2], "Third row defined incorrectly"
assert [name_age_df.iloc[3][x] for x in ["Name", "Age"]] == ["Bailey", 8], "Fourth row defined incorrectly"
print("Test passed!")

# COMMAND ----------

df['Human Age'] = df['Age'] * 7
df

# COMMAND ----------

assert df.shape == (4, 4), "There are not the correct number of rows or columns"
assert (df.columns == ["Name", "Age", "Breed", "Human Age"]).all(), "The columns are named incorrectly"
assert [df.iloc[0][x] for x in ["Name", "Age", "Breed", "Human Age"]] == ["Buddy", 3, "Australian Shepherd", 21], "First row defined incorrectly"
assert [df.iloc[1][x] for x in ["Name", "Age", "Breed", "Human Age"]] == ["Harley", 10, "Labrador", 70], "Second row defined incorrectly"
assert [df.iloc[2][x] for x in ["Name", "Age", "Breed", "Human Age"]] == ["Luna", 2, "Golden Retriever", 14], "Third row defined incorrectly"
assert [df.iloc[3][x] for x in ["Name", "Age", "Breed", "Human Age"]] == ["Bailey", 8, "Chihuahua", 56], "Fourth row defined incorrectly"
print("Test passed!")

# COMMAND ----------

breed = df['Breed'][0]
breed

# COMMAND ----------

assert breed == "Australian Shepherd", "Breed is not defined correctly"
print("Test passed!")

# COMMAND ----------


