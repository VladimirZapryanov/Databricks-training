# Databricks notebook source
# MAGIC %run ../../Includes/_common

# COMMAND ----------

lesson_config = LessonConfig(name=None,
                             create_schema=True,
                             create_catalog=False,
                             requires_uc=False,
                             installing_datasets=True,
                             enable_streaming_support=False,
                             enable_ml_support = False)

DA = DBAcademyHelper(course_config=course_config,
                     lesson_config=lesson_config)
DA.reset_lesson()
DA.init()
DA.conclude_setup()

# COMMAND ----------

# TODO
my_name = 'Vlad'

# COMMAND ----------

example_df = spark.range(16)
spark.read.load(f"{DA.paths.datasets}/nyctaxi-with-zipcodes/data").write.mode("overwrite").saveAsTable("nyc_taxi")


# COMMAND ----------

assert my_name is not None, "Name is still None"
print(my_name)

# COMMAND ----------

display(example_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM nyc_taxi

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.datasets}/nyctaxi-with-zipcodes/data")
display(files)

# COMMAND ----------

DA.cleanup()

# COMMAND ----------


