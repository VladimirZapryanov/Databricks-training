# Databricks notebook source
from pyspark.sql.functions import substring, lit, col

# COMMAND ----------

s = 'Hello World'

# COMMAND ----------

s[:5]

# COMMAND ----------

s[1:4]

# COMMAND ----------

l = [('X', )]

# COMMAND ----------

df = spark.createDataFrame(l, 'dummy STRING')

# COMMAND ----------

help(substring)

# COMMAND ----------

df.select(substring(lit('Hello World'), 7, 5)).show()

# COMMAND ----------

df.select(substring(lit('Hello World'), -5, 5)).show()

# COMMAND ----------

employees = [(1, "Scott", "Tiger", 1000.0, 
                      "united states", "+1 123 456 7890", "123 45 6789"
                     ),
                     (2, "Henry", "Ford", 1250.0, 
                      "India", "+91 234 567 8901", "456 78 9123"
                     ),
                     (3, "Nick", "Junior", 750.0, 
                      "united KINGDOM", "+44 111 111 1111", "222 33 4444"
                     ),
                     (4, "Bill", "Gomes", 1500.0, 
                      "AUSTRALIA", "+61 987 654 3210", "789 12 6118"
                     )
                ]

# COMMAND ----------

employeesDF = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING, 
                    last_name STRING, salary FLOAT, nationality STRING,
                    phone_number STRING, ssn STRING"""
                   )

# COMMAND ----------

employeesDF.show(truncate=False)

# COMMAND ----------

employeesDF. \
    select('first_name', 'ssn', 'phone_number'). \
    withColumn('last_four_ssn', substring(col('ssn'), -4, 4).cast('int')). \
    withColumn('last_four_phone', substring(col('phone_number'), -4, 4).cast('int')). \
    show()

# COMMAND ----------

employeesDF. \
    select('first_name', 'ssn', 'phone_number'). \
    withColumn('last_four_ssn', substring(col('ssn'), -4, 4)). \
    withColumn('last_four_phone', substring(col('phone_number'), -4, 4)). \
    printSchema()

# COMMAND ----------

employeesDF. \
    select('first_name', 'ssn', 'phone_number'). \
    withColumn('last_four_ssn', substring(col('ssn'), -4, 4).cast('int')). \
    withColumn('last_four_phone', substring(col('phone_number'), -4, 4).cast('int')). \
    show()

# COMMAND ----------

employeesDF. \
    select('first_name', 'ssn', 'phone_number'). \
    withColumn('last_four_ssn', substring(col('ssn'), -4, 4).cast('int')). \
    withColumn('last_four_phone', substring(col('phone_number'), -4, 4).cast('int')). \
    printSchema()

# COMMAND ----------


