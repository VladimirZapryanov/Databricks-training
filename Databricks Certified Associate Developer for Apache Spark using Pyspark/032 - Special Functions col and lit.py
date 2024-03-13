# Databricks notebook source
from pyspark.sql.functions import col, lit, upper, concat
from pyspark.sql.column import Column

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

employeesDF. \
    select("first_name", "last_name"). \
    show()

# COMMAND ----------

employeesDF. \
    groupBy('nationality'). \
    count(). \
    show()

# COMMAND ----------

help(col)

# COMMAND ----------

type(col('first_name'))

# COMMAND ----------

help(upper)

# COMMAND ----------

employeesDF. \
    select(upper('first_name'), upper('last_name')). \
    show()

# COMMAND ----------

employeesDF. \
    groupBy(upper(col("nationality"))). \
    count(). \
    show()

# COMMAND ----------

help(Column)

# COMMAND ----------

employeesDF. \
    orderBy(col("employee_id").desc()). \
    show()

# COMMAND ----------

employeesDF. \
    orderBy(col("first_name").desc()). \
    show()

# COMMAND ----------

employeesDF. \
    orderBy(upper(employeesDF['first_name']).alias('first_name')). \
    show()

# COMMAND ----------

employeesDF. \
    orderBy(upper(employeesDF.first_name).alias('first_name')). \
    show()

# COMMAND ----------

help(lit)

# COMMAND ----------

help(concat)

# COMMAND ----------

employeesDF. \
    select(concat(col("first_name"), 
                  lit(", "), 
                  col("last_name")
                 ).alias("full_name")
          ). \
    show(truncate=False)

# COMMAND ----------

employeesDF. \
    withColumn('bonus', col('salary') * lit(0.2)). \
    show()

# COMMAND ----------


