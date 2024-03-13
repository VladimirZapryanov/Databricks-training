# Databricks notebook source
from pyspark.sql.functions import concat, lit, concat_ws, col, lower, upper, initcap, length

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

employeesDF.show()

# COMMAND ----------

help(concat)

# COMMAND ----------

employeesDF. \
    withColumn('full_name', concat('first_name', 'last_name')). \
    show()

# COMMAND ----------

employeesDF. \
    withColumn('full_name', concat('first_name', lit(', '), 'last_name')). \
    show()

# COMMAND ----------

help(concat_ws)

# COMMAND ----------

employeesDF. \
    withColumn('full_name', concat_ws('-', 'first_name', 'last_name')). \
    show()

# COMMAND ----------

employeesDF. \
    select('employee_id', concat_ws('-', 'first_name', 'last_name', 'salary', 'nationality', 'phone_number', 'ssn').alias('full_data')). \
    show()

# COMMAND ----------

help(lower)

# COMMAND ----------

employeesDF. \
    select('employee_id', 'nationality'). \
    withColumn('upper_case', upper('nationality')). \
    withColumn('lower_case', lower('nationality')). \
    withColumn('initcap_case', initcap('nationality')). \
    withColumn('length', length('nationality')). \
    show()

# COMMAND ----------


