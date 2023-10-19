# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture

# COMMAND ----------

if print('Hello!') # This is not correct Python code, so it throws a Syntax Error

# COMMAND ----------

1 / 0

# COMMAND ----------

try:
    1/0
except:
    print('Exception Handled')

# COMMAND ----------

try:
    1/0
except ZeroDivisionError:
    print('Exception Handled')

# COMMAND ----------

try:
    print(undefined_variable)
except ZeroDivisionError:
    print('Exceptions Handled')

# COMMAND ----------

try:
    1/0 
    # print(undefined_variable)
except (ZeroDivisionError, NameError):
    print('Exceptions Handled')

# COMMAND ----------

assert 1 == 1
assert 1 == 2, 'That is not True'
