# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Lecture

# COMMAND ----------

# This is our first line of Python code!
1+1

# COMMAND ----------

# Integer expression
2 * 3 + 5 - 1

# COMMAND ----------

# Float expression
1.2 * 2.3 + 5.5

# COMMAND ----------

type(1.2)

# COMMAND ----------

type(1.)

# COMMAND ----------

# String expression
"Hello World!"  + "123"

# COMMAND ----------

# String expression with space
"Hello World!" + " " + "123"

# COMMAND ----------

True or False

# COMMAND ----------

True and False

# COMMAND ----------

not False

# COMMAND ----------

a = 3
b = 2 
c = a*b

c

# COMMAND ----------

b = 4
c

# COMMAND ----------

b = "Hello World"
b = 1
b

# COMMAND ----------

a = 1
b = 2

a # This line isn't printed becouse it's not the last line of code  
b 

# COMMAND ----------

print(a)
print(b)

# COMMAND ----------

print('Hello World!')

# COMMAND ----------

name = 'Robin'
age = 30

print(f'My name is {name} and I am {age} years old')

# COMMAND ----------

# MAGIC %md 
# MAGIC # Lab

# COMMAND ----------

name = 'Vlad'
num_chocolate = 2
chocolate_string = f'{name} would like to eat {num_chocolate} bars of chocolate'

print(chocolate_string)

# COMMAND ----------

assert type(name) == str, "Name should be a string"
assert type(num_chocolate) == int, "You have to eat the entire chocolate bar! No floats allowed"
assert chocolate_string == f"{name} would like to eat {num_chocolate} bars of chocolate", "Did you mistype something?"
print("Test passed!")

# COMMAND ----------

num_students = 32
num_days = 20
class_name = 'Class Test'
class_information = f'There are {num_students * num_days} student days in the {class_name}'

print(class_information)

# COMMAND ----------

assert type(class_name) == str, "Name should be a string"
assert type(num_students) == int, "Use an integer"
assert type(num_days) == int, "Use an integer"
assert class_information == f"There are {num_students * num_days} student days in the {class_name}"

print("Test passed!")

# COMMAND ----------


