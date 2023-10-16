# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture

# COMMAND ----------

greeting = 'hello'
print(greeting.upper())
print(greeting)

# COMMAND ----------

help(greeting.capitalize)

# COMMAND ----------

greeting.capitalize()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collection Type 1: List

# COMMAND ----------

breakfast_list = ['pancakes', 'eggs', 'waffles']
breakfast_list

# COMMAND ----------

type(breakfast_list)

# COMMAND ----------

breakfast_list.append('yogurt')
breakfast_list

# COMMAND ----------

print(breakfast_list[0])
print(breakfast_list[-1])

# COMMAND ----------

breakfast_list[0:2]

# COMMAND ----------

print(breakfast_list[:2])
print(breakfast_list[1:])

# COMMAND ----------

print(breakfast_list)
breakfast_list[0] = 'sausage'
print(breakfast_list)

# COMMAND ----------

'waffles' in breakfast_list

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Collection Type 2: Dictionaries

# COMMAND ----------

breakfast_dict = {'pancakes': 1, 'eggs': 2, 'waffles': 3}
breakfast_dict

# COMMAND ----------

breakfast_dict.get('waffles')

# COMMAND ----------

breakfast_dict['waffles']

# COMMAND ----------

print(breakfast_dict)
breakfast_dict['waffles'] += 1
breakfast_dict['yogurt'] = 1
print(breakfast_dict)

# COMMAND ----------

print(breakfast_dict.keys())
print('bacon' in breakfast_dict.keys())

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab

# COMMAND ----------

dinner_list = ['potatoes', 'peppers', 'onions']

dinner_list[0] = 'sweet potatoes'

# COMMAND ----------

assert dinner_list == ['sweet potatoes', 'peppers', 'onions'], "dinner_list should be ['sweet potatoes', 'peppers', 'onions']"
print("Test passed!")

# COMMAND ----------

dinner_list.append('rice')

# COMMAND ----------

assert dinner_list == ['sweet potatoes', 'peppers', 'onions', 'rice'], "dinner_list should be ['sweet potatoes', 'peppers', 'onions', 'rice']"
print("Test passed!")

# COMMAND ----------

dinner_dict = {"sweet potatoes": 3, "peppers": 4, "onions": 1}

# COMMAND ----------

assert dinner_dict["sweet potatoes"] == 3, "We had 3 sweet potatoes"
assert dinner_dict["peppers"] == 4, "We had 4 peppers"
assert dinner_dict["onions"] == 1, "We had 1 onion"
print("Tests passed!")

# COMMAND ----------

dinner_dict['sweet potatoes'] = 2
dinner_dict['ice cream'] = 1

# COMMAND ----------

assert dinner_dict["sweet potatoes"] == 2, "We had 2 sweet potatoes"
assert dinner_dict["ice cream"] == 1, "We had 1 ice cream (but don't tell!)"
print("Tests passed!")

# COMMAND ----------

ingredient_set_1 = {'carrots', 'onions', 'potatoes'}
ingredient_set_2 = {'broccoli', 'carrots', 'rice'}
ingredient_set_3 = {'sweet potatoes', 'carrots', 'corn'}
# ingredient_intersection_set = ingredient_set_1.intersection(ingredient_set_2)
# ingredient_intersection_set = ingredient_intersection_set.intersection(ingredient_set_3)
ingredient_intersection_set = ingredient_set_1 & ingredient_set_2 & ingredient_set_3
print(ingredient_intersection_set)

# COMMAND ----------

assert ingredient_intersection_set == {"carrots"}, "Only carrots occurs in all the ingredients"
assert "broccoli" in ingredient_set_2, "Did you forget your broccoli?"

print("Test passed!")

# COMMAND ----------


