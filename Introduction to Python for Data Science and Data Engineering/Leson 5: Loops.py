# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture

# COMMAND ----------

for number in [0, 1, 2]:
    print(number)

# COMMAND ----------

for element in range(0, 10):
    print('Hello!')

# COMMAND ----------

for element in range(0, 10):
    print(element)

# COMMAND ----------

for element in range(1, 11):
    print(element)

# COMMAND ----------

numbers_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
final_list = []

for element in numbers_list:
    if element > 4:
        final_list.append(element)

final_list

# COMMAND ----------

final_list_shortcut = [el for el in numbers_list if el > 4]
final_list_shortcut

# COMMAND ----------

double_list = [2 * el for el in numbers_list if el > 4]
double_list

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab

# COMMAND ----------

def detention_helper(detention_message, num_lines):
    for e in range(1, num_lines + 1):
        print(f'{e}. {detention_message}')

detention_message = 'I will not let my dog eat my homework'
num_lines = 50

detention_helper(detention_message, num_lines)

# COMMAND ----------

def detention_helper(detention_message, num_lines):
    count = 1
    while count < num_lines + 1:
        print(f'{count}. {detention_message}')
        count += 1

detention_message = 'I will do my python homework'
num_lines = 25

detention_helper(detention_message, num_lines)

# COMMAND ----------

cities = ['San Francisko', 'Paris', 'Mumbai']
temperatures = [58, 75, 81]
humidities = [.85, .5, .88]

print(f"{'City':15} {'Temperature':15} {'Humidity':15}")

for e in range(0, len(cities)):
    print(f"{cities[e]:15} {temperatures[e]:11} {humidities[e]:12.2f}")


# COMMAND ----------

def item_count(list_of_values):
    dict_values = {}

    for e in list_of_values:
        if e in dict_values:
            dict_values[e] += 1
        else:
            dict_values[e] = 1

    return dict_values

numbers = [1, 2, 3, 4, 5, 1, 3, 4, 6, 1, 4]
letters = ['a', 'b', 'c', 'a', 'a', 'a', 'b', 'c', 'b', 'c']
print(item_count(numbers))
print(item_count(letters))

# COMMAND ----------


