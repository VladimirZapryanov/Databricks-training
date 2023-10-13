# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture

# COMMAND ----------

if True:
    print('True')
else:
    print('False')

# COMMAND ----------

print(1 == 1)
print(1.5 != 2.5)
print("abc" == "xyz")
print(True == True)

# COMMAND ----------

lunch_price = 20

if lunch_price <= 15:
    print('Buy it!')
else:
    print('Too expensive')

# COMMAND ----------

if lunch_price <= 15:
    print('Buy it!')
else:
    if lunch_price < 25:
        print('Is it really good?')
    else:
        print('This better be the best food of all time')

# COMMAND ----------

lunch_price = 15

if lunch_price == 10:
    print('10 dollars exactly! buy it!')
elif lunch_price <= 15:
    print('Buy it!')
elif lunch_price < 25:
    print('Is it really good?')
else:
    print('This better be the best food of all time')

# COMMAND ----------

dog_person = True
cat_person = True
age = 30

if age < 18:
    print('Ask your parents for permission!')
else:
    if dog_person and cat_person:
        print('Golden Retriever')
    elif dog_person and not cat_person:
        print('Scottish Deerhound')
    elif cat_person and not dog_person:
        print('You are barking up the wrong tree!')
    else:
        print('Are you sure a pet is right for you')

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab

# COMMAND ----------

# temperature = 65
# sunny = True
# message = ''

# if temperature >= 60:
#     if sunny:
#         message = 'Ice cream'
#     else:
#         message = 'Dumplings'
# else:
#     message = 'Hot tea'

# print(message)

# -------------------------------
# temperature = 65
# sunny = False
# message = ''

# if temperature >= 60:
#     if sunny:
#         message = 'Ice cream'
#     else:
#         message = 'Dumplings'
# else:
#     message = 'Hot tea'

# print(message)

# ------------------------------
temperature = 50
sunny = True
message = ''

if temperature >= 60:
    if sunny:
        message = 'Ice cream'
    else:
        message = 'Dumplings'
else:
    message = 'Hot tea'

print(message)

# COMMAND ----------

# km_since_last_change = 15000
# oil_change_light = True
# message = ''

# if km_since_last_change >= 15000:
#     if oil_change_light:
#         message = 'oil change'
#     else:
#         message = 'wait'
# else:
#     message = 'wait'

# print(message)

# -------------------------------
# km_since_last_change = 15000
# oil_change_light = False
# message = ''

# if km_since_last_change >= 15000:
#     if oil_change_light:
#         message = 'oil change'
#     else:
#         message = 'wait'
# else:
#     message = 'wait'

# print(message)

# --------------------------------
km_since_last_change = 10000
oil_change_light = True
message = ''

if km_since_last_change >= 15000:
    if oil_change_light:
        message = 'oil change'
    else:
        message = 'wait'
else:
    message = 'wait'

print(message)

# COMMAND ----------

year = 2000
is_leap_year = False

if year % 4 == 0:
    is_leap_year = True
    if year % 100 == 0:
        is_leap_year = False
        if year % 400 == 0:
            is_leap_year = True

print(is_leap_year)
