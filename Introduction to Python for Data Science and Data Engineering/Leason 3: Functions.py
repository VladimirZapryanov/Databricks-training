# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture

# COMMAND ----------

def ten_dollars_to_euros():
    print(10.0 * 0.93)

ten_dollars_to_euros()

# COMMAND ----------

def dollars_to_euros(dollar_amount):
    print(dollar_amount * 0.93)
    

# COMMAND ----------

dollars_to_euros(5.0)
dollars_to_euros(10)
dollars_to_euros(20)

# COMMAND ----------

def dollars_to_euros_with_rate(dollar_amount, conversion_rate):
    print(dollar_amount * conversion_rate)

# COMMAND ----------

dollars_to_euros_with_rate(10.0, 0.93)
dollars_to_euros_with_rate(5, 1)

# COMMAND ----------

dollars_to_euros_with_rate(10.0, 0.93)
dollars_to_euros_with_rate(5, 1)

# COMMAND ----------

def dollars_to_euros_with_default_rate(dollar_amount, conversion_rate = 0.93):
    print(dollar_amount * conversion_rate)

# COMMAND ----------

dollars_to_euros_with_default_rate(10.0)
dollars_to_euros_with_default_rate(10, 0.5)

# COMMAND ----------

def dollars_to_euros_with_default_rate(dollar_amount, conversion_rate = 0.93):
    return dollar_amount * conversion_rate

# COMMAND ----------

a = dollars_to_euros_with_default_rate(10)

print(a)

# COMMAND ----------

print(max(1,2))

print(len("abc"))

# COMMAND ----------

help(max)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Lab

# COMMAND ----------

def even_or_odd(num):
    message = ''
    
    if num % 2 == 0:
        message = "even"
    else:
        message = "odd"

    return message

# COMMAND ----------

assert even_or_odd(2) == "even", "2 is an even number"
assert even_or_odd(42) == "even", "42 is an even number"
assert even_or_odd(1) == "odd", "1 is an odd number"
assert even_or_odd(327) == "odd", "327 is an odd number"
print("Test passed!")

# COMMAND ----------

def fizz_buzz(num):
    message = ""

    if type(num) != int:
        message = "Wrong type"
    elif num % 5 == 0 and num % 3 != 0:
        message = "Fizz"
    elif num % 5 != 0 and num % 3 == 0:
        message = "Buzz"
    elif num % 5 == 0 and num % 3 == 0: 
        message = "FizzBuzz"
    else:
        message = num

    return message

# COMMAND ----------

assert fizz_buzz(15) == "FizzBuzz", "15 is divisible by both 5 and 3. Should return 'FizzBuzz'"
assert fizz_buzz(30) == "FizzBuzz", "30 is divisible by both 5 and 3. Should return 'FizzBuzz'"
assert fizz_buzz(5) == "Fizz", "5 is divisible by 5. Should return 'Fizz'"
assert fizz_buzz(25) == "Fizz", "25 is divisible by 5. Should return 'Fizz'"
assert fizz_buzz(3) == "Buzz", "3 is divisible by 3. Should return 'Buzz'"
assert fizz_buzz(81) == "Buzz", "81 is divisible by 3. Should return 'Buzz'"
assert fizz_buzz(23) == 23, "23 is not divisible by 3 or 5. Should return 23"
assert fizz_buzz(23.0) == "Wrong type", "Input is not an integer. Should return `Wrong type'"
assert fizz_buzz(True) == "Wrong type", "Input is not an integer. Should return `Wrong type'"
print("Test passed!")

# COMMAND ----------

def is_leap_year(year):
    is_leap_year = False

    if year % 4 == 0:
        is_leap_year = True
        if year % 100 == 0:
            is_leap_year = False
            if year % 400 == 0:
                is_leap_year = True

    return is_leap_year

# COMMAND ----------

assert is_leap_year(1900) == False, "1900 was a leap year"
assert is_leap_year(1901) == False, "1901 was not a leap year"
assert is_leap_year(1904) == True, "1904 was a leap year"
assert is_leap_year(2000) == True, "2000 was a leap year"
print("All tests passed")

# COMMAND ----------


