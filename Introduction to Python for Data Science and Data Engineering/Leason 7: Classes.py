# Databricks notebook source
# MAGIC %md 
# MAGIC # Lecture
# MAGIC

# COMMAND ----------

class Dog():
    pass

# COMMAND ----------

my_dog = Dog()

type(my_dog)

# COMMAND ----------

class UpdatedDog():

    def return_name(self, name):
        return f'name: {name}'

# COMMAND ----------

my_updated_dog = UpdatedDog()

my_updated_dog.return_name('Rex')

# COMMAND ----------

class DogWithSelf():
    def print_self(self):
        print(self)

dog_with_self = DogWithSelf()
print(dog_with_self)
dog_with_self.print_self()

# COMMAND ----------

class DogWithAttributes():

    def __init__(self, name, color):
        print('This ran automatically')
        self.name = name
        self.color = color

dog_with_attributes = DogWithAttributes('Rex', 'Orange')

# COMMAND ----------

dog_with_attributes.name

# COMMAND ----------

dog_with_attributes.color

# COMMAND ----------

class DogWithAttributesAndMethod():

    def __init__(self, name, color):
        self.name = name
        self.color = color

    def return_name(self):
        return self.name
    
my_dog = DogWithAttributesAndMethod('Rex', 'Blue')
my_dog.return_name()

# COMMAND ----------

class DogWithAttributesAndMethod():

    def __init__(self, name, color):
        self.name = name
        self.color = color

    def return_name(self):
        return self.name
    
    def update_name(self, new_name):
        self.name = new_name
        return new_name
    
my_dog = DogWithAttributesAndMethod('Rex', 'Blue')
print(f"Here's my name now: {my_dog.return_name()}")

my_dog.update_name('Brady')
print(f"Here's my name after updating it: {my_dog.return_name()}")

# COMMAND ----------

class FinalDog():

    def __init__(self, name, color):
        self.name = name
        self.color = color
    
    def return_name(self):
        return self.name
    
    def update_name(self, new_name):
        self.name = new_name
        return new_name
    
    def return_both_names(self, other_dog_object):
        return self.name + " and " + other_dog_object.name
    
dog_1 = FinalDog("Rex", "Blue")
dog_2 = FinalDog("Brady", "Purple")

dog_1.return_both_names(dog_2)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab
# MAGIC

# COMMAND ----------

class Simpson():
    def __init__(self, first_name, age, favorite_food):
        self.first_name = first_name
        self.age = age
        self.favorite_food = favorite_food

    def simpson_summary(self):
        message = f'{self.first_name} Simpson is {self.age} years old and their favorite food is {self.favorite_food}'
        return message
    
    def older(self, another):
        older = False 

        if self.age > another.age:
            older = True

        return older


homer = Simpson("Homer", 39, "Donuts")
bart = Simpson("Bart", 10, "Hamburgers")
assert homer.first_name == "Homer", "first_name is not set properly"
assert homer.favorite_food == "Donuts", "favorite_food is not set properly"
assert bart.age == 10, "Age is not set properly"
assert homer.simpson_summary() == "Homer Simpson is 39 years old and their favorite food is Donuts", "simpson_summary is incorrect"
assert bart.simpson_summary() == "Bart Simpson is 10 years old and their favorite food is Hamburgers", "simpson_summary is incorrect"
assert homer.older(bart) == True, "Homer is older than Bart"
assert bart.older(homer) == False, "Bart is not older than Homer"
print("Test passed!")


# COMMAND ----------

class Fraction():
    def __init__(self, numerator, denominator):
        self._numerator = numerator
        self._denominator = denominator


    def to_string(self):
        return f'{self.numerator} / {self.denominator}'


    def as_decimal(self):
        return self.numerator / self.denominator


    def invert(self):
        self.numerator, self.denominator = self.denominator, self.numerator

        return (self.numerator, self.denominator)


    def least_common_denominator(self, second_denominator):
        return max(self.denominator, second_denominator.denominator)


    def is_equal(self, second_number):
        is_equal = False
        first_num = self.numerator / self.denominator
        second_num = second_number.numerator/ second_number.denominator

        if first_num == second_num:
            is_equal = True

        return is_equal


    def is_less_than(self, second_number):
        is_less = False
        first_num = self.numerator / self.denominator
        second_num = second_number.numerator/ second_number.denominator

        if first_num < second_num:
            is_less = True

        return is_less


    def scale(self, scale_number):
        self.numerator, self.denominator = self.numerator * scale_number, self.denominator * scale_number
        
        return (self.numerator, self.denominator)


    def clone(self, new_fraction):
        self.numerator, self.denominator = new_fraction.numerator, new_fraction.denominator

        return (self.numerator, self.denominator)
    

# COMMAND ----------

# Test your work
one_half = Fraction(1,2)
two_fourths = Fraction(2,4)
two_thirds = Fraction(2,3)
six_ninths = Fraction(6,9)

# to work with this code you must unencapsulated numerator and denominator in Class attributes!!!

# assert one_half.to_string() == "1 / 2", "to_string did not convert the string properly"
# assert two_fourths.as_decimal() == .5, "as_decimal did not convert the fraction to a decimal number properly"

# four_eighths = Fraction(4, 8)
# four_eighths.invert()
# assert four_eighths.to_string() == "8 / 4", "invert did not convert the fraction properly"

# assert two_thirds.least_common_denominator(six_ninths) == 9.0, "least_common_denominator did not compute the least common denominator properly"

# assert one_half.is_equal(two_fourths) == True, "is_equal did not compare two fractions properly"
# assert one_half.is_less_than(two_thirds) == True, "is_less_than did not compare two fractions properly"

# two_fourths.scale(3) 

# assert two_fourths.to_string() == "6 / 12", "Not True Scale" 

# one_half.clone(two_thirds)
# assert one_half.to_string == "2 / 3", "Not True Clone" 

try:
    six_ninths.numerator 
    six_ninths.denominator
    print("The fraction has not been encapsulated properly")
except Exception as ex:
    pass

# COMMAND ----------


