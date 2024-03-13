# Databricks notebook source
import getpass

# COMMAND ----------

username = getpass.getuser()

# COMMAND ----------

orders = spark.read.csv(f'/user/{username}/retail_db_pipe/orders')

# COMMAND ----------

orders.show(truncate=False)

# COMMAND ----------

help(spark.read.csv)

# COMMAND ----------

orders = spark. \
    read. \
    csv(
        f'/user/{username}/retail_db_pipe/orders',
        sep='|',
        header=None,
        inferSchema=True
    ). \
    toDF('order_id', 'order_date', 'order_customer_id', 'order_status')

# COMMAND ----------

orders.show()

# COMMAND ----------

help(spark.read.format('csv').load)

# COMMAND ----------

orders = spark. \
    read. \
    format('csv'). \
    load(
        f'/user/{username}/retail_db_pipe/orders',
        sep='|',
        header=None,
        inferSchema=True
    ). \
    toDF('order_id', 'order_date', 'order_customer_id', 'order_status')

# COMMAND ----------

help(spark.read.option)

# COMMAND ----------

orders = spark. \
    read. \
    option('sep', '|'). \
    option('header', None). \
    option('inferSchema', True). \
    csv(f'/user/{username}/retail_db_pipe/orders'). \
    toDF('order_id', 'order_date', 'order_customer_id', 'order_status')

# COMMAND ----------

orders.show()

# COMMAND ----------

orders.dtypes

# COMMAND ----------

help(spark.read.options)

# COMMAND ----------

orders = spark. \
    read. \
    options(sep='|', header=None, inferSchema=True). \
    csv(f'/user/{username}/retail_db_pipe/orders'). \
    toDF('order_id', 'order_date', 'order_customer_id', 'order_status')

# COMMAND ----------

orders.show()

# COMMAND ----------

options = {
    'sep': '|',
    'header': None,
    'inferSchema': True
}

# COMMAND ----------

orders = spark. \
    read. \
    options(**options). \
    csv(f'/user/{username}/retail_db_pipe/orders'). \
    toDF('order_id', 'order_date', 'order_customer_id', 'order_status')

# COMMAND ----------

orders.show()

# COMMAND ----------

orders = spark. \
    read. \
    options(**options). \
    format('csv'). \
    load(f'/user/{username}/retail_db_pipe/orders'). \
    toDF('order_id', 'order_date', 'order_customer_id', 'order_status')

# COMMAND ----------

orders.show()

# COMMAND ----------


