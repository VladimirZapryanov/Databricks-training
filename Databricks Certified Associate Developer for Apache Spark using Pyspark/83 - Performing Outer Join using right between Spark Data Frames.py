# Databricks notebook source
from pyspark.sql.functions import sum, when, expr

# COMMAND ----------

# MAGIC %run "./78 - Setup Data Sets to perform joins"

# COMMAND ----------

course_enrolments_df. \
    join(users_df, users_df.user_id == course_enrolments_df.user_id, 'right'). \
    show()

# COMMAND ----------

course_enrolments_df. \
    join(users_df, 'user_id', 'right'). \
    show()

# COMMAND ----------

course_enrolments_df. \
    join(users_df, users_df.user_id == course_enrolments_df.user_id, 'right'). \
    select(users_df['*'], course_enrolments_df['course_id'], course_enrolments_df['course_enrolment_id']). \
    show()

# COMMAND ----------

course_enrolments_df.alias('ce'). \
    join(users_df.alias('u'), users_df.user_id == course_enrolments_df.user_id, 'right'). \
    select('u.*', 'course_id', 'course_enrolment_id'). \
    show()

# COMMAND ----------

course_enrolments_df.alias('ce'). \
    join(users_df.alias('u'), users_df.user_id == course_enrolments_df.user_id, 'right'). \
    filter('ce.course_enrolment_id IS NULL'). \
    select('u.*', 'course_id', 'course_enrolment_id'). \
    show()

# COMMAND ----------

course_enrolments_df.alias('ce'). \
    join(users_df.alias('u'), users_df.user_id == course_enrolments_df.user_id, 'right'). \
    groupBy('u.user_id'). \
    count(). \
    orderBy('u.user_id'). \
    show()

# COMMAND ----------

course_enrolments_df.alias('ce'). \
    join(users_df.alias('u'), users_df.user_id == course_enrolments_df.user_id, 'right'). \
    groupBy('u.user_id'). \
    agg(sum(when(course_enrolments_df['course_enrolment_id'].isNull(), 0).otherwise(1)).alias('course_count')). \
    orderBy('u.user_id'). \
    show()

# COMMAND ----------

course_enrolments_df.alias('ce'). \
    join(users_df.alias('u'), users_df.user_id == course_enrolments_df.user_id, 'right'). \
    groupBy('u.user_id'). \
    agg(sum(expr('''
        CASE WHEN ce.course_enrolment_id IS NULL
            THEN 0
        ELSE 1
        END
    ''')).alias('course_count')). \
    orderBy('u.user_id'). \
    show()

# COMMAND ----------


