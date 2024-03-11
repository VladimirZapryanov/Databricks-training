# Databricks notebook source
# MAGIC %run "./78 - Setup Data Sets to perform joins"

# COMMAND ----------

help(courses_df.join)

# COMMAND ----------

users_df. \
    join(course_enrolments_df, users_df.user_id == course_enrolments_df.user_id).show()

# COMMAND ----------

users_df. \
    join(course_enrolments_df, 'user_id').show()

# COMMAND ----------

users_df. \
    join(course_enrolments_df, users_df.user_id == course_enrolments_df.user_id). \
    select(users_df['*'], course_enrolments_df['course_id'], course_enrolments_df['course_enrolment_id']). \
    show()

# COMMAND ----------

users_df.alias('u'). \
    join(course_enrolments_df.alias('ce'), users_df.user_id == course_enrolments_df.user_id). \
    select('u.*', 'course_id', 'course_enrolment_id'). \
    show()

# COMMAND ----------

users_df. \
    join(course_enrolments_df, users_df.user_id == course_enrolments_df.user_id). \
    groupBy(users_df['user_id']). \
    count(). \
    show()

# COMMAND ----------

users_df.alias('u'). \
    join(course_enrolments_df.alias('ce'), users_df.user_id == course_enrolments_df.user_id). \
    groupBy('u.user_id'). \
    count(). \
    show()

# COMMAND ----------

users_df. \
    join(course_enrolments_df, 'user_id'). \
    groupBy('user_id'). \
    count(). \
    show()

# COMMAND ----------


