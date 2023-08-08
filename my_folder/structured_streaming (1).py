# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

(spark.readStream
    .table("books_csv")
    .createOrReplaceTempView("books_streaming_tmp_vw")
);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM books_streaming_tmp_vw;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT author, count(book_id) AS total_books
# MAGIC FROM books_streaming_tmp_vw
# MAGIC GROUP BY author;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw AS (
# MAGIC   SELECT author, count(book_id) AS total_books
# MAGIC   FROM books_streaming_tmp_vw
# MAGIC   GROUP BY author
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM author_counts_tmp_vw;

# COMMAND ----------

(spark.table("author_counts_tmp_vw")
      .writeStream
      .trigger(processingTime = "4 seconds")
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO books_csv
# MAGIC VALUES("16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC       ("17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC       ("18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35);

# COMMAND ----------

(spark.table("author_counts_tmp_vw")
      .writeStream
      .trigger(availableNow = True)
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
      .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT *
# MAGIC FROM books_csv

# COMMAND ----------


