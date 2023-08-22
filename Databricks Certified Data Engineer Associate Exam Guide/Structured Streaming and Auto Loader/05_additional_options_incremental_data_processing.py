# Databricks notebook source
# MAGIC %md
# MAGIC #1) Create Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE books (
# MAGIC   book_id INT,
# MAGIC   title STRING,
# MAGIC   author STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO books
# MAGIC VALUES
# MAGIC   (1, 'Harry Potter and the Philosophers Stone', 'J.K. Rowling'),
# MAGIC   (2, 'The Great Gatsby', 'F. Scott Fitzgerald'),
# MAGIC   (3, 'To Kill a Mockingbird', 'Harper Lee'),  
# MAGIC   (4, 'Pride and Prejudice', 'Jane Austen'),
# MAGIC   (5, 'The Old Man and the Sea', 'Ernest Hemingway'),
# MAGIC   (6, 'The Catcher in the Rye', 'J.D. Salinger'),
# MAGIC   (7, 'Fahrenheit 451', 'Ray Bradbury'),
# MAGIC   (8, 'Of Mice and Men', 'John Steinbeck'),
# MAGIC   (9, 'ord of the Flies', 'William Golding'),
# MAGIC   (10, 'The Handmaids Tale', 'Margaret Atwood'),
# MAGIC   (11, 'Beloved', 'Toni Morrison'),
# MAGIC   (12, 'One Hundred Years of Solitude', 'Gabriel Garcia Marquez');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM books;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE EXTENDED books;

# COMMAND ----------

# MAGIC %md
# MAGIC #2) Reading  a Stream

# COMMAND ----------

(spark.readStream
        .table('books')
        .createOrReplaceTempView('books_streaming_tmp_vw'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM books_streaming_tmp_vw;

# COMMAND ----------

#cansel the stream
for s in spark.streams.active:
    print(f'Stoping: {s.id}')
    s.stop()
    s.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC #3) Working with Streaming Data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT author, count(book_id) AS total_books
# MAGIC FROM books_streaming_tmp_vw
# MAGIC GROUP BY author;

# COMMAND ----------

# MAGIC %md
# MAGIC #4) Unsupported Operations

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM books_streaming_tmp_vw
# MAGIC ORDER BY author;

# COMMAND ----------

# MAGIC %md
# MAGIC #5) Persisting Streaming Results

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

(spark.table('author_counts_tmp_vw')
    .writeStream
    .trigger(processingTime='4 seconds')
    .outputMode('complete')
    .option('checkpointLocation', 'dbfs:/FileStore/books/author_counts_checkpoint')
    .table('author_counts'))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM author_counts;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values
# MAGIC (13, 'Harry Potter and the Chamber of Secrets', 'J.K. Rowling'),
# MAGIC (14, 'The Casual Vacancy', 'J.K. Rowling'),
# MAGIC (15, 'Sense and Sensibility', 'Jane Austen');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM author_counts;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values
# MAGIC (16, 'Moby Dick', 'Herman Melville'),
# MAGIC (17, 'War and Peace', 'Leo Tolstoy'),
# MAGIC (18, 'Ready Player One', 'Ernest Cline');

# COMMAND ----------

(spark.table("author_counts_tmp_vw")                               
      .writeStream           
      .trigger(availableNow=True)
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/FileStore/books/author_counts_checkpoint")
      .table("author_counts")
      .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM author_counts;

# COMMAND ----------


