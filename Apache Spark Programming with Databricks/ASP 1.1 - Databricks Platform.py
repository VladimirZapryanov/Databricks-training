# Databricks notebook source
# MAGIC %md 
# MAGIC # Lecture
# MAGIC

# COMMAND ----------

# MAGIC %run "/Apache Spark/apache-spark-programming-with-databricks-english/Includes/Classroom-Setup" 

# COMMAND ----------

print("Run default language")

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC print("Run python")

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC println("Run scala")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select "Run SQL"

# COMMAND ----------

# MAGIC %r
# MAGIC
# MAGIC print("Run R")

# COMMAND ----------

# MAGIC %sh ps | grep 'java'
# MAGIC

# COMMAND ----------

html = """<h1 style="color:orange;text-align:center;font-family:Courier">Render HTML</h1>"""

displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create documentation cells
# MAGIC Render cell as <a href="https://www.markdownguide.org/cheat-sheet/" target="_blank">Markdown</a> using the magic command: **`%md`**
# MAGIC
# MAGIC Below are some examples of how you can use Markdown to format documentation. Click this cell and press **`Enter`** to view the underlying Markdown syntax.
# MAGIC
# MAGIC
# MAGIC # Heading 1
# MAGIC ### Heading 3
# MAGIC > block quote
# MAGIC
# MAGIC 1. **bold**
# MAGIC 2. *italicized*
# MAGIC 3. ~~strikethrough~~
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC - <a href="https://www.markdownguide.org/cheat-sheet/" target="_blank">link</a>
# MAGIC - `code`
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC   "message": "This is a code block",
# MAGIC   "method": "https://www.markdownguide.org/extended-syntax/#fenced-code-blocks",
# MAGIC   "alternative": "https://www.markdownguide.org/basic-syntax/#code-blocks"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ![Spark Logo](https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png)
# MAGIC
# MAGIC | Element         | Markdown Syntax |
# MAGIC |-----------------|-----------------|
# MAGIC | Heading         | `#H1` `##H2` `###H3` `#### H4` `##### H5` `###### H6` |
# MAGIC | Block quote     | `> blockquote` |
# MAGIC | Bold            | `**bold**` |
# MAGIC | Italic          | `*italicized*` |
# MAGIC | Strikethrough   | `~~strikethrough~~` |
# MAGIC | Horizontal Rule | `---` |
# MAGIC | Code            | ``` `code` ``` |
# MAGIC | Link            | `[text](https://www.example.com)` |
# MAGIC | Image           | `![alt text](image.jpg)`|
# MAGIC | Ordered List    | `1. First items` <br> `2. Second Item` <br> `3. Third Item` |
# MAGIC | Unordered List  | `- First items` <br> `- Second Item` <br> `- Third Item` |
# MAGIC | Code Block      | ```` ``` ```` <br> `code block` <br> ```` ``` ````|
# MAGIC | Table           |<code> &#124; col &#124; col &#124; col &#124; </code> <br> <code> &#124;---&#124;---&#124;---&#124; </code> <br> <code> &#124; val &#124; val &#124; val &#124; </code> <br> <code> &#124; val &#124; val &#124; val &#124; </code> <br>|

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets

# COMMAND ----------

# MAGIC %fs head /databricks-datasets/README.md

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

# MAGIC %fs help

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets")

# COMMAND ----------

files = dbutils.fs.ls("/databricks-datasets")
display(files)

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.events)
display(files)

# COMMAND ----------

spark.sql(f"SET c.events_path = {DA.paths.events}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS events
# MAGIC USING DELTA
# MAGIC OPTIONS (path "${c.events_path}");

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE EXTENDED events;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT * FROM events;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT traffic_source, SUM(ecommerce.purchase_revenue_in_usd) AS total_revenue
# MAGIC FROM events
# MAGIC GROUP BY traffic_source;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE WIDGET TEXT state DEFAULT "CA";

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM events
# MAGIC WHERE geo.state = getArgument("state");

# COMMAND ----------

# MAGIC %sql
# MAGIC REMOVE WIDGET state;

# COMMAND ----------

dbutils.widgets.text('name', 'Brickster', 'Name')
dbutils.widgets.multiselect('colors', 'orange', ['red', 'orange', 'black', 'blue'], 'Traffic Sources')

# COMMAND ----------

name = dbutils.widgets.get('name')
colors = dbutils.widgets.get('colors').split(',')

html = '<div>Hi {}! Select your color preference.</div>'.format(name)
for c in colors:
    html += """<label for='{}' style='color:{}'><input type='radio'> {}</labe><br>""".format(c, c, c)

displayHTML(html)



# COMMAND ----------

dbutils.widgets.removeAll

# COMMAND ----------

classroom_cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/dbacademy-users/

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/mnt/dbacademy-users/")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS users
# MAGIC USING DELTA
# MAGIC OPTIONS (path "${DA.paths.users}");
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS sales
# MAGIC USING DELTA
# MAGIC OPTIONS (path "${DA.paths.sales}");
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS products
# MAGIC USING DELTA
# MAGIC OPTIONS (path "${DA.paths.products}");
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS events
# MAGIC USING DELTA
# MAGIC OPTIONS (path "${DA.paths.events}");

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM products;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT AVG(purchase_revenue_in_usd)
# MAGIC FROM sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT(event_name)
# MAGIC FROM events;

# COMMAND ----------

DA.cleanup

# COMMAND ----------


