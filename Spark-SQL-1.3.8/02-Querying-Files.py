# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Querying Files with SQL
# MAGIC
# MAGIC Apache Spark&trade; and Databricks&reg; allow you to use SQL to query large data files.
# MAGIC
# MAGIC ## In this lesson you:
# MAGIC * Query large files using Spark SQL
# MAGIC * Visualize query results using charts
# MAGIC * Create temporary views to simplify complex queries
# MAGIC
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Analysts
# MAGIC * Additional Audiences: Data Engineers and Data Scientists
# MAGIC
# MAGIC ## Prerequisites
# MAGIC * Web browser: Chrome or Firefox
# MAGIC * Concept: <a href="https://www.w3schools.com/sql/" target="_blank">Basic SQL</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."
# MAGIC
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Remember to attach your notebook to a cluster by clicking "Detached" in the upper left hand corner and then choosing your preferred cluster
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/attach-to-cluster.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %run "./Includes/Test-Library"

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/glq179t3sr?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/glq179t3sr?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Querying Tables
# MAGIC This lesson uses the table `People10m`. 
# MAGIC
# MAGIC The data is fictitious; in particular, the Social Security numbers are fake.

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/wqp0pe2mol?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/wqp0pe2mol?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM People10M

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at its schema with the `DESCRIBE` function.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE People10M

# COMMAND ----------

# MAGIC %md
# MAGIC A simple SQL statement can answer the following question:
# MAGIC > According to our data, which women were born after 1990?
# MAGIC
# MAGIC In Databricks, a `SELECT` statement in a SQL cell is automatically run through Spark, and the results are displayed in an HTML table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT firstName, middleName, lastName, birthDate
# MAGIC FROM People10M
# MAGIC WHERE year(birthDate) > 1990 AND gender = 'F'

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT firstname, middlename,lastname,birthDate
# MAGIC FROM People
# MAGIC WHERE year(Date)>1990 AND geneder='F'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Built-in functions
# MAGIC
# MAGIC Spark provides a number of <a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions" target="_blank">built-in functions</a>, many of which can be used directly from SQL.  These functions can be used in the `WHERE` expressions to filter data and in `SELECT` expressions to create derived columns.
# MAGIC
# MAGIC The following SQL statement finds women born after 1990; it uses the `year` function, and it creates a `birthYear` column on the fly.

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/jsd9u7ep3k?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/jsd9u7ep3k?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT firstName, middleName, lastName, year(birthDate) as birthYear, salary 
# MAGIC FROM People10M
# MAGIC WHERE year(birthDate) > 1990 AND gender = 'F'

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Visualization
# MAGIC
# MAGIC Databricks provides built-in easy to use visualizations for your data. 
# MAGIC
# MAGIC Take the query below, and visualize it by selecting the bar graph icon once the table is displayed:
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/visualization-1.png" style="border: 1px solid #aaa; padding: 10px; border-radius: 10px 10px 10px 10px"/>

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/pl68ybkps2?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/pl68ybkps2?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC How many women were named Mary in seach year?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year(birthDate) as birthYear, count(*) AS total
# MAGIC FROM People10M
# MAGIC WHERE firstName = 'Mary' AND gender = 'F'
# MAGIC GROUP BY birthYear
# MAGIC ORDER BY birthYear

# COMMAND ----------

# MAGIC %md
# MAGIC Compare popularity of two names from 1990

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year(birthDate) as birthYear,  firstName, count(*) AS total
# MAGIC FROM People10M
# MAGIC WHERE (firstName = 'Dorothy' or firstName = 'Donna') AND gender = 'F' AND year(birthDate) > 1990
# MAGIC GROUP BY birthYear, firstName

# COMMAND ----------

# MAGIC %md
# MAGIC ### Temporary Views
# MAGIC
# MAGIC Temporary views assign a name to a query that will be reused as if they were tables themselves. Unlike tables, temporary views aren't stored on disk and are visible only to the current user. This course makes use of temporary views in the exercises to enable the test cases to verify your queries are correct.
# MAGIC
# MAGIC A temporary view gives you a name to query from SQL, but unlike a table, it exists only for the duration of your Spark Session. As a result, the temporary view will not carry over when you restart the cluster or switch to a new notebook. It also won't show up in the Data tab that, linked on the left of a Databricks notebook, provides easy access to databases and tables.
# MAGIC
# MAGIC The following statement creates a temporary view containing the same data.

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/kh6opy2t14?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/kh6opy2t14?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW TheDonnas AS
# MAGIC   SELECT * 
# MAGIC   FROM People10M 
# MAGIC   WHERE firstName = 'Donna'

# COMMAND ----------

# MAGIC %md
# MAGIC To view the contents of temporary view, use select notation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM TheDonnas

# COMMAND ----------

# MAGIC %md
# MAGIC Create more complex query from People10M table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW WomenBornAfter1990 AS
# MAGIC   SELECT firstName, middleName, lastName, year(birthDate) AS birthYear, salary 
# MAGIC   FROM People10M
# MAGIC   WHERE year(birthDate) > 1990 AND gender = 'F'

# COMMAND ----------

# MAGIC %md
# MAGIC Once a temporary view has been created, it can be queried as if it were itself a table. Find out how many Marys are in the WomenBornAfter1990 view.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT birthYear, count(*) 
# MAGIC FROM WomenBornAfter1990 
# MAGIC WHERE firstName = 'Mary' 
# MAGIC GROUP BY birthYear 
# MAGIC ORDER BY birthYear

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Exercise 1
# MAGIC
# MAGIC Create a temporary view called `Top10FemaleFirstNames` that contains the 10 most common female first names in the `People10M` table. The view must have two columns:
# MAGIC
# MAGIC * `firstName` - the first name
# MAGIC * `total` - the total number of rows with that first name
# MAGIC
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You may need to break ties by firstName because some of the totals are identical
# MAGIC
# MAGIC Display the results.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1
# MAGIC Create the temporary view.

# COMMAND ----------


from pyspark.sql import functions as F 
df_people = spark.table("People10M").filter(F.col("gender") == "F").groupby("firstName").agg(F.count("firstName").alias("total")).sort(F.desc("total"), "firstName").limit(10).createOrReplaceTempView("Top10FemaleFirstNames")

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC val df_people = spark.table("People10M").filter(col("gender") === "F").groupBy("firstName").agg(count("firstName").alias("total")).sort(desc("total"), col("firstName")).limit(10).createOrReplaceTempView("Top10FemaleFirstNames")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC resultsDF = spark.sql("SELECT * FROM Top10FemaleFirstNames ORDER BY firstName")
# MAGIC dbTest("SQL-L2-count", 10, resultsDF.count())
# MAGIC
# MAGIC results = [ (r[0]+", "+str(r[1]) ) for r in resultsDF.collect()]
# MAGIC dbTest("SQL-L2-names-0", "Alesha, 1368", results[0])
# MAGIC dbTest("SQL-L2-names-1", "Alice, 1384", results[1])
# MAGIC dbTest("SQL-L2-names-2", "Bridgette, 1373", results[2])
# MAGIC dbTest("SQL-L2-names-3", "Cristen, 1375", results[3])
# MAGIC dbTest("SQL-L2-names-4", "Jacquelyn, 1381", results[4])
# MAGIC dbTest("SQL-L2-names-5", "Katherin, 1373", results[5])
# MAGIC dbTest("SQL-L2-names-6", "Lashell, 1387", results[6])
# MAGIC dbTest("SQL-L2-names-7", "Louie, 1382", results[7])
# MAGIC dbTest("SQL-L2-names-8", "Lucille, 1384", results[8])
# MAGIC dbTest("SQL-L2-names-9", "Sharyn, 1394", results[9])
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2
# MAGIC
# MAGIC Display the contents of the temporary view.

# COMMAND ----------

spark.sql("SELECT * FROM Top10FemaleFirstNames").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC * Spark SQL queries tables that are backed by physical files
# MAGIC * You can visualize the results of your queries with built-in Databricks graphs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Questions
# MAGIC **Q:** What is the prefix used in databricks cells to execute SQL queries?  
# MAGIC **A:** `%sql`
# MAGIC
# MAGIC **Q:** How do temporary views differ from tables?  
# MAGIC **A:** Tables are visible to all users, can be accessed from any notebook, and persist across server resets.  Temporary views are only visible to the current user, in the current notebook, and are gone once the spark session ends.
# MAGIC
# MAGIC **Q:** What is the SQL syntax to create a temporary view?  
# MAGIC **A:** ```CREATE OR REPLACE TEMPORARY VIEW <<ViewName>> AS <<Query>>```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Start the next lesson, [Aggregations, JOINs and Nested Queries]($./03-Joins-Aggregations ).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC
# MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/index.html" target="_blank">Spark SQL Reference</a>
# MAGIC * <a href="http://spark.apache.org/docs/latest/sql-programming-guide.html" target="_blank">Spark SQL, DataFrames and Datasets Guide</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>