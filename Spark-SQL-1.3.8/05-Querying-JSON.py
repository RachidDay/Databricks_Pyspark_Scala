# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Querying JSON & Hierarchical Data with SQL
# MAGIC
# MAGIC Apache Spark&trade; and Databricks&reg; make it easy to work with hierarchical data, such as nested JSON records.
# MAGIC
# MAGIC ## In this lesson you:
# MAGIC * Use SQL to query a table backed by JSON data
# MAGIC * Query nested structured data
# MAGIC * Query data containing array columns
# MAGIC
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Analysts
# MAGIC * Additional Audiences: Data Engineers and Data Scientists
# MAGIC
# MAGIC ## Prerequisites
# MAGIC * Web browser: Chrome or Firefox
# MAGIC * Lesson: <a href="$./02-Querying-Files">Querying Files with SQL</a>
# MAGIC * Concept: <a href="https://www.w3schools.com/sql/" target="_blank">Basic SQL</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %run "./Includes/Test-Library"

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/a3098jg2t0?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/a3098jg2t0?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Examining the contents of a JSON file
# MAGIC
# MAGIC JSON is a common file format in big data applications and in data lakes (or large stores of diverse data).  Datatypes such as JSON arise out of a number of data needs.  For instance, what if...  
# MAGIC <br>
# MAGIC * Your schema, or the structure of your data, changes over time?
# MAGIC * You need nested fields like an array with many values or an array of arrays?
# MAGIC * You don't know how you're going use your data yet so you don't want to spend time creating relational tables?
# MAGIC
# MAGIC The popularity of JSON is largely due to the fact that JSON allows for nested, flexible schemas.
# MAGIC
# MAGIC This lesson uses the `DatabricksBlog` table, which is backed by JSON file `dbfs:/mnt/training/databricks-blog.json`. If you examine the raw file, you can see that it contains compact JSON data. There's a single JSON object on each line of the file; each object corresponds to a row in the table. Each row represents a blog post on the <a href="https://databricks.com/blog" target="_blank">Databricks blog</a>, and the table contains all blog posts through August 9, 2017.

# COMMAND ----------

# MAGIC %fs head dbfs:/mnt/training/databricks-blog.json

# COMMAND ----------

# MAGIC %md
# MAGIC To expose the JSON file as a table, use the standard SQL create table using syntax introduced in the previous lesson:

# COMMAND ----------

#the API will read some data to infer the schema >>inferSchema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS DatabricksBlog
# MAGIC   USING json
# MAGIC   OPTIONS (
# MAGIC     path "dbfs:/mnt/training/databricks-blog.json",
# MAGIC     inferSchema "true" 
# MAGIC   )

# COMMAND ----------

blog =  spark.read.json('dbfs:/mnt/training/databricks-blog.json')

# COMMAND ----------

blog.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at the schema with the `DESCRIBE` function.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DatabricksBlog

# COMMAND ----------

blog.printSchema()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Run a query to view the contents of the table.
# MAGIC
# MAGIC Notice:
# MAGIC * The `authors` column is an array containing multiple author names.
# MAGIC * The `categories` column is an array of multiple blog post category names.
# MAGIC * The `dates` column contains nested fields `createdOn`, `publishedOn` and `tz`.

# COMMAND ----------

# MAGIC %sql 
# MAGIC Select authors, categories, dates, content
# MAGIC From DatabricksBlog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nested Data
# MAGIC
# MAGIC Think of nested data as columns within columns. 
# MAGIC
# MAGIC For instance, look at the `dates` column.

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/kqmfblujy9?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/kqmfblujy9?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dates FROM DatabricksBlog

# COMMAND ----------

blog.select('dates').display()

# COMMAND ----------

# MAGIC %md
# MAGIC Pull out a specific subfield with "dot" notation.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dates.createdOn, dates.publishedOn 
# MAGIC FROM DatabricksBlog

# COMMAND ----------

blog.select(F.col('dates').createdOn.alias('createdOn'), F.col('dates').publishedOn.alias('publishedOn')).display()

# COMMAND ----------

blog_dates = spark.sql('SELECT dates.createdOn, dates.publishedOn FROM DatabricksBlog').show()

# COMMAND ----------

# MAGIC %md
# MAGIC Both `createdOn` and `publishedOn` are stored as strings.
# MAGIC
# MAGIC Cast those values to SQL timestamps:
# MAGIC
# MAGIC In this case, use a single `SELECT` statement to:
# MAGIC 1. Cast `dates.publishedOn` to a `timestamp` data type.
# MAGIC 2. "Flatten" the `dates.publishedOn` column to just `publishedOn`.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from DatabricksBlog limit 5;

# COMMAND ----------

blog.limit(5).display()

# COMMAND ----------

from pyspark.sql.types import  IntegerType , StringType , FloatType, StructField , StructType, TimestampType
blog.select(F.col('dates').createdOn.cast(TimestampType()).alias('publishedOn ') , F.col('title')).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT title, cast(dates.createdOn AS timestamp) AS publishedOn 
# MAGIC FROM DatabricksBlog

# COMMAND ----------

# MAGIC %md
# MAGIC Create the temporary view `DatabricksBlog2` to capture the conversion and flattening of the `publishedOn` column.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW DatabricksBlog2 AS
# MAGIC   SELECT *, 
# MAGIC          cast(dates.publishedOn AS timestamp) AS publishedOn 
# MAGIC   FROM DatabricksBlog

# COMMAND ----------

blog.withColumn('publishedOn',F.col('dates').publishedOn.cast(TimestampType()).alias('publishedOn ')).createOrReplaceTempView('DatabricksBlog2_Spark')

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have this temporary view, we can use `DESCRIBE` to check its schema and confirm the timestamp conversion.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DatabricksBlog2

# COMMAND ----------

spark.sql("DESCRIBE DatabricksBlog2_Spark").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Now the dates are represented by a `timestamp` data type, query for articles within certain date ranges (such as getting a list of all articles published in 2013), and format the date for presentation purposes.
# MAGIC
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> See the Spark documentation, <a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions" target="_blank">built-in functions</a>, for a long list of date-specific functions.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT title, 
# MAGIC        date_format(publishedOn, "MMM dd, yyyy") AS date, 
# MAGIC        link 
# MAGIC FROM DatabricksBlog2
# MAGIC WHERE year(publishedOn) = 2013
# MAGIC ORDER BY publishedOn

# COMMAND ----------

test= spark.table('DatabricksBlog2_Spark').filter(F.year(F.col('publishedOn')) == 2013 ).select(F.col('title') , F.date_format(F.col('publishedOn'),'MMM dd, yyyy').alias('date'), F.col('link')).sort(F.col('publishedOn')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Array Data
# MAGIC
# MAGIC The table also contains array columns. 
# MAGIC
# MAGIC Easily determine the size of each array using the built-in `size(..)` function with array columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT size(authors), 
# MAGIC        authors 
# MAGIC FROM DatabricksBlog

# COMMAND ----------

blog.select(F.size(F.col('authors')), F.col('authors')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Pull the first element from the array `authors` using an array subscript operator.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT link,authors, authors[1] AS primaryAuthor 
# MAGIC FROM DatabricksBlog

# COMMAND ----------

test1= blog.select('link' , 'authors' , F.col('authors')[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explode
# MAGIC
# MAGIC The `explode` function allows you to split an array column into multiple rows, copying all the other columns into each new row. 
# MAGIC
# MAGIC For example, you can split the column `authors` into the column `author`, with one author per row.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT title, 
# MAGIC        authors, 
# MAGIC        explode(authors) AS author, 
# MAGIC        link 
# MAGIC FROM DatabricksBlog

# COMMAND ----------

test2= blog.select('title' , 'authors' , F.explode(F.col('authors'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC It's more obvious to restrict the output to articles that have multiple authors, and sort by the title.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT title, 
# MAGIC        authors, 
# MAGIC        explode(authors) AS author, 
# MAGIC        link 
# MAGIC FROM DatabricksBlog 
# MAGIC WHERE size(authors) > 2
# MAGIC ORDER BY title

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lateral View
# MAGIC The data has multiple columns with nested objects.  In this case, the data has multiple dates, authors, and categories.
# MAGIC
# MAGIC Take a look at the blog entry **Apache Spark 1.1: The State of Spark Streaming**:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dates.publishedOn, title, authors, categories
# MAGIC FROM DatabricksBlog
# MAGIC WHERE title = "Apache Spark 1.1: The State of Spark Streaming"

# COMMAND ----------

# MAGIC %md
# MAGIC Next, use `LATERAL VIEW` to explode multiple columns at once, in this case, the columns `authors` and `categories`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dates.publishedOn, title, author, category
# MAGIC FROM DatabricksBlog
# MAGIC LATERAL VIEW explode(authors) exploded_authors_view AS author
# MAGIC LATERAL VIEW explode(categories) exploded_categories AS category
# MAGIC WHERE title = "Apache Spark 1.1: The State of Spark Streaming"
# MAGIC ORDER BY author, category

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1
# MAGIC
# MAGIC Identify all the articles written or co-written by Michael Armbrust.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1
# MAGIC
# MAGIC Starting with the table `DatabricksBlog`, create a temporary view called `ArticlesByMichael` where:
# MAGIC 1. Michael Armbrust is the author
# MAGIC 2. The data set contains the column `title` (it may contain others)
# MAGIC 3. It contains only one record per article
# MAGIC
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** See the Spark documentation, <a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions" target="_blank">built-in functions</a>.  
# MAGIC
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Include the column `authors` in your view, to help you debug your solution.

# COMMAND ----------

step1= blog.filter(F.array_contains(F.col('authors'), 'Michael Armbrust')).select('title' , 'authors').createOrReplaceTempView('ArticlesByMichael_Spark')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ArticlesByMichael AS
# MAGIC   SELECT title,authors
# MAGIC   FROM DatabricksBlog
# MAGIC   where array_contains(authors,'Michael Armbrust')

# COMMAND ----------

# MAGIC  %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ArticlesByMichael AS
# MAGIC   SELECT title,authors,author
# MAGIC   FROM DatabricksBlog
# MAGIC  LATERAL VIEW explode(authors) exploded_authors_view AS author
# MAGIC  WHERE author= "Michael Armbrust"

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC resultsDF = spark.sql("select title from ArticlesByMichael_Spark order by title")
# MAGIC dbTest("SQL-L5-articlesByMichael-count", 3, resultsDF.count())
# MAGIC
# MAGIC results = [r[0] for r in resultsDF.collect()]
# MAGIC dbTest("SQL-L5-articlesByMichael-0", "Exciting Performance Improvements on the Horizon for Spark SQL", results[0])
# MAGIC dbTest("SQL-L5-articlesByMichael-1", "Spark SQL Data Sources API: Unified Data Access for the Apache Spark Platform", results[1])
# MAGIC dbTest("SQL-L5-articlesByMichael-2", "Spark SQL: Manipulating Structured Data Using Apache Spark", results[2])
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2
# MAGIC Show the list of Michael Armbrust's articles.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ArticlesByMichael

# COMMAND ----------

# MAGIC %sql
# MAGIC Select title,author from DatabricksBlog
# MAGIC LATERAL VIEW explode(authors) exploded_authors_view AS author
# MAGIC  WHERE author= "Michael Armbrust"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2
# MAGIC
# MAGIC Identify the complete set of categories used in the Databricks blog articles.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1
# MAGIC
# MAGIC Starting with the table `DatabricksBlog`, create another view called `UniqueCategories` where:
# MAGIC 1. The data set contains the one column `category` (and no others)
# MAGIC 2. This list of categories should be unique

# COMMAND ----------

# MAGIC  %sql
# MAGIC select categories from DatabricksBlog

# COMMAND ----------

UniqueCategories=blog.select(F.explode(F.col('categories')).alias('category')).dropDuplicates(['category']).createOrReplaceTempView('UniqueCategories')

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC   SELECT explode(categories) as category
# MAGIC   FROM DatabricksBlog

# COMMAND ----------

# MAGIC %sql
# MAGIC   CREATE OR REPLACE TEMPORARY VIEW UniqueCategories AS
# MAGIC   SELECT distinct explode(categories) as category
# MAGIC   FROM DatabricksBlog

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC resultsCount = spark.sql("SELECT category FROM UniqueCategories order by category")
# MAGIC
# MAGIC dbTest("SQL-L5-uniqueCategories-count", 12, resultsCount.count())
# MAGIC
# MAGIC results = [r[0] for r in resultsCount.collect()]
# MAGIC dbTest("SQL-L5-uniqueCategories-0", "Announcements", results[0])
# MAGIC dbTest("SQL-L5-uniqueCategories-1", "Apache Spark", results[1])
# MAGIC dbTest("SQL-L5-uniqueCategories-2", "Company Blog", results[2])
# MAGIC
# MAGIC dbTest("SQL-L5-uniqueCategories-9", "Platform", results[9])
# MAGIC dbTest("SQL-L5-uniqueCategories-10", "Product", results[10])
# MAGIC dbTest("SQL-L5-uniqueCategories-11", "Streaming", results[11])
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2
# MAGIC Show the complete list of categories.

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * From UniqueCategories
# MAGIC Order by category

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3
# MAGIC
# MAGIC Count how many times each category is referenced in the Databricks blog.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1
# MAGIC
# MAGIC Starting with the table `DatabricksBlog`, create a temporary view called `TotalArticlesByCategory` where:
# MAGIC 1. The new table contains two columns, `category` and `total`
# MAGIC 2. The `category` column is a single, distinct category (similar to the last exercise)
# MAGIC 3. The `total` column is the total number of articles in that category
# MAGIC
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You need either multiple views or a `LATERAL VIEW` to solve this.
# MAGIC
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Because articles can be tagged with multiple categories, the sum of the totals adds up to more than the total number of articles.

# COMMAND ----------

TotalArticlesByCategory=blog.select(F.explode(F.col('categories')).alias('category')).groupBy('category').agg(F.count(F.col('category')).alias('total')).createOrReplaceTempView('TotalArticlesByCategory')

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC resultsDF = spark.sql("SELECT category, total FROM TotalArticlesByCategory ORDER BY category")
# MAGIC dbTest("SQL-L5-articlesByCategory-count", 12, resultsDF.count())
# MAGIC
# MAGIC results = [ (r[0]+" w/"+str(r[1])) for r in resultsDF.collect()]
# MAGIC
# MAGIC dbTest("SQL-L5-articlesByCategory-0", "Announcements w/72", results[0])
# MAGIC dbTest("SQL-L5-articlesByCategory-1", "Apache Spark w/132", results[1])
# MAGIC dbTest("SQL-L5-articlesByCategory-2", "Company Blog w/224", results[2])
# MAGIC
# MAGIC dbTest("SQL-L5-articlesByCategory-9", "Platform w/4", results[9])
# MAGIC dbTest("SQL-L5-articlesByCategory-10", "Product w/83", results[10])
# MAGIC dbTest("SQL-L5-articlesByCategory-11", "Streaming w/21", results[11])
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2
# MAGIC Display the totals of each category, order by `category`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM  TotalArticlesByCategory
# MAGIC order BY total desc
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC * Spark SQL allows you to query and manipulate structured and semi-structured data
# MAGIC * Spark SQL's built-in functions provide powerful primitives for querying complex schemas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Questions
# MAGIC **Q:** What is the syntax for accessing nested columns?  
# MAGIC **A:** Use the dot notation: ```SELECT dates.publishedOn```
# MAGIC
# MAGIC **Q:** What is the syntax for accessing the first element in an array?  
# MAGIC **A:** Use the [subscript] notation:  ```SELECT authors[0]```
# MAGIC
# MAGIC **Q:** What is the syntax for expanding an array into multiple rows?  
# MAGIC **A:** Use the explode keyword, either:  
# MAGIC ```SELECT explode(authors) as Author``` or  
# MAGIC ```LATERAL VIEW explode(authors) exploded_authors_view AS author```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Start the next lesson, [Querying Data Lakes with SQL]($./06-Data-Lakes).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC
# MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/index.html" target="_blank">Spark SQL Reference</a>
# MAGIC * <a href="http://spark.apache.org/docs/latest/sql-programming-guide.html" target="_blank">Spark SQL, DataFrames and Datasets Guide</a>
# MAGIC * <a href="https://stackoverflow.com/questions/36876959/sparksql-can-i-explode-two-different-variables-in-the-same-query" target="_blank">SparkSQL: Can I explode two different variables in the same query? (StackOverflow)</a>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>