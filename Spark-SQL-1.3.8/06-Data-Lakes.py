# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Querying Data Lakes with SQL
# MAGIC
# MAGIC Apache Spark&trade; and Databricks&reg; make it easy to work with hierarchical data, such as nested JSON records.
# MAGIC
# MAGIC Perform exploratory data analysis (EDA) to gain insights from a Data Lake.
# MAGIC
# MAGIC ## In this lesson you:
# MAGIC * Use SQL to query a Data Lake
# MAGIC * Clean messy data sets
# MAGIC * Join two cleaned data sets
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
# MAGIC ## Data Lakes
# MAGIC
# MAGIC Companies frequently have thousands of large data files gathered from various teams and departments, typically using a diverse variety of formats including CSV, JSON, and XML.  Analysts often wish to extract insights from this data.
# MAGIC
# MAGIC The classic approach to querying this data is to load it into a central database called a Data Warehouse.  This involves the time-consuming operation of designing the schema for the central database, extracting the data from the various data sources, transforming the data to fit the warehouse schema, and loading it into the central database.  The analyst can then query this enterprise warehouse directly or query smaller data marts created to optimize specific types of queries.
# MAGIC
# MAGIC This classic Data Warehouse approach works well but requires a great deal of upfront effort to design and populate schemas.  It also limits historical data, which is restrained to only the data that fits the warehouse’s schema.
# MAGIC
# MAGIC An alternative to this approach is the Data Lake.  A _Data Lake_:
# MAGIC
# MAGIC * Is a storage repository that cheaply stores a vast amount of raw data in its native format
# MAGIC * Consists of current and historical data dumps in various formats including XML, JSON, CSV, Parquet, etc.
# MAGIC * Also may contain operational relational databases with live transactional data
# MAGIC
# MAGIC Spark is ideal for querying Data Lakes as the Spark SQL query engine is capable of reading directly from the raw files and then executing SQL queries to join and aggregate the Data.
# MAGIC
# MAGIC You will see in this lesson that once two tables are created (independent of their underlying file type), we can join them, execute nested queries, and perform other operations across our Data Lake.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %run "./Includes/Test-Library"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/yyblq4fgfl?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/yyblq4fgfl?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Looking at our Data Lake
# MAGIC
# MAGIC You can start by reviewing which files are in our Data Lake.
# MAGIC
# MAGIC In `dbfs:/mnt/training/crime-data-2016`, there are some Parquet files containing 2016 crime data from several United States cities.
# MAGIC
# MAGIC As you can see in the cell below, we have data for Boston, Chicago, New Orleans, and more.

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/6v5a6qgfbb?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/6v5a6qgfbb?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %fs ls /mnt/training/crime-data-2016

# COMMAND ----------

# MAGIC %md
# MAGIC The next step in looking at the data is to create a temporary view for each file.  Recall that temporary views use a similar syntax to `CREATE TABLE` but using the command `CREATE TEMPORARY VIEW`.  Temporary views are removed once your session has ended while tables are persisted beyond a given session.
# MAGIC
# MAGIC Start by creating a view of the data from New York and then Boston:
# MAGIC
# MAGIC | City          | Table Name              | Path to DBFS file
# MAGIC | ------------- | ----------------------- | -----------------
# MAGIC | **New York**  | `CrimeDataNewYork`      | `dbfs:/mnt/training/crime-data-2016/Crime-Data-New-York-2016.parquet`
# MAGIC | **Boston**    | `CrimeDataBoston`       | `dbfs:/mnt/training/crime-data-2016/Crime-Data-Boston-2016.parquet`

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CrimeDataNewYork
# MAGIC   USING parquet
# MAGIC   OPTIONS (
# MAGIC     path "dbfs:/mnt/training/crime-data-2016/Crime-Data-New-York-2016.parquet"
# MAGIC   )

# COMMAND ----------

CrimeDataNewYork_Spark= spark.read.parquet("dbfs:/mnt/training/crime-data-2016/Crime-Data-New-York-2016.parquet").createOrReplaceTempView('CrimeDataNewYork_Spark')

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC spark.read.parquet("dbfs:/mnt/training/crime-data-2016/Crime-Data-New-York-2016.parquet").createOrReplaceTempView("CrimeDataNewYork_Spark_Scala")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CrimeDataBoston
# MAGIC   USING parquet
# MAGIC   OPTIONS (
# MAGIC     path "dbfs:/mnt/training/crime-data-2016/Crime-Data-Boston-2016.parquet"
# MAGIC   )

# COMMAND ----------

CrimeDataNewYork_Spark= spark.read.parquet("dbfs:/mnt/training/crime-data-2016/Crime-Data-Boston-2016.parquet").createOrReplaceTempView('CrimeDataBoston_Spark')

# COMMAND ----------

# MAGIC %scala
# MAGIC val rimeDataBoston_Spark_Scala =spark.read.parquet("dbfs:/mnt/training/crime-data-2016/Crime-Data-Boston-2016.parquet").createOrReplaceTempView("CrimeDataBoston_Spark_Scala")

# COMMAND ----------

# MAGIC %md
# MAGIC With the view created, it is now possible to review the first couple records of each file.
# MAGIC
# MAGIC Notice in the example below:
# MAGIC * The `CrimeDataNewYork` and `CrimeDataBoston` datasets use different names for the columns
# MAGIC * The data itself is formatted differently and different names are used for similar concepts
# MAGIC
# MAGIC This is common in a Data Lake.  Often files are added to a Data Lake by different groups at different times.  While each file itself usually has clean data, there is little consistency across files.  The advantage of this strategy is that anyone can contribute information to the Data Lake and that Data Lakes scale to store arbitrarily large and diverse data.  The tradeoff for this ease in storing data is that it doesn’t have the rigid structure of a more traditional relational data model so the person querying the Data Lake will need to clean the data before extracting useful insights.
# MAGIC
# MAGIC The alternative to a Data Lake is a Data Warehouse.  In a Data Warehouse, a committee often regulates the schema and ensures data is cleaned before being made available.  This makes querying much easier but also makes gathering the data much more expensive and time-consuming.  Many companies choose to start with a Data Lake to accumulate data.  Then, as the need arises, they clean the data and produce higher quality tables for querying.  This reduces the upfront costs while still making data easier to query over time.  These cleaned tables can even be later loaded into a formal data warehouse through nightly batch jobs.  In this way, Apache Spark can be used to manage and query both Data Lakes and Data Warehouses.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM CrimeDataNewYork

# COMMAND ----------

spark.table('CrimeDataNewYork_Spark').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM CrimeDataBoston

# COMMAND ----------

# MAGIC %md
# MAGIC ## Same type of data, different structure
# MAGIC
# MAGIC In this section, we examine crime data to figure out how to extract homicide statistics.
# MAGIC
# MAGIC Because our data sets are pooled together in a Data Lake, each city may use different field names and values to indicate homicides, dates, etc.
# MAGIC
# MAGIC For example:
# MAGIC * Some cities use the value "HOMICIDE", "CRIMINAL HOMICIDE" or even "MURDER"
# MAGIC * In New York, the column is named `offenseDescription` but, in Boston, the column is named `OFFENSE_CODE_GROUP`
# MAGIC * In New York, the date of the event is in the `reportDate` column but, in Boston, there is a single column named `MONTH`

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/9mc9dtyx5u?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/9mc9dtyx5u?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC To get started, create a temporary view containing only the homicide-related rows.
# MAGIC
# MAGIC At the same time, normalize the data structure of each table so that all the columns (and their values) line up with each other.
# MAGIC
# MAGIC In the case of New York and Boston, here are the unique characteristics of each data set:
# MAGIC
# MAGIC | | Offense-Column        | Offense-Value          | Reported-Column  | Reported-Data Type |
# MAGIC |-|-----------------------|------------------------|-----------------------------------|
# MAGIC | New York | `offenseDescription`  | starts with "murder" or "homicide" | `reportDate`     | `timestamp`    |
# MAGIC | Boston | `OFFENSE_CODE_GROUP`  | "Homicide"             | `MONTH`          | `integer`      |
# MAGIC
# MAGIC For the upcoming aggregation, you will need to alter the New York data set to include a `month` column which can be computed from the `reportDate` column using the `month()` function. Boston already has this column.
# MAGIC
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> One helpful tool for finding the offences we're looking for is using <a href="https://en.wikipedia.org/wiki/Regular_expression" target="_blank">regular expressions</a> supported by SQL
# MAGIC
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We can also normalize the values with the `CASE`, `WHEN`, `THEN` & `ELSE` expressions but that is not required for the task at hand.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW HomicidesNewYork AS
# MAGIC   SELECT month(reportDate) AS month, offenseDescription AS offense
# MAGIC   FROM CrimeDataNewYork
# MAGIC   WHERE lower(offenseDescription) LIKE 'murder%' OR lower(offenseDescription) LIKE 'homicide%'

# COMMAND ----------

HomicidesNewYor_Spark=spark.table('CrimeDataNewYork_Spark').filter((F.lower(F.col('offenseDescription')).like('murder%'))&(F.lower(F.col('offenseDescription')).like('homicide%'))).select(F.month(F.col('reportDate')).alias('month') , F.col('offenseDescription').alias('offense')).createOrReplaceTempView('HomicidesNewYor_Spark')

# COMMAND ----------

# MAGIC %scala
# MAGIC val HomicidesNewYork_Spark_Scala = spark.table("CrimeDataNewYork_Spark_Scala")
# MAGIC   .filter(lower(col("offenseDescription")).like("murder%") || lower(col("offenseDescription")).like("homicide%"))
# MAGIC   .select(month(col("reportDate")).alias("month"), col("offenseDescription").alias("offense"))
# MAGIC   .createOrReplaceTempView("HomicidesNewYork_Spark_Scala")
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW HomicidesBoston AS
# MAGIC   SELECT month, OFFENSE_CODE_GROUP AS offense
# MAGIC   FROM CrimeDataBoston
# MAGIC   WHERE lower(OFFENSE_CODE_GROUP) = 'homicide'

# COMMAND ----------

HomicidesBoston_Spark=spark.table('CrimeDataBoston_Spark').filter(F.lower(F.col('OFFENSE_CODE_GROUP')) == 'homicide').select(F.col('month'), F.col('OFFENSE_CODE_GROUP').alias('offense')).createOrReplaceTempView('HomicidesBoston_Spark')

# COMMAND ----------

# MAGIC %scala
# MAGIC val HomicidesBoston_Spark_Scala=spark.table("CrimeDataBoston_Spark_Scala").filter(lower(col("OFFENSE_CODE_GROUP")) === "homicide").select(col("month"), col("OFFENSE_CODE_GROUP").alias("offense")).createOrReplaceTempView("HomicidesBoston_Spark_Scala")

# COMMAND ----------

# MAGIC %md
# MAGIC You can see below that the structure of our two tables is now identical.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM HomicidesNewYork LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM HomicidesBoston LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyzing our data

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have normalized the homicide data for each city we can combine the two by taking their union.
# MAGIC
# MAGIC When we are done, we can then aggregate that data to compute the number of homicides per month.
# MAGIC
# MAGIC Start by creating a new view called `HomicidesBostonAndNewYork` which simply unions the result of two `SELECT` statements together.
# MAGIC
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> See <a href="https://stackoverflow.com/questions/49925/what-is-the-difference-between-union-and-union-all">this Stack Overflow post</a> for the difference between `UNION` and `UNION ALL`

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/ld3fh1x0ig?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/ld3fh1x0ig?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW HomicidesBostonAndNewYork AS
# MAGIC   SELECT * FROM HomicidesNewYork
# MAGIC     UNION ALL
# MAGIC   SELECT * FROM HomicidesBoston

# COMMAND ----------

HomicidesBostonAndNewYork_Spark=spark.table('HomicidesNewYor_Spark').unionAll(spark.table('HomicidesBoston_Spark')).createOrReplaceTempView('HomicidesBostonAndNewYork_Spark')

# COMMAND ----------

# MAGIC %scala
# MAGIC val HomicidesBostonAndNewYork_Spark_Scala=spark.table("HomicidesNewYork_Spark_Scala").unionAll(spark.table("HomicidesBoston_Spark_Scala")).createOrReplaceTempView("HomicidesBostonAndNewYork_Spark_Scala")

# COMMAND ----------

# MAGIC %md
# MAGIC You can now see below all the data in one table:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM HomicidesBostonAndNewYork
# MAGIC ORDER BY month

# COMMAND ----------

spark.table('HomicidesBostonAndNewYork_spark').sort('month').display()

# COMMAND ----------

# MAGIC %md
# MAGIC And finally we can perform a simple aggregation to see the number of homicides per month:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT month, count(*) AS homicides
# MAGIC FROM HomicidesBostonAndNewYork
# MAGIC GROUP BY month
# MAGIC ORDER BY month

# COMMAND ----------

homicides=spark.table('HomicidesBostonAndNewYork_spark').groupBy('month').agg(F.count(F.col('month')).alias('homicides')).sort('month').display()

# COMMAND ----------

# MAGIC %scala
# MAGIC display(spark.table("HomicidesBostonAndNewYork_spark_Scala").groupBy("month").agg(count(col("month")).alias("homicides")).sort("month"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1
# MAGIC
# MAGIC Merge the crime data for Chicago with the data for New York and Boston and then update our final aggregation of counts-by-month.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1
# MAGIC
# MAGIC Create the initial view of the Chicago data.
# MAGIC 1. The source file is `dbfs:/mnt/training/crime-data-2016/Crime-Data-Chicago-2016.parquet`
# MAGIC 2. Name the view `CrimeDataChicago`
# MAGIC 3. View the data with a simple `SELECT` statement

# COMMAND ----------

CrimeDataChicago=spark.read.parquet("dbfs:/mnt/training/crime-data-2016/Crime-Data-Chicago-2016.parquet").createOrReplaceTempView('CrimeDataChicago')

# COMMAND ----------

# MAGIC %scala
# MAGIC val CrimeDataChicago_Scala=spark.read.parquet("dbfs:/mnt/training/crime-data-2016/Crime-Data-Chicago-2016.parquet").createOrReplaceTempView("CrimeDataChicago_Scala")

# COMMAND ----------

spark.table('CrimeDataChicago').display()

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC total = spark.sql("select count(*) from CrimeDataChicago").first()[0]
# MAGIC dbTest("SQL-L6-crimeDataChicago-count", 267872, total)
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2
# MAGIC
# MAGIC Create a new view that normalizes the data structure.
# MAGIC 1. Name the view `HomicidesChicago`
# MAGIC 2. The table should have at least two columns: `month` and `offense`
# MAGIC 3. Filter the data to only include homicides
# MAGIC 4. View the data with a simple `SELECT` statement
# MAGIC
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You will need to use the `month()` function to extract the month-of-the-year.
# MAGIC
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** To find out which values for each offense constitutes a homicide, produce a distinct list of values from the table `CrimeDataChicago`.

# COMMAND ----------

spark.table('CrimeDataChicago').drop_duplicates(['description']).filter((F.lower(F.col('description')).like('%murder%')) | (F.lower(F.col('description')).like('%homicide%')) ).select('description').display()

# COMMAND ----------

HomicidesChicago=spark.table('CrimeDataChicago').filter((F.lower(F.col('description')).like('%murder%')) | (F.lower(F.col('description')).like('%homicide%')) ).select(F.month(F.col('date')).alias('month') , F.col('description').alias('offense')).createOrReplaceTempView('HomicidesChicago')

# COMMAND ----------

# MAGIC %scala
# MAGIC val HomicidesChicago_Scala=spark.table("CrimeDataChicago_Scala").filter((lower(col("description")).like("%murder%")) || (lower(col("description")).like("%homicide%"))).select(month(col("date")).alias("month") , col("description").alias("offense")).createOrReplaceTempView("HomicidesChicago_Scala")

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC homicidesChicago = spark.sql("SELECT month, count(*) FROM HomicidesChicago GROUP BY month ORDER BY month").collect()
# MAGIC dbTest("SQL-L6-homicideChicago-len", 12, len(homicidesChicago))
# MAGIC
# MAGIC dbTest("SQL-L6-homicideChicago-0", 54, homicidesChicago[0][1])
# MAGIC dbTest("SQL-L6-homicideChicago-6", 71, homicidesChicago[6][1])
# MAGIC dbTest("SQL-L6-homicideChicago-11", 58, homicidesChicago[11][1])
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3
# MAGIC
# MAGIC Create a new view that merges all three data sets (New York, Boston, Chicago):
# MAGIC 1. Name the view `AllHomicides`
# MAGIC 2. Use the `UNION ALL` expression introduced earlier to merge all three tables
# MAGIC   * `HomicidesNewYork`
# MAGIC   * `HomicidesBoston`
# MAGIC   * `HomicidesChicago`
# MAGIC 0. View the data with a simple `SELECT` statement
# MAGIC
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** To union three tables together, copy the previous example and just add as second `UNION` statement followed by the appropriate `SELECT` statement.

# COMMAND ----------

spark.table('HomicidesChicago').union(spark.table('HomicidesBoston_Spark')).union(spark.table('HomicidesNewYor_Spark')).createOrReplaceTempView('AllHomicides')

# COMMAND ----------

# MAGIC %scala
# MAGIC val AllHomicides_Scala=spark.table("HomicidesChicago_Scala").union(spark.table("HomicidesBoston_Spark_Scala")).union(spark.table("HomicidesNewYork_Spark_Scala")).createOrReplaceTempView("AllHomicides_Scala")

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC allHomicides = spark.sql("SELECT count(*) AS total FROM AllHomicides").first().total
# MAGIC dbTest("SQL-L6-allHomicides-count", 885, allHomicides)
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4
# MAGIC
# MAGIC Create a new view that counts the number of homicides per month.
# MAGIC 1. Name the view `HomicidesByMonth`
# MAGIC 2. Rename the column `count(1)` to `homicides`
# MAGIC 3. Group the data by `month`
# MAGIC 4. Sort the data by `month`
# MAGIC 5. Count the number of records for each aggregate
# MAGIC 6. View the data with a simple `SELECT` statement

# COMMAND ----------

HomicidesByMonth=spark.table('AllHomicides').groupBy('month').agg(F.count(F.col('offense')).alias('homicides')).sort('month').createOrReplaceTempView('HomicidesByMonth')

# COMMAND ----------

# MAGIC %scala
# MAGIC val HomicidesByMonth_Scala=spark.table("AllHomicides_Scala").groupBy("month").agg(count(col("offense")).alias("homicides")).sort("month").createOrReplaceTempView("HomicidesByMonth_Scala")

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC allHomicides = spark.sql("SELECT * FROM HomicidesByMonth").collect()
# MAGIC dbTest("SQL-L6-homicidesByMonth-len", 12, len(allHomicides))
# MAGIC
# MAGIC dbTest("SQL-L6-homicidesByMonth-0", 1, allHomicides[0].month)
# MAGIC dbTest("SQL-L6-homicidesByMonth-11", 12, allHomicides[11].month)
# MAGIC
# MAGIC dbTest("SQL-L6-allHomicides-0", 61, allHomicides[0].homicides)
# MAGIC dbTest("SQL-L6-allHomicides-1", 51, allHomicides[1].homicides)
# MAGIC dbTest("SQL-L6-allHomicides-2", 47, allHomicides[2].homicides)
# MAGIC dbTest("SQL-L6-allHomicides-3", 48, allHomicides[3].homicides)
# MAGIC dbTest("SQL-L6-allHomicides-4", 74, allHomicides[4].homicides)
# MAGIC dbTest("SQL-L6-allHomicides-5", 88, allHomicides[5].homicides)
# MAGIC dbTest("SQL-L6-allHomicides-6", 86, allHomicides[6].homicides)
# MAGIC dbTest("SQL-L6-allHomicides-7", 108, allHomicides[7].homicides)
# MAGIC dbTest("SQL-L6-allHomicides-8", 75, allHomicides[8].homicides)
# MAGIC dbTest("SQL-L6-allHomicides-9", 89, allHomicides[9].homicides)
# MAGIC dbTest("SQL-L6-allHomicides-10", 90, allHomicides[10].homicides)
# MAGIC dbTest("SQL-L6-allHomicides-11", 68, allHomicides[11].homicides)
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC * Spark SQL allows you to easily manipulate data in a Data Lake
# MAGIC * Temporary views help to save your cleaned data for downstream analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Questions
# MAGIC **Q:** What is a Data Lake?  
# MAGIC **A:** Data Lakes are a loose collection of data files gathered from various sources.  Spark loads each file as a table and then executes queries joining and aggregating these files.
# MAGIC
# MAGIC **Q:** What are some advantages of Data Lakes over more classic Data Warehouses?  
# MAGIC **A:** Data Lakes allow for large amounts of data to be aggregated from many sources with minimal ceremony or overhead.  Data Lakes also allow for very very large files.  Powerful query engines such as Spark can read the diverse collection of files and execute complex queries efficiently.
# MAGIC
# MAGIC **Q:** What are some advantages of Data Warehouses?  
# MAGIC **A:** Data warehouses are neatly curated to ensure data from all sources fit a common schema.  This makes them very easy to query.
# MAGIC
# MAGIC **Q:** What's the best way to combine the advantages of Data Lakes and Data Warehouses?  
# MAGIC **A:** Start with a Data Lake.  As you query, you will discover cases where the data needs to be cleaned, combined, and made more accessible.  Create periodic Spark jobs to read these raw sources and write new "golden" tables that are cleaned and more easily queried.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC * Please complete this short [feedback survey](https://www.surveymonkey.com/r/WBMS7CG).  Your input is extremely important and will shape future development.
# MAGIC * Next, take what you learned about working data lakes and apply them in the [Capstone Project]($./07-Capstone-Project).

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>