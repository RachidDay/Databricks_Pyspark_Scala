# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregations, JOINs and Nested Queries
# MAGIC Apache Spark&trade; and Databricks&reg; allow you to create on-the-fly data lakes.
# MAGIC
# MAGIC ## In this lesson you:
# MAGIC * Use basic aggregations.
# MAGIC * Correlate two data sets with a join
# MAGIC * Use subselects
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

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Basic aggregations
# MAGIC
# MAGIC Using <a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions" target="_blank">built-in Spark functions</a>, you can aggregate data in various ways. 
# MAGIC
# MAGIC Run the cell below to compute the average of all salaries in the `People10M` table.
# MAGIC
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> By default, you get a floating point value.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT avg(salary) AS averageSalary 
# MAGIC FROM People10M

# COMMAND ----------

# MAGIC %md
# MAGIC Convert that value to an integer using the SQL `round()` function. See
# MAGIC <a href="http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.round" class="text-info">the PySpark documentation for <tt>round()</tt></a>
# MAGIC for more details.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT round(avg(salary)) AS averageSalary 
# MAGIC FROM People10M

# COMMAND ----------

# MAGIC %md
# MAGIC In addition to the average salary, what are the maximum and minimum salaries?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT max(salary) AS max, min(salary) AS min, round(avg(salary)) AS average 
# MAGIC FROM People10M

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joining two tables
# MAGIC
# MAGIC Correlate the data in two data sets using a SQL join. 
# MAGIC
# MAGIC The `People10M` table has 10 million names in it. 
# MAGIC
# MAGIC > How many of the first names appear in Social Security data files? 
# MAGIC
# MAGIC To find out, use the `SSANames` table with first name popularity data from the United States Social Security Administration. 
# MAGIC
# MAGIC For every year from 1880 to 2014, `SSANames` lists the first names of people born in that year, their gender, and the total number of people given that name. 
# MAGIC
# MAGIC By joining the `People10M` table with `SSANames`, weed out the names that aren't represented in the Social Security data.
# MAGIC
# MAGIC (In a real application, you might use a join like this to filter out bad data.)

# COMMAND ----------

# MAGIC %md
# MAGIC Start by taking a quick peek at what `SSANames` looks like.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM SSANames

# COMMAND ----------

# MAGIC %md
# MAGIC Next, get an idea of how many distinct names there are in each of our tables, with a quick count of distinct names.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(DISTINCT firstName) 
# MAGIC FROM People10M

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(DISTINCT firstName)
# MAGIC FROM SSANames

# COMMAND ----------

# MAGIC %md
# MAGIC By introducing two more temporary views, each one consisting of distinct names, the join will be easier to read/write.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW SSADistinctNames AS 
# MAGIC   SELECT DISTINCT firstName AS ssaFirstName 
# MAGIC   FROM SSANames;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW PeopleDistinctNames AS 
# MAGIC   SELECT DISTINCT firstName 
# MAGIC   FROM People10M

# COMMAND ----------

# MAGIC %md
# MAGIC Next, join the two tables together to get the answer.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT firstName 
# MAGIC FROM PeopleDistinctNames 
# MAGIC INNER JOIN SSADistinctNames ON firstName = ssaFirstName

# COMMAND ----------

# MAGIC %md
# MAGIC How many are there?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) 
# MAGIC FROM PeopleDistinctNames 
# MAGIC INNER JOIN SSADistinctNames ON firstName = ssaFirstName

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nested Queries
# MAGIC
# MAGIC Joins are not the only way to solve the problem. 
# MAGIC
# MAGIC A sub-select works as well.

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/hs7vn1o0et?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/hs7vn1o0et?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(firstName) 
# MAGIC FROM PeopleDistinctNames
# MAGIC WHERE firstName IN (
# MAGIC   SELECT ssaFirstName FROM SSADistinctNames
# MAGIC )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Exercise 1
# MAGIC
# MAGIC In the tables above, some of the salaries in the `People10M` table are negative. 
# MAGIC
# MAGIC These salaries represent bad data. 
# MAGIC
# MAGIC Your job is to convert all the negative salaries to positive ones, and then sort the top 20 people by their salary.
# MAGIC
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** See the Apache Spark documentation, <a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions" target="_blank">built-in functions</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1
# MAGIC Create a temporary view called `PeopleWithFixedSalaries`, where all the negative salaries have been converted to positive numbers.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC
# MAGIC DROP TABLE IF EXISTS PeopleWithFixedSalaries;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW PeopleWithFixedSalaries AS
# MAGIC   SELECT firstName, middleName, lastName, gender, birthDate, ssn, abs(salary) AS salary
# MAGIC   FROM People10M

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC belowZero = spark.read.table("PeopleWithFixedSalaries").where("salary < 0").count()
# MAGIC dbTest("SQL-L3-belowZero", 0, belowZero)
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2
# MAGIC
# MAGIC Starting with the table `PeopleWithFixedSalaries`, create another view called `PeopleWithFixedSalariesSorted` where:
# MAGIC 0. The data set has been reduced to the first 20 records
# MAGIC 0. The records are sorted by the column `salary` in ascending order

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC
# MAGIC DROP TABLE IF EXISTS PeopleWithFixedSalariesSorted;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW PeopleWithFixedSalariesSorted AS
# MAGIC   SELECT * 
# MAGIC   FROM PeopleWithFixedSalaries
# MAGIC   ORDER BY salary
# MAGIC   LIMIT 20

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC resultsDF = spark.sql("select salary from PeopleWithFixedSalariesSorted")
# MAGIC dbTest("SQL-L3-count", 20, resultsDF.count())
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC results = [r[0] for r in resultsDF.collect()]
# MAGIC
# MAGIC dbTest("SQL-L3-fixedSalaries-0", 2, results[0])
# MAGIC dbTest("SQL-L3-fixedSalaries-1", 3, results[1])
# MAGIC dbTest("SQL-L3-fixedSalaries-2", 4, results[2])
# MAGIC
# MAGIC dbTest("SQL-L3-fixedSalaries-10", 19, results[10])
# MAGIC dbTest("SQL-L3-fixedSalaries-11", 19, results[11])
# MAGIC dbTest("SQL-L3-fixedSalaries-12", 20, results[12])
# MAGIC
# MAGIC dbTest("SQL-L3-fixedSalaries-17", 28, results[17])
# MAGIC dbTest("SQL-L3-fixedSalaries-18", 30, results[18])
# MAGIC dbTest("SQL-L3-fixedSalaries-19", 31, results[19])
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2
# MAGIC
# MAGIC As a refinement, assume that all salaries under $20,000 represent bad rows and filter them out.
# MAGIC
# MAGIC Additionally, categorize each person's salary into $10K groups.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1
# MAGIC Create a temporary view called `PeopleWithFixedSalaries20K` where:
# MAGIC 0. Start with the table `PeopleWithFixedSalaries`
# MAGIC 0. The data set excludes all records where salaries are below $20K
# MAGIC 0. The data set includes a new column called `salary10k`, that should be the salary in groups of 10,000. For example:
# MAGIC   * A salary of 23,000 should report a value of "2"
# MAGIC   * A salary of 57,400 should report a value of "6"
# MAGIC   * A salary of 1,231,375 should report a value of "123"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC
# MAGIC DROP TABLE IF EXISTS PeopleWithFixedSalaries20K;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW PeopleWithFixedSalaries20K AS
# MAGIC   SELECT *, round(salary / 10000) AS salary10k 
# MAGIC   FROM PeopleWithFixedSalaries 
# MAGIC   WHERE salary >= 20000

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC below2K = spark.sql("select * from PeopleWithFixedSalaries20K where salary < 20000").count()
# MAGIC dbTest("SQL-L3-count-salaries", 0, below2K)
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC resultsDF = spark.sql("select salary10k, count(*) as total from PeopleWithFixedSalaries20K  group by salary10k order by salary10k, total limit 5")
# MAGIC results = [ (str(int(r[0]))+" w/"+str(r[1])) for r in resultsDF.collect()]
# MAGIC
# MAGIC dbTest("SQL-L3-countSalaries-0", "2 w/43792", results[0])
# MAGIC dbTest("SQL-L3-countSalaries-1", "3 w/212630", results[1])
# MAGIC dbTest("SQL-L3-countSalaries-2", "4 w/536536", results[2])
# MAGIC dbTest("SQL-L3-countSalaries-3", "5 w/1055261", results[3])
# MAGIC dbTest("SQL-L3-countSalaries-4", "6 w/1623248", results[4])
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3
# MAGIC
# MAGIC Using the `People10M` table, count the number of females named Caren who were born before March 1980. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1
# MAGIC Starting with the table `People10M`, create a temporary view called `Carens` where:
# MAGIC 0. The result set has a single record
# MAGIC 0. The data set has a single column named `total`
# MAGIC 0. The result counts only 
# MAGIC   * Females (`gender`)
# MAGIC   * First Name is "Caren" (`firstName`)
# MAGIC   * Born before March 1980 (`birthDate`)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC
# MAGIC DROP TABLE IF EXISTS Carens;
# MAGIC
# MAGIC CREATE TEMPORARY VIEW Carens AS
# MAGIC   SELECT count(*) AS total 
# MAGIC   FROM People10M
# MAGIC   WHERE birthDate < '1980-03-01' AND firstName = 'Caren' AND gender = 'F'

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC rows = spark.sql("SELECT total FROM Carens").collect()
# MAGIC dbTest("SQL-L3-carens-len", 1, len(rows))
# MAGIC dbTest("SQL-L3-carens-total", 750, rows[0].total)
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC * Do the [Challenge Exercise]($./Optional/03-Joins-Aggregations).
# MAGIC * Start the next lesson, [Accessing Data]($./04-Accessing-Data).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC
# MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/index.html" target="_blank">Spark SQL Reference</a>
# MAGIC * <a href="http://spark.apache.org/docs/latest/sql-programming-guide.html" target="_blank">Spark SQL, DataFrames and Datasets Guide</a>
# MAGIC * <a href="https://databricks.com/blog/2017/08/31/cost-based-optimizer-in-apache-spark-2-2.html" target="_blank">Cost-based Optimizer in Apache Spark 2.2</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>