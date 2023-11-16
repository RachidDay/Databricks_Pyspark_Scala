# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Project: Exploratory Data Analysis
# MAGIC Perform exploratory data analysis (EDA) to gain insights from a data lake.
# MAGIC
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Analysts
# MAGIC * Additional Audiences: Data Engineers and Data Scientists
# MAGIC
# MAGIC ## Prerequisites
# MAGIC * Web browser: Chrome or Firefox
# MAGIC * Lesson: <a href="$./04-Querying-Files">Querying Files with SQL</a>
# MAGIC * Lesson: <a href="$./05-Querying-Lakes">Querying Data Lakes with SQL</a>
# MAGIC * Concept: <a href="https://www.w3schools.com/sql/" target="_blank">Basic SQL</a>
# MAGIC
# MAGIC ## Instructions
# MAGIC
# MAGIC In `dbfs:/mnt/training/crime-data-2016`, there are a number of Parquet files containing 2016 crime data from seven United States cities:
# MAGIC
# MAGIC * New York
# MAGIC * Los Angeles
# MAGIC * Chicago
# MAGIC * Philadelphia
# MAGIC * Dallas
# MAGIC * Boston
# MAGIC
# MAGIC
# MAGIC The data is cleaned up a little but has not been normalized. Each city reports crime data slightly differently, so you have to
# MAGIC examine the data for each city to determine how to query it properly.
# MAGIC
# MAGIC Your job is to use some of this data to gain insights about certain kinds of crimes.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 1
# MAGIC
# MAGIC Start by creating temporary views for Los Angeles, Philadelphia, and Dallas
# MAGIC
# MAGIC Use `CREATE TEMPORARY VIEW` to create named views for the files you choose. Use a similar syntax as `CREATE TABLE`:
# MAGIC
# MAGIC ```
# MAGIC CREATE OR REPLACE TEMPORARY VIEW name
# MAGIC   USING parquet
# MAGIC   OPTIONS (
# MAGIC     ...
# MAGIC   )
# MAGIC ```
# MAGIC
# MAGIC Use the following view names:
# MAGIC
# MAGIC | City          | Table Name              | Path to DBFS file
# MAGIC | ------------- | ----------------------- | -----------------
# MAGIC | Los Angeles   | `CrimeDataLosAngeles`   | `dbfs:/mnt/training/crime-data-2016/Crime-Data-Los-Angeles-2016.parquet`
# MAGIC | Philadelphia  | `CrimeDataPhiladelphia` | `dbfs:/mnt/training/crime-data-2016/Crime-Data-Philadelphia-2016.parquet`
# MAGIC | Dallas        | `CrimeDataDallas`       | `dbfs:/mnt/training/crime-data-2016/Crime-Data-Dallas-2016.parquet`
# MAGIC
# MAGIC
# MAGIC
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You learned how to create a table from an external file in [Lesson 3]($./03-Accessing-Data). The syntax is exactly the same, except that you use `CREATE OR REPLACE TEMPORARY VIEW` instead of `CREATE TABLE IF EXISTS`.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Los Angeles

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CrimeDataLosAngeles
# MAGIC   USING parquet
# MAGIC   OPTIONS (
# MAGIC     path "dbfs:/mnt/training/crime-data-2016/Crime-Data-Los-Angeles-2016.parquet"
# MAGIC   )

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC rowsLosAngeles = spark.sql('SELECT count(*) FROM CrimeDataLosAngeles').collect()[0][0]
# MAGIC dbTest("SQL-L7-crimeDataLA-count", 217945, rowsLosAngeles)
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Philadelphia

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CrimeDataPhiladelphia
# MAGIC USING parquet
# MAGIC OPTIONS (
# MAGIC   path "dbfs:/mnt/training/crime-data-2016/Crime-Data-Philadelphia-2016.parquet"
# MAGIC )

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC rowsPhiladelphia = spark.sql('SELECT count(*) FROM CrimeDataPhiladelphia').collect()[0][0]
# MAGIC dbTest("SQL-L7-crimeDataPA-count", 168664, rowsPhiladelphia)
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dallas

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CrimeDataDallas
# MAGIC USING parquet
# MAGIC OPTIONS (
# MAGIC   path "dbfs:/mnt/training/crime-data-2016/Crime-Data-Dallas-2016.parquet"
# MAGIC )

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC rowsDallas = spark.sql('SELECT count(*) FROM CrimeDataDallas').collect()[0][0]
# MAGIC dbTest("SQL-L7-crimeDataDAL-count", 99642, rowsDallas) 
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 2
# MAGIC
# MAGIC For each table, examine the data to figure out how to extract _robbery_ statistics.
# MAGIC
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Each city uses different values to indicate robbery. Some cities use "larceny", "burglary", and "robbery".  These challenges are common in data lakes.  To simplify things, restrict yourself to only the word "robbery" (and not attempted-roberty, larceny, or burglary).
# MAGIC
# MAGIC Explore the data for the three cities until you understand how each city records robbery information. If you don't want to worry about upper- or lower-case, remember that SQL has a `LOWER()` function that converts a column's value to lowercase.
# MAGIC
# MAGIC Create a temporary view containing only the robbery-related rows, as shown in the table below.
# MAGIC
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** For each table, focus your efforts on the column listed below.
# MAGIC
# MAGIC Focus on the following columns for each table:
# MAGIC
# MAGIC | Table Name              | Robbery View Name     | Column
# MAGIC | ----------------------- | ----------------------- | -------------------------------
# MAGIC | `CrimeDataLosAngeles`   | `RobberyLosAngeles`   | `crimeCodeDescription`
# MAGIC | `CrimeDataPhiladelphia` | `RobberyPhiladelphia` | `ucr_general_description`
# MAGIC | `CrimeDataDallas`       | `RobberyDallas`       | `typeOfIncident`

# COMMAND ----------

# MAGIC %md
# MAGIC #### Los Angeles

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RobberyLosAngeles AS
# MAGIC   SELECT * 
# MAGIC   FROM CrimeDataLosAngeles
# MAGIC   WHERE lower(crimeCodeDescription) = 'robbery'

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC totalLosAngeles = spark.sql("SELECT count(*) AS total FROM RobberyLosAngeles").collect()[0].total
# MAGIC dbTest("SQL-L7-robberyDataLA-count", 9048, totalLosAngeles)
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Philadelphia

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RobberyPhiladelphia AS
# MAGIC   SELECT * 
# MAGIC   FROM CrimeDataPhiladelphia
# MAGIC   WHERE lower(ucr_general_description) = 'robbery'

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC totalPhiladelphia = spark.sql("SELECT count(*) AS total FROM RobberyPhiladelphia").collect()[0].total
# MAGIC dbTest("SQL-L7-robberyDataPA-count", 6149, totalPhiladelphia)
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dallas

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RobberyDallas AS
# MAGIC   SELECT * 
# MAGIC   FROM CrimeDataDallas
# MAGIC   WHERE lower(typeOfIncident) LIKE 'robbery%'

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC totalDallas = spark.sql("SELECT count(*) AS total FROM RobberyDallas").collect()[0].total
# MAGIC dbTest("SQL-L7-robberyDataDAL-count", 6824, totalDallas)
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 3
# MAGIC
# MAGIC Now that you have views of only the robberies in each city, create temporary views for each city, summarizing the number of robberies in each month.
# MAGIC
# MAGIC Your views must contain two columns:
# MAGIC * `month`: The month number (e.g., 1 for January, 2 for February, etc.)
# MAGIC * `robberies`: The total number of robberies in the month
# MAGIC
# MAGIC Use the following temporary view names and date columns:
# MAGIC
# MAGIC
# MAGIC | City          | View Name     | Date Column 
# MAGIC | ------------- | ------------- | -------------
# MAGIC | Los Angeles   | `RobberiesByMonthLosAngeles` | `timeOccurred`
# MAGIC | Philadelphia  | `RobberiesByMonthPhiladelphia` | `dispatch_date_time`
# MAGIC | Dallas        | `RobberiesByMonthDallas` | `startingDateTime`
# MAGIC
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> For each city, figure out which column contains the date of the incident. Then, extract the month from that date.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Los Angeles

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RobberiesByMonthLosAngeles AS
# MAGIC   SELECT month(timeOccurred) AS month, count(*) AS robberies
# MAGIC   FROM RobberyLosAngeles
# MAGIC   GROUP BY month(timeOccurred)

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC la = { r.month : r.robberies for r in spark.sql("SELECT * FROM RobberiesByMonthLosAngeles ORDER BY month").collect() }
# MAGIC dbTest("SQL-L7-robberyByMonthLA-counts", {1: 719, 2: 675, 3: 709, 4: 713, 5: 790, 6: 698, 7: 826, 8: 765, 9: 722, 10: 814, 11: 764, 12: 853}, la)
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Philadelphia

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RobberiesByMonthPhiladelphia AS
# MAGIC   SELECT month(dispatch_date_time) AS month, count(*) as robberies
# MAGIC   FROM RobberyPhiladelphia
# MAGIC   GROUP BY month(dispatch_date_time)

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC philadelphia = { r.month : r.robberies for r in spark.sql("SELECT * FROM RobberiesByMonthPhiladelphia ORDER BY month").collect() }
# MAGIC dbTest("SQL-L7-robberyByMonthPA-counts", {1: 520, 2: 416, 3: 432, 4: 466, 5: 533, 6: 509, 7: 537, 8: 561, 9: 514, 10: 572, 11: 545, 12: 544}, philadelphia)
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dallas

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RobberiesByMonthDallas AS
# MAGIC   SELECT month(startingDateTime) AS month, count(*) as robberies
# MAGIC   FROM RobberyDallas
# MAGIC   GROUP BY month(startingDateTime)

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC dallas = { r.month : r.robberies for r in spark.sql("SELECT * FROM RobberiesByMonthDallas ORDER BY month").collect() }
# MAGIC dbTest("SQL-L7-robberyByMonthDAL-counts", {1: 743, 2: 435, 3: 412, 4: 594, 5: 615, 6: 495, 7: 535, 8: 627, 9: 512, 10: 603, 11: 589, 12: 664}, dallas)
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Step 4
# MAGIC
# MAGIC Plot the robberies per month for each of your three cities, producing a plot similar to the following:
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/robberies-by-month.png" style="max-width: 700px; border: 1px solid #aaaaaa; border-radius: 10px 10px 10px 10px"/>
# MAGIC
# MAGIC When you first run your cell, you'll get an HTML table as the result. To configure the plot,
# MAGIC
# MAGIC 1. Click the graph button
# MAGIC 2. If the plot doesn't look correct, click the **Plot Options** button
# MAGIC 3. Configure the plot similar to the following example
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/capstone-plot-1.png" style="width: 440px; margin: 10px; border: 1px solid #aaaaaa; border-radius: 10px 10px 10px 10px"/>
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/capstone-plot-2.png" style="width: 268px; margin: 10px; border: 1px solid #aaaaaa; border-radius: 10px 10px 10px 10px"/>
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/capstone-plot-3.png" style="width: 362px; margin: 10px; border: 1px solid #aaaaaa; border-radius: 10px 10px 10px 10px"/>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Los Angeles

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC
# MAGIC SELECT * FROM RobberiesByMonthLosAngeles ORDER BY month

# COMMAND ----------

# MAGIC %md
# MAGIC #### Philadelphia

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC
# MAGIC SELECT * FROM RobberiesByMonthPhiladelphia ORDER BY month

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dallas

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC
# MAGIC SELECT * FROM RobberiesByMonthDallas ORDER BY month

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 5
# MAGIC
# MAGIC Create another temporary view called `CombinedRobberiesByMonth`, that combines all three robberies-per-month views into one.
# MAGIC In creating this view, add a new column called `city`, that identifies the city associated with each row.
# MAGIC The final view will have the following columns:
# MAGIC
# MAGIC * `city`: The name of the city associated with the row (Use the strings "Los Angeles", "Philadelphia", and "Dallas".)
# MAGIC * `month`: The month number associated with the row
# MAGIC * `robbery`: The number of robbery in that month (for that city)
# MAGIC
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You may want to use `UNION` in this example to combine the three datasets.
# MAGIC
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** In Databricks, all table schemas are immutable and therefore standard SQL commands such as `ALTER…ADD` and `UPDATE…SET` do not work for adding the new "city" column. 
# MAGIC
# MAGIC Instead, new columns can be added by simply naming them in the `SELECT` statement within the `CREATE OR REPLACE TEMPORARY VIEW` statement.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CombinedRobberiesByMonth AS
# MAGIC   SELECT 'Los Angeles' AS city, * FROM RobberiesByMonthLosAngeles
# MAGIC     UNION ALL
# MAGIC   SELECT 'Philadelphia' AS city, * FROM RobberiesByMonthPhiladelphia
# MAGIC     UNION ALL
# MAGIC   SELECT 'Dallas' AS city, * FROM RobberiesByMonthDallas

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC results = [ (r.city, r.month, r.robberies) for r in spark.sql("SELECT * FROM CombinedRobberiesByMonth").collect() ]
# MAGIC
# MAGIC dbTest("SQL-L7-combinedRobberiesByMonth-counts-0", (u'Los Angeles', 12, 853), results[0])
# MAGIC dbTest("SQL-L7-combinedRobberiesByMonth-counts-10", (u'Los Angeles', 11, 764), results[10])
# MAGIC dbTest("SQL-L7-combinedRobberiesByMonth-counts-20", (u'Philadelphia', 7, 537), results[20])
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 6
# MAGIC
# MAGIC Graph the contents of `CombinedRobberiesByMonth`, producing a graph similar to the following. (The diagram below deliberately
# MAGIC uses different data.)
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/combined-homicides.png" style="width: 800px; border: 1px solid #aaaaaa; border-radius: 10px 10px 10px 10px"/>
# MAGIC
# MAGIC Adjust the plot options to configure the plot properly, as shown below:
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/capstone-plot-4.png" style="width: 362px; margin: 10px; border: 1px solid #aaaaaa; border-radius: 10px 10px 10px 10px"/>
# MAGIC
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Order your results by `month`, then `city`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC
# MAGIC SELECT * FROM CombinedRobberiesByMonth ORDER BY month, city

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7
# MAGIC
# MAGIC While the above graph is interesting, it's flawed: it's comparing the raw numbers of robberies, not the per capita robbery rates.
# MAGIC
# MAGIC The table (already created) called `CityData`  contains, among other data, estimated 2016 population values for all United States cities
# MAGIC with populations of at least 100,000. (The data is from [Wikipedia](https://en.wikipedia.org/wiki/List_of_United_States_cities_by_population).)
# MAGIC
# MAGIC * Use the population values in that table to normalize the robberies so they represent per-capita values (i.e. total robberies divided by population)
# MAGIC * Save your results in a temporary view called `RobberyRatesByCity`
# MAGIC * The robbery rate value must be stored in a new column, `robberyRate`
# MAGIC
# MAGIC Next, graph the results, as above.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RobberyRatesByCity AS
# MAGIC   SELECT CombinedRobberiesByMonth.city, month, (robberies / estPopulation2016) AS robberyRate
# MAGIC   FROM CombinedRobberiesByMonth
# MAGIC     INNER JOIN CityData
# MAGIC     ON CityData.city = CombinedRobberiesByMonth.City

# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC results = [ (r.city, r.month, "{0:0.6f}".format(r.robberyRate)) for r in spark.sql("SELECT * FROM RobberyRatesByCity").collect() ]
# MAGIC
# MAGIC dbTest("SQL-L7-roberryRatesByCity-counts-0", (u'Los Angeles', 12, '0.000215'), results[0])
# MAGIC dbTest("SQL-L7-roberryRatesByCity-counts-10", (u'Los Angeles', 11, '0.000192'), results[10])
# MAGIC dbTest("SQL-L7-roberryRatesByCity-counts-20", (u'Philadelphia', 7, '0.000343'), results[20])
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## IMPORTANT Next Steps
# MAGIC * Please complete this short [feedback survey](https://www.surveymonkey.com/r/WBMS7CG).  Your input is extremely important and will shape future development.
# MAGIC * Congratulations, you have completed the Spark SQL course!

# COMMAND ----------

# MAGIC %md
# MAGIC ## References
# MAGIC
# MAGIC The crime data used in this notebook comes from the following locations:
# MAGIC
# MAGIC | City          | Original Data 
# MAGIC | ------------- | -------------
# MAGIC | Boston        | <a href="https://data.boston.gov/group/public-safety" target="_blank">https&#58;//data.boston.gov/group/public-safety</a>
# MAGIC | Chicago       | <a href="https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2" target="_blank">https&#58;//data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2</a>
# MAGIC | Dallas        | <a href="https://www.dallasopendata.com/Public-Safety/Police-Incidents/tbnj-w5hb/data" target="_blank">https&#58;//www.dallasopendata.com/Public-Safety/Police-Incidents/tbnj-w5hb/data</a>
# MAGIC | Los Angeles   | <a href="https://data.lacity.org/A-Safe-City/Crime-Data-From-2010-to-Present/y8tr-7khq" target="_blank">https&#58;//data.lacity.org/A-Safe-City/Crime-Data-From-2010-to-Present/y8tr-7khq</a>
# MAGIC | New Orleans   | <a href="https://data.nola.gov/Public-Safety-and-Preparedness/Electronic-Police-Report-2016/4gc2-25he/data" target="_blank">https&#58;//data.nola.gov/Public-Safety-and-Preparedness/Electronic-Police-Report-2016/4gc2-25he/data</a>
# MAGIC | New York      | <a href="https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i" target="_blank">https&#58;//data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i</a>
# MAGIC | Philadelphia  | <a href="https://www.opendataphilly.org/dataset/crime-incidents" target="_blank">https&#58;//www.opendataphilly.org/dataset/crime-incidents</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>