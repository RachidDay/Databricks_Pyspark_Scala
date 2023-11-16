# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Challenge Exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom".

# COMMAND ----------

# MAGIC %run "../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %run "../Includes/Test-Library"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge Exercise 3
# MAGIC
# MAGIC Use the `SSANames` table to find the most popular first name for girls in 1885, 1915, 1945, 1975, and 2005.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 1
# MAGIC Create a temporary view called `HistoricNames` where:
# MAGIC 0. The table `HistoricNames` is created using a **single** SQL query.
# MAGIC 0. The result has three columns:
# MAGIC   * `firstName`
# MAGIC   * `year`
# MAGIC   * `total`
# MAGIC
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Explore the data before crafting your solution.

# COMMAND ----------

step1= spark.table("SSANames").select("firstName","year","total").createOrReplaceTempView("HistoricNames")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2
# MAGIC Display the table `HistoricNames`.

# COMMAND ----------

# DBTITLE 1,I will first display the SSANames to see its content
# MAGIC %sql
# MAGIC SELECT firstName, year, total
# MAGIC FROM SSANames

# COMMAND ----------

# DBTITLE 1,Selection of gender and max on total for the female gender 'F'.
# MAGIC  %sql
# MAGIC
# MAGIC   SELECT year, gender, max(total) AS total
# MAGIC   FROM  SSANames
# MAGIC   WHERE gender='F' AND year IN (1885, 1915, 1945, 1975, 2005)  GROUP BY year, gender 

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table("SSANames")
result = (df.filter((df.gender == 'F') & df.year.isin(1885, 1915, 1945, 1975, 2005)).groupBy("year", "gender").agg(F.max("total").alias("total")).orderBy("year"))
result.show()


# COMMAND ----------

# DBTITLE 1,Now I will apply a natural joint to avoid alias problem on tables where gender is 'F' 
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW HistoricNames AS
# MAGIC SELECT firstName, year, total
# MAGIC FROM SSANames NATURAL INNER JOIN (
# MAGIC   SELECT year, gender, max(total) AS total
# MAGIC   FROM  SSANames
# MAGIC   GROUP BY year, gender
# MAGIC ) AS frequent_names
# MAGIC WHERE gender='F' AND year IN (1885, 1915, 1945, 1975, 2005)
# MAGIC ORDER BY year

# COMMAND ----------


from pyspark.sql import SparkSession
from pyspark.sql import functions
spark = SparkSession.builder.appName("HistoricNames").getOrCreate()

ssanames_df= spark.table("SSANames")
frequent_names = (ssanames_df
                  .groupBy("year", "gender")
                  .agg(F.max("total").alias("total")))

historic_names = (ssanames_df
                  .join(frequent_names, ["year", "gender", "total"])
                  .filter((F.col("gender") == "F") & (F.col("year").isin([1885, 1915, 1945, 1975, 2005])))
                  .select("firstName", "year", "total")
                  .orderBy("year"))
historic_names.show()

historic_names.createOrReplaceTempView("HistoricNames")


# COMMAND ----------

# MAGIC %python
# MAGIC # TEST - Run this cell to test your solution.
# MAGIC
# MAGIC resultsDF = spark.sql("select firstName, year, total from HistoricNames order by total")
# MAGIC results = [ (r[0]+" "+str(r[1])+": "+str(r[2])) for r in resultsDF.collect()]
# MAGIC
# MAGIC dbTest("SQL-L3-Opt-historicNames-0", "Mary 1885: 9128", results[0])
# MAGIC dbTest("SQL-L3-Opt-historicNames-1", "Emily 2005: 23928", results[1])
# MAGIC dbTest("SQL-L3-Opt-historicNames-2", "Jennifer 1975: 58185", results[2])
# MAGIC dbTest("SQL-L3-Opt-historicNames-3", "Mary 1915: 58187", results[3])
# MAGIC dbTest("SQL-L3-Opt-historicNames-4", "Mary 1945: 59284", results[4])
# MAGIC
# MAGIC print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>