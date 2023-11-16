# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/training/crime-data-2016

# COMMAND ----------

# !pip install --upgrade pip
#!pip install pathlib2

# COMMAND ----------

from pyspark import 
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating temporary view for crime data in 3 locations:
# MAGIC
# MAGIC --  Los Angeles
# MAGIC
# MAGIC create or replace temporary view LA_CD
# MAGIC using parquet
# MAGIC options (path 'dbfs:/mnt/training/crime-data-2016/Crime-Data-Los-Angeles-2016.parquet');
# MAGIC
# MAGIC --  Philadelphia
# MAGIC
# MAGIC create or replace temporary view Philly_CD
# MAGIC using parquet
# MAGIC options (path 'dbfs:/mnt/training/crime-data-2016/Crime-Data-Philadelphia-2016.parquet');
# MAGIC
# MAGIC --  Dallas
# MAGIC
# MAGIC create or replace temporary view Dallas_CD
# MAGIC using parquet
# MAGIC options (path 'dbfs:/mnt/training/crime-data-2016/Crime-Data-Dallas-2016.parquet');
# MAGIC
# MAGIC -- select count(*) as rows_crime_data_in_LA from LA_CD; 
# MAGIC -- 217 945 lignes
# MAGIC -- select count(*) as rows_crime_data_in_Philly from Philly_CD; 
# MAGIC -- 168 664 lignes
# MAGIC -- select count(*) as rows_crime_data_in_Dallas from Dallas_CD; 
# MAGIC -- 99 642 lignes

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 2

# COMMAND ----------

LA_CD =  spark.sql('select * from LA_CD')
Philly_CD =  spark.sql('select * from Philly_CD')
Dallas_CD =  spark.sql('select * from Dallas_CD')

print(LA_CD.printSchema() , Philly_CD.printSchema() , Dallas_CD.printSchema())

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as number_of_RobberyCrimes_inLA from LA_CD  where crimeCodeDescription LIKE 'ROBBERY%';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Burglary Data in LA:
# MAGIC -- select * from LA_CD LIMIT 5;
# MAGIC -- select count(*) as number_of_RobberyCrimes_inLA from LA_CD  where crimeCodeDescription LIKE 'ROBBERY%';
# MAGIC
# MAGIC -- Burglary Data in Philadelphia:
# MAGIC -- select * from Philly_CD LIMIT 20;
# MAGIC -- select count(*) as number_of_RobberyCrimes_inPhilly from Philly_CD  where ucr_general_description LIKE 'ROBBERY%';
# MAGIC
# MAGIC -- Burglary Data in Dallas:
# MAGIC -- select * from Dallas_CD LIMIT 20;
# MAGIC -- select count(*) as number_of_RobberyCrimes_inDallas from Dallas_CD  where typeOfIncident LIKE 'ROBBERY%';
# MAGIC
# MAGIC -- union (aber Falsch)
# MAGIC select * from
# MAGIC (select count(*) as nb_of_robbery_crimes from LA_CD  where crimeCodeDescription LIKE 'ROBBERY%') union (select count(*) from Philly_CD  where ucr_general_description LIKE 'ROBBERY%')
# MAGIC union (select count(*) from Dallas_CD  where typeOfIncident LIKE 'ROBBERY%'); 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from Dallas_CD LIMIT 20;
# MAGIC select count(*) as number_of_RobberyCrimes_inDallas from Dallas_CD  where typeOfIncident LIKE 'ROBBERY%';

# COMMAND ----------

#In Python
robb_LA = spark.sql("select * from LA_CD  where crimeCodeDescription LIKE 'ROBBERY%' ")
robb_Philly = spark.sql("select * from Philly_CD  where ucr_general_description LIKE 'ROBBERY%' ")
robb_Dallas = spark.sql("select * from Dallas_CD  where typeOfIncident LIKE 'ROBBERY%' ")

print(robb_LA.count(), robb_Philly.count(), robb_Dallas.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 3 and 4
# MAGIC
# MAGIC *remarque*: les plots peuvent se faire sur les select de sql ou avec les dispaly des DataFrames de Pyspark

# COMMAND ----------

# Los Angeles
# robb_LA.select(month('timeOccurred').alias('month_occ')).distinct().sort('month_occ').show()

monthly_robb_LA =\
robb_LA.groupBy(month('timeOccurred').alias('month'))\
.agg(count('*').alias('robberies'))\
.sort('month')

monthly_robb_LA.createOrReplaceTempView("monthly_robb_LA")

# Philadelphia
# robb_Philly.select(month('dispatch_date_time').alias('month_occ')).distinct().sort('month_occ').show()

monthly_robb_Philly =\
robb_Philly.groupBy(month('dispatch_date_time').alias('month'))\
.agg(count('*').alias('robberies'))\
.sort('month')

monthly_robb_Philly.createOrReplaceTempView("monthly_robb_Philly")

# Dallas
# robb_Dallas.select(month('startingDateTime').alias('month_occ')).distinct().sort('month_occ').show()

monthly_robb_Dallas =\
robb_Dallas.groupBy(month('startingDateTime').alias('month'))\
.agg(count('*').alias('robberies'))\
.sort('month')

monthly_robb_Dallas.createOrReplaceTempView("monthly_robb_Dallas")

# display(monthly_robb_LA)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from monthly_robb_LA

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from monthly_robb_Philly

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from monthly_robb_Dallas

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 5 et 6
# MAGIC

# COMMAND ----------

monthly_robb_LA = monthly_robb_LA.withColumn('city', lit('Los Angeles'))
monthly_robb_Philly = monthly_robb_Philly.withColumn('city', lit('Philadelphia'))
monthly_robb_Dallas = monthly_robb_Dallas.withColumn('city', lit('Dallas'))

# monthlyrobberies_combined  = monthly_robb_LA.union(monthly_robb_Philly).union(monthly_robb_Dallas)

# COMMAND ----------

monthlyrobberies_combined  =\
monthly_robb_LA.union(monthly_robb_Philly).union(monthly_robb_Dallas)

monthlyrobberies_combined.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 7

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace temporary view citydata as select * from databricks.citydata; 
# MAGIC
# MAGIC select * from citydata;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- création de view de population estimée par ville en 2016
# MAGIC create or replace temporary view pop2016_city as (select sum(estpopulation2016) as estpopulation2016, city from citydata group by city);

# COMMAND ----------

# création de dataFrame depuis la view précédente
pop2016_city = spark.sql("select * from pop2016_city")

# COMMAND ----------

monthly_robb_LA.printSchema()
monthly_robb_Philly.printSchema()
monthly_robb_Dallas.printSchema()

# COMMAND ----------

robbery_city_LA = pop2016_city.join(monthly_robb_LA, monthly_robb_LA.city == pop2016_city.city, 'inner')

robbery_city_Philly = pop2016_city.join(monthly_robb_Philly, monthly_robb_Philly.city == pop2016_city.city, 'inner')

robbery_city_Dallas = pop2016_city.join(monthly_robb_Dallas , monthly_robb_Dallas .city == pop2016_city.city, 'inner')

# COMMAND ----------

robbery_city_combined = robbery_city_LA.union(robbery_city_Philly).union(robbery_city_Dallas).drop(pop2016_city.city)

# COMMAND ----------

robbery_rate_city = robbery_city_combined.withColumn("per capita robbery rate in %", round(col('robberies')/col('estpopulation2016'), 7)*100).drop('estpopulation2016')

# COMMAND ----------

display(robbery_rate_city)