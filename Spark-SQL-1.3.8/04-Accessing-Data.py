# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Accessing Data
# MAGIC
# MAGIC Apache Spark&trade; and Databricks&reg; have numerous ways to access your data.
# MAGIC
# MAGIC ## In this lesson you
# MAGIC * Create a table from an existing file
# MAGIC * Create a table by uploading a data file from your local machine
# MAGIC * Mount an Azure Blob to DBFS
# MAGIC * Create tables for Databricks data sets to use throughout the course
# MAGIC
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Analysts
# MAGIC * Additional Audiences: Data Engineers and Data Scientists
# MAGIC
# MAGIC ## Prerequisites
# MAGIC * Web browser: Chrome or Firefox
# MAGIC * Lesson: <a href="$./01-Getting-Started">Getting Started</a>
# MAGIC * Concept: <a href="https://www.w3schools.com/sql/" target="_blank">Basic SQL</a>
# MAGIC
# MAGIC <h2 style="color:red">WARNING!</h2> This notebook must be run using Databricks runtime 4.0 or better.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a table from an existing file
# MAGIC
# MAGIC The <a href="https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html" target="_blank">Databricks File System</a> (DBFS) is the built-in, Azure-blob-backed, alternative to the <a href="http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html" target="_blank">Hadoop Distributed File System</a> (HDFS).
# MAGIC
# MAGIC Creating a table from an existing file in DBFS allows you to access the file as if it were a Spark table. It does **not** copy any data.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC The example below creates a table from the **ip-geocode.parquet** file (if it doesn't exist).
# MAGIC
# MAGIC For Parquet files, you need to specify only one option: the path to the file.
# MAGIC
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> A Parquet "file" is actually a collection of files stored in a single directory.  The Parquet format offers features making it the ideal choice for storing "big data" on distributed file systems. For more information, see <a href="https://parquet.apache.org/" target="_blank">Apache Parquet</a>.
# MAGIC
# MAGIC You can create a table from an existing DBFS file with a simple SQL `CREATE TABLE` statement. If you don't select a database, the database called "default" is used. Here, we'll use a database called "junk", to remind us to delete these tables later.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS databricks;
# MAGIC
# MAGIC USE databricks;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS IPGeocode
# MAGIC   USING parquet
# MAGIC   OPTIONS (
# MAGIC     path "dbfs:/mnt/training/ip-geocode.parquet"
# MAGIC   )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Now the table has been defined. You can see it in Databricks.
# MAGIC 0. Click the **Data** icon on the left sidebar<br/>
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/data-tab.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px"/></div>
# MAGIC 0. Select the database **databricks**
# MAGIC 0. Select the table **ipgeocode**
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Right-click and open in a new tab, so you don't lose your place in this notebook.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/SQL-MSFT/create-table-1-databricks-sql.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; width: auto; height: auto; max-height: 383px"/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC You see the schema of the table, along with a sample of its data.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/SQL-MSFT/db-table-example-1.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### File formats other than Parquet
# MAGIC
# MAGIC You can also create a table from other file formats. 
# MAGIC
# MAGIC One common format is CSV (comma-separated-values) for which you can specify:
# MAGIC * The file's delimiter, the default is "**,**"
# MAGIC * Whether the file has a header or not, the default is **false**
# MAGIC * Whether or not to infer the schema, the default is **false**

# COMMAND ----------

# MAGIC %md
# MAGIC In order to know which options to use, look at the first couple of lines of the file.
# MAGIC
# MAGIC Take a look at the head of the file **/mnt/training/bikeSharing/data-001/day.csv.**

# COMMAND ----------

# MAGIC %fs head /mnt/training/bikeSharing/data-001/day.csv --maxBytes=492

# COMMAND ----------

# MAGIC %md
# MAGIC Spark can create a table from that CSV file, as well.
# MAGIC
# MAGIC As you can see above:
# MAGIC * There is a header
# MAGIC * The file is comma separated (the default)
# MAGIC * Let Spark infer what the schema is

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS BikeSharingDay
# MAGIC   USING csv
# MAGIC   OPTIONS (
# MAGIC     path "/mnt/training/bikeSharing/data-001/day.csv",
# MAGIC     inferSchema "true",
# MAGIC     header "true"
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC Now the table is defined: view its contents with a simple select statement.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM BikeSharingDay

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Next, drop the table.
# MAGIC
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This does not delete the file from which the table was created.  Rather, it simply removes the table definition from Spark.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE BikeSharingDay

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upload a local file as a table
# MAGIC
# MAGIC The last two examples use files already loaded on the "server."
# MAGIC
# MAGIC Databricks also supports creating tables by uploading files. 
# MAGIC
# MAGIC Next, download the following file to your local machine: <a href="https://dbtrainwestus.blob.core.windows.net/training/dataframes/state-income.csv?sp=rl&st=2018-08-23T21:08:25Z&se=2024-08-24T21:08:00Z&sv=2017-11-09&sig=7fD9Zc5OZ9AOBdstZGyNrbvX%2FmNUiBYBbPtbtVrmiUY%3D&sr=b">state-income.csv</a>

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/3vo1bm6ak0?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/3vo1bm6ak0?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC 1. Select **Data** from the sidebar, and click the **junk** database
# MAGIC 2. Select the **+** icon to create a new table
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/create-table-1-junk-db.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; width: auto; height: auto; max-height: 383px"/>
# MAGIC
# MAGIC <br>
# MAGIC 1. Select **Upload File**
# MAGIC 2. click on Browse and select the **state-income.csv** file from your machine, or drag-and-drop the file to initiate the upload
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/DataFrames-MSFT/create-table-2.png" style="border: 1px solid #aaa; border-radius: 5px 5px 5px 5px; width: auto; height: auto; max-height: 300px  "/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Once the file is uploaded, create the actual table:
# MAGIC
# MAGIC 1. Click the **Create Table with UI** button  
# MAGIC <br>
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/DataFrames-MSFT/create-table-3.png" style="border: 1px solid #aaa; border-radius: 5px 5px 5px 5px; width: auto; height: auto; max-height: 500px  "/>
# MAGIC <br>
# MAGIC 2. In the drop-down dialog, select a cluster
# MAGIC 3. Click the **Preview Table** button  
# MAGIC <br>
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/DataFrames-MSFT/create-table-4.png" style="border: 1px solid #aaa; border-radius: 5px 5px 5px 5px; width: auto; height: auto; max-height: 200px  "/>
# MAGIC 4. Another dialog will drop down. Choose the **junk** database
# MAGIC 5. Select the **First row is header** checkbox
# MAGIC 6. Click the **Create Table** button
# MAGIC <br>
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/create-table-5.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; margin-top: 20px"/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Once Databricks finishes processing the file, you'll see another table preview.
# MAGIC
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Databricks tries to choose a table name that doesn't clash with tables created by other users. However, a name clash is still possible. If the table already exists, you'll see an error like the following:
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/create-table-6.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; margin-top: 20px; padding: 10px"/>
# MAGIC
# MAGIC If that happens, just type in a different table name, and try again.

# COMMAND ----------

# MAGIC %md
# MAGIC Next, drop the table to ensure other users don't have a name conflict when uploading their tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS state_income

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### How to Mount an Azure Blob to DBFS
# MAGIC
# MAGIC Microsoft Azure provides cloud file storage in the form of the Blob Store.  Files are stored in "blobs."
# MAGIC If you have an Azure account, create a blob, store data files in that blob, and mount the blob as a DBFS directory. 
# MAGIC
# MAGIC Once the blob is mounted as a DBFS directory, access it without exposing your Azure Blob Store keys.

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at the blobs already mounted to your DBFS:

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Mount a Databricks Azure blob (using read-only access and secret key pair), access one of the files in the blob as a DBFS path, then unmount the blob.
# MAGIC
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> The mount point **must** start with `/mnt/`.
# MAGIC
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> If the directory was already mounted, you would receive the following error:
# MAGIC
# MAGIC > Directory already mounted: /mnt/temp-training
# MAGIC
# MAGIC In this case, use a different mount point such as `temp-training-2`, and ensure you update all three references below.
# MAGIC
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> the next cell is in Scala!

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val SasURL = "https://dbtraineastus2.blob.core.windows.net/?sv=2017-07-29&ss=b&srt=sco&sp=rl&se=2023-04-19T06:32:30Z&st=2018-04-18T22:32:30Z&spr=https&sig=BB%2FQzc0XHAH%2FarDQhKcpu49feb7llv3ZjnfViuI9IWo%3D"
# MAGIC val SasKey = SasURL.slice(SasURL.indexOf('?'), SasURL.length)
# MAGIC val StorageAccount = "dbtraineastus2"
# MAGIC val ContainerName = "training"
# MAGIC val MountPoint = "/mnt/temp-training"
# MAGIC dbutils.fs.mount(
# MAGIC   source = s"wasbs://$ContainerName@$StorageAccount.blob.core.windows.net/",
# MAGIC   mountPoint = MountPoint,
# MAGIC   extraConfigs = Map(s"fs.azure.sas.$ContainerName.$StorageAccount.blob.core.windows.net" -> SasKey)
# MAGIC )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### Creating a Shared Access Signature (SAS) URL
# MAGIC Azure provides you with a secure way to create and share access keys for your Azure Blob Store without compromising your account keys.
# MAGIC
# MAGIC More details are provided <a href="http://docs.microsoft.com/en-us/azure/storage/common/storage-dotnet-shared-access-signature-part-1" target="_blank"> in this document</a>.
# MAGIC
# MAGIC This allows access to your Azure Blob Store data directly from Databricks distributed file system (DBFS).
# MAGIC
# MAGIC As shown in the screen shot, in the Azure Portal, go to the storage account containing the blob to be mounted. Then:
# MAGIC
# MAGIC 1. Select Shared access signature from the menu.
# MAGIC 2. Click the Generate SAS button.
# MAGIC 3. Copy the entire Blog service SAS URL to the clipboard.
# MAGIC 4. Use the URL in the mount operation, as shown below.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/DataFrames-MSFT/create-sas-keys.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; margin-top: 20px; padding: 10px"/>

# COMMAND ----------

# MAGIC %md
# MAGIC List the contents of the directory you just mounted:

# COMMAND ----------

# MAGIC %fs ls /mnt/temp-training

# COMMAND ----------

# MAGIC %md
# MAGIC Take a peek at the head of the file `auto-mpg.csv`:

# COMMAND ----------

# MAGIC %fs head /mnt/temp-training/auto-mpg.csv

# COMMAND ----------

# MAGIC %md
# MAGIC Now you are done, unmount the directory.

# COMMAND ----------

# MAGIC %fs unmount /mnt/temp-training

# COMMAND ----------

type(sum([1,2])) #int (3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Databricks allows you to:
# MAGIC   * Create DataFrames from existing data
# MAGIC   * Create DataFrames from uploaded files
# MAGIC   * Mount your own Azure blobs

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Review Questions
# MAGIC **Q:** What is Azure Blob Store?  
# MAGIC **A:** Blob Storage stores from hundreds to billions of objects such as unstructured data—images, videos, audio, documents easily and cost-effectively.
# MAGIC
# MAGIC **Q:** What is DBFS?  
# MAGIC **A:** DBFS stands for Databricks File System.  DBFS provides for the cloud what the Hadoop File System (HDFS) provides for local spark deployments.  DBFS uses Azure Blob Store and makes it easy to access files by name.
# MAGIC
# MAGIC **Q:** Which is more efficient to query, a parquet file or a CSV file?  
# MAGIC **A:** Parquet files are highly optimized binary formats for storing tables.  The overhead is less than required to parse a CSV file.  Parquet is the big data analogue to CSV as it is optimized, distributed, and more fault tolerant than CSV files.
# MAGIC
# MAGIC **Q:** How can you create a new table?  
# MAGIC **A:** Create new tables by either:
# MAGIC * Uploading a new file using the Data tab on the left.
# MAGIC * Mounting an existing file from DBFS.
# MAGIC
# MAGIC **Q:** What is the SQL syntax for defining a table in Spark from an existing parquet file in DBFS?  
# MAGIC **A:** ```CREATE TABLE IF NOT EXISTS IPGeocode
# MAGIC USING parquet
# MAGIC OPTIONS (
# MAGIC   path "dbfs:/mnt/training/ip-geocode.parquet"
# MAGIC )```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Start the next lesson, [Querying JSON & Hierarchical Data with SQL]($./05-Querying-JSON).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC
# MAGIC * <a href="https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html" target="_blank">The Databricks DBFS File System</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>