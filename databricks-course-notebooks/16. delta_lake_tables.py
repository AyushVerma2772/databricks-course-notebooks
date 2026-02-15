# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS demo
# MAGIC LOCATION 'abfss://demo@formula1strgaccav.dfs.core.windows.net/';

# COMMAND ----------

races_df = spark.read.csv('abfss://raw@formula1strgaccav.dfs.core.windows.net/2021-03-28/races.csv', inferSchema=True, header=True)

# COMMAND ----------

races_df.write.format("delta").mode("overwrite").saveAsTable("demo.managed_races_table")

# COMMAND ----------

races_df.write.format("parquet").mode("overwrite").save("abfss://demo@formula1strgaccav.dfs.core.windows.net/races_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE demo.races_df 
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://demo@formula1strgaccav.dfs.core.windows.net/races_df';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.races_df;

# COMMAND ----------

race_df_delta = spark.read.format("delta").load("abfss://demo@formula1strgaccav.dfs.core.windows.net/races_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE demo CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table demo.parquet_demo_delta_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- creating a delta parquet table
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS demo.external_demo_parquet_table (
# MAGIC   num INT
# MAGIC )
# MAGIC USING PARQUET
# MAGIC LOCATION "abfss://demo@formula1strgaccav.dfs.core.windows.net/external_demo_parquet_table";
# MAGIC
# MAGIC
# MAGIC INSERT INTO demo.external_demo_parquet_table VALUES (1),(2),(3);
# MAGIC
# MAGIC SELECT * FROM demo.external_demo_parquet_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- try to update parquet table
# MAGIC
# MAGIC UPDATE demo.external_demo_parquet_table
# MAGIC SET num = 100
# MAGIC WHERE num = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.external_demo_parquet_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- creating a delta external table
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS demo.external_demo_delta_table (
# MAGIC   num INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://demo@formula1strgaccav.dfs.core.windows.net/external_demo_delta_table";
# MAGIC
# MAGIC
# MAGIC INSERT INTO demo.external_demo_delta_table VALUES (1),(2),(3);
# MAGIC
# MAGIC SELECT * FROM demo.external_demo_delta_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- we can update the delta table
# MAGIC
# MAGIC UPDATE demo.external_demo_delta_table
# MAGIC SET num = 100
# MAGIC WHERE num = 1;
# MAGIC     
# MAGIC select * from demo.external_demo_delta_table;

# COMMAND ----------

# using python
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, 'abfss://demo@formula1strgaccav.dfs.core.windows.net/external_demo_delta_table')

deltaTable.update(
    condition="num = 2",
    set={"num": "num * 20"}
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.external_demo_delta_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- we also can't delte the data in parquet format
# MAGIC
# MAGIC delete 
# MAGIC from demo.external_demo_parquet_table
# MAGIC where num = 100;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- but we can delete the data from delta table
# MAGIC
# MAGIC delete 
# MAGIC from demo.external_demo_delta_table
# MAGIC where num = 100; 
# MAGIC
# MAGIC
# MAGIC select * from demo.external_demo_delta_table;

# COMMAND ----------

# using python
deltaTable.delete(condition="num = 40")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.external_demo_delta_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE

# COMMAND ----------

drivers_day1_df = spark.read \
.option("inferSchema", True) \
.json("abfss://raw@formula1strgaccav.dfs.core.windows.net/2021-03-28/drivers.json") \
.filter("driverId <= 10") \
.select("driverId", "dob", "name.forename", "name.surname")

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
.option("inferSchema", True) \
.json("abfss://raw@formula1strgaccav.dfs.core.windows.net/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 6 AND 15") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read \
.option("inferSchema", True) \
.json("abfss://raw@formula1strgaccav.dfs.core.windows.net/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS demo.drivers_merge (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MERGE
# MAGIC MERGE INTO demo.drivers_merge target
# MAGIC USING drivers_day1 source
# MAGIC ON source.driverId = target.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET 
# MAGIC         target.driverId = source.driverId,
# MAGIC         target.dob = source.dob,
# MAGIC         target.forename = source.forename,
# MAGIC         target.surname = source.surname,
# MAGIC         target.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (driverId, dob, forename, surname, createdDate) VALUES (source.driverId, source.dob, source.forename, source.surname, current_timestamp)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MERGE
# MAGIC MERGE INTO demo.drivers_merge target
# MAGIC USING drivers_day2 source
# MAGIC ON source.driverId = target.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET 
# MAGIC         target.driverId = source.driverId,
# MAGIC         target.dob = source.dob,
# MAGIC         target.forename = source.forename,
# MAGIC         target.surname = source.surname,
# MAGIC         target.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (driverId, dob, forename, surname, createdDate) VALUES (source.driverId, source.dob, source.forename, source.surname, current_timestamp)
# MAGIC

# COMMAND ----------

from delta.tables import *

driver_df = DeltaTable.forPath(spark, 'abfss://demo@formula1strgaccav.dfs.core.windows.net/drivers_merge')

driver_df.alias('target') \
  .merge(
    drivers_day3_df.alias('source'),
    'target.driverId = source.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
      "target.driverId" : "source.driverId",
      "target.dob" : "source.dob",
      "target.forename" : "source.forename",
      "target.surname" : "source.surname",
      "target.updatedDate" : "current_timestamp",
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "target.driverId" : "source.driverId",
      "target.dob" : "source.dob",
      "target.forename" : "source.forename",
      "target.surname" : "source.surname",
      "target.createdDate" : "current_timestamp",
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql select * from demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC How to check the history of the table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC How to time travel in Delta Tales

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo.drivers_merge VERSION AS OF 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.drivers_merge timestamp as of '2026-01-23T13:52:12.000+00:00'

# COMMAND ----------

# using pyspark
driver_merge_df = spark.read.format("delta").option("versionAsOf", "2").load("abfss://demo@formula1strgaccav.dfs.core.windows.net/drivers_merge")

driver_merge_df

# COMMAND ----------

# MAGIC %md
# MAGIC How to delete the `History` (historical data)

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum demo.drivers_merge;
# MAGIC -- it will delete the history of 7 days

# COMMAND ----------

# MAGIC %md
# MAGIC How to restore the data 

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM demo.drivers_merge WHERE driverId <= 5;
# MAGIC
# MAGIC select * from demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from demo.drivers_merge version as of 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO demo.drivers_merge t
# MAGIC USING (select * from demo.drivers_merge version as of 5) s
# MAGIC ON (t.driverId = s.driverId)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT *;
# MAGIC
# MAGIC
# MAGIC SELECT * FROM demo.drivers_merge;
# MAGIC

# COMMAND ----------

