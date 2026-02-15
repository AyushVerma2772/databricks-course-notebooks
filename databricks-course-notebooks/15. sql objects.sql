-- Databricks notebook source
-- MAGIC %run "../formula1-notebooks/includes/configuration"

-- COMMAND ----------

create database if not exists demo;

-- COMMAND ----------

show databases

-- COMMAND ----------

describe database extended demo;

-- COMMAND ----------

use demo;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Managed Tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{adls_path_presentation}/race_results").filter("race_year = 2020")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # creating using python
-- MAGIC
-- MAGIC race_results_df.write.format("parquet").saveAsTable("race_results_managed_table_py")
-- MAGIC

-- COMMAND ----------

use demo;

-- COMMAND ----------

show tables;

-- COMMAND ----------

select * from demo.race_results_managed_table_py limit 2;

-- COMMAND ----------

describe extended demo.race_results_managed_table_py

-- COMMAND ----------

-- creating using sql

create table myManagedTable (
  id int, 
  name string
);

-- COMMAND ----------

show tables;

-- COMMAND ----------

insert into mymanagedtable values (1, 'Aman'), (2, "Ayush"), (3, "Aryan");

-- COMMAND ----------

select * from demo.mymanagedtable;

-- COMMAND ----------

describe extended demo.mymanagedtable;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### External table
-- MAGIC
-- MAGIC in the case of external table we only stores the metadata at databricks side and store the actual data at Azure Data lake storage.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # creating using python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{adls_path_presentation}/race_results_ext_table_py").saveAsTable("demo.race_results_ext_table_py")

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

describe extended demo.race_results_ext_table_py;

-- COMMAND ----------

-- creating using sql

create table myExternalTable (
  id int, 
  name string
)
using csv
location 'abfss://raw@formula1strgaccav.dfs.core.windows.net/myExternalTable'

-- COMMAND ----------

describe extended demo.myExternalTable;

-- COMMAND ----------

insert into demo.myexternaltable values (1, "Aman"), (2, "Ayush");

-- COMMAND ----------

select * from demo.myexternaltable;

-- COMMAND ----------

drop table demo.mymanagedtable;
drop table demo.myexternaltable;

-- COMMAND ----------

create view demo.v_mymanagedtable as select * from demo.mymanagedtable;

-- COMMAND ----------

create global temp view gv_mymanagedtable as select * from demo.mymanagedtable;

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

create temp view temp_v_mymanagedtable as (select * from demo.mymanagedtable);


-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo_db
LOCATION 'abfss://raw@formula1strgaccav.dfs.core.windows.net/'

-- COMMAND ----------

describe database extended demo_db;

-- COMMAND ----------

create table if not exists demo_db.demo_managed_table (
  num int
);

-- It is a managed table because we didn't give any location. But It is stored in the Database location.


-- COMMAND ----------

describe table extended demo_db.demo_managed_table;

-- COMMAND ----------

insert into demo_db.demo_managed_table values (1),(2),(3);

-- COMMAND ----------

drop table if exists demo_db.demo_managed_table;

-- so if we delete this. It will be deleted from the databricks as well as ADLS (DB location)

-- COMMAND ----------

-- Now if we give path while creating table it will be treated as external table
create table if not exists demo_db.demo_external_table (
  num int
)
LOCATION 'abfss://raw@formula1strgaccav.dfs.core.windows.net/demo_db/demo_external_table'

-- COMMAND ----------

describe table extended demo_db.demo_external_table;

-- COMMAND ----------

insert into demo_db.demo_external_table values (1),(2),(3);

-- COMMAND ----------

-- if we delete this. It will be only deleted from the databricks not from ADLS (DB location)
drop table demo_db.demo_external_table;

-- COMMAND ----------

DROP DATABASE demo_db;

-- COMMAND ----------

