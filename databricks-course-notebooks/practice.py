# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()


# COMMAND ----------

# import col from pyspark.sql.functions
from pyspark.sql.functions import *
from pyspark.sql.window import Window


# COMMAND ----------

# MAGIC %run "../formula1-notebooks/includes/configuration"

# COMMAND ----------

# it will create 2 partitions.
rdd = spark.sparkContext.parallelize([1, 2, 3, 4], numSlices=2)

# collect is used to pull all the data to the driver node.
print(rdd.collect())

# performing transformation 
rdd_sq = rdd.map(lambda x: x*x)

print(rdd_sq.collect())

# COMMAND ----------

data = [[1, "Ayush"], [2, "Aman"], [3, "Aryan"]]

# creating a dataframe using 2-d list
df = spark.createDataFrame(data, schema=["id", "name"])

df.show()

# COMMAND ----------

adls_path_raw

# COMMAND ----------

# MAGIC %fs
# MAGIC ls abfss://raw@formula1strgaccav.dfs.core.windows.net/2021-03-21/

# COMMAND ----------

df = spark.read.format("csv").options(**{"header": "true", "inferSchema": "true"}).load(f"{adls_path_raw}/2021-03-28/circuits.csv")

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs
# MAGIC ls abfss://demo@formula1strgaccav.dfs.core.windows.net/

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    BooleanType,
    DateType
)

emp_schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
    StructField("salary", DoubleType(), nullable=True),
    StructField("is_active", BooleanType(), nullable=True),
    StructField("join_date", DateType(), nullable=True),
    StructField("department", StringType(), nullable=True),
    StructField("rating", DoubleType(), nullable=True),
    StructField("email", StringType(), nullable=True)
])


# COMMAND ----------

emp_df = spark.read.csv(path="abfss://demo@formula1strgaccav.dfs.core.windows.net/employees.csv", header=True, schema=emp_schema, dateFormat="yyyy-MM-dd")

emp_df.limit(5).display()

# COMMAND ----------

# selecting employees with id, name and is_active
select_emp_df = emp_df.select("id", "name", "is_active")

select_emp_df.display()

# COMMAND ----------

select_emp_df = emp_df.select(col("id"), col("name"), col("is_active"))

# COMMAND ----------

select_emp_df = emp_df.select(col("id").alias("emp_id"), col("name").alias("emp_name"))

display_data(select_emp_df)

# COMMAND ----------

select_emp_df = emp_df.select(emp_df.id, emp_df.name, emp_df.is_active)

display_data(select_emp_df)

# COMMAND ----------

select_emp_df = emp_df.select(emp_df['id'], emp_df['name'], emp_df['is_active'])


display_data(select_emp_df)

# COMMAND ----------

# adding new column bonus using withColumn
new_emp_df = emp_df.withColumn("bonus", col("salary") * 0.05)

# adding new column total_salary using select 
new_emp_df = new_emp_df.select("*", (col("salary") + col("bonus")).alias("total_salary"))

display_data(new_emp_df)


# COMMAND ----------

# adding a constant value to a column
new_emp_df = emp_df.withColumn("country", lit("India"))

# using select
new_emp_df = emp_df.select("*", lit("India").alias("country"))

display_data(new_emp_df)

# COMMAND ----------

# rename a col with withColumnRename
new_emp_df = emp_df.withColumnRenamed("name", "full_name")

# rename a col with select
new_emp_df = emp_df.select(col("name").alias("full_name"))

display_data(new_emp_df)


# COMMAND ----------

# changind datatype of id column cast(datatype)
new_emp_df = emp_df.withColumn("id", col("id").cast(StringType()))


# COMMAND ----------

# single condition
new_emp_df = emp_df.filter(col("salary") > 130000)

# multiple condition
new_emp_df = emp_df.filter((col("salary") > 130000)\
                           & (col("is_active") == 'true')\
                           & (col("department") == 'IT')
                           )

# using where()
new_emp_df = emp_df.where(col("salary") > 130000)

# prefered way
new_emp_df = emp_df.where("salary > 130000")

new_emp_df.display()

# COMMAND ----------

# isNull() : filters rows where column value is NULL
new_emp_df = emp_df.filter(col("salary").isNull())

# isNotNull() : filters rows where column value is NOT NULL
new_emp_df = emp_df.filter(col("salary").isNotNull())

new_emp_df = emp_df.filter(~(col("salary").isNull()))

# isin() : filters rows where column value matches any value in a list
new_emp_df = emp_df.filter(col("department").isin(["IT", "HR"]))

# between() : filters rows where column value lies between two values
new_emp_df = emp_df.filter(col("salary").between(130000, 150000))

# like() : filters rows using SQL LIKE pattern matching
new_emp_df = emp_df.filter(col("name").like("%5"))

# startswith() : filters rows where column value starts with given string
new_emp_df = emp_df.filter(col("name").startswith("A"))

# endswith() : filters rows where column value ends with given string
new_emp_df = emp_df.filter(col("name").endswith("h"))

# contains() : filters rows where column value contains given string
new_emp_df = emp_df.filter(col("name").contains("am"))


# COMMAND ----------

# sort the data in ascending order by salary
new_emp_df = emp_df.sort(col("salary").asc())

# using orderBy
new_emp_df = emp_df.orderBy(col("salary").desc())

# multiple cols
new_emp_df = emp_df.sort(col("salary").desc(), col("age").asc_nulls_last())

display_data(new_emp_df)


# COMMAND ----------

emp_df.select(count("id"), max("salary"), min("age"), avg("salary")).display()

# COMMAND ----------

# group based on department 
emp_df.groupBy("department").agg(
    count("*").alias("total"),
    max("salary").alias("max_salary"),
    min("age").alias("min_age"),
    avg("salary").alias("avg_salary")
).display()

# group on multiple columns
emp_df.groupBy(col("department"), col("is_active")).count().orderBy(col("department"), col("is_active")).display()


# COMMAND ----------

# for single column
# emp_df.filter(col("is_active").isNull()).count()

# you can get the count of null values in each column
emp_df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in emp_df.columns  
]).display()

# COMMAND ----------

# drop the row if any of the columns is null
# new_emp_df = emp_df.dropna()

# only drop the row if any of these col is null
new_emp_df = emp_df.dropna(subset=["is_active", "join_date", "department"])

# COMMAND ----------

new_emp_df = new_emp_df.withColumn("email", concat(col("name"), lit("@example.com")))

# COMMAND ----------

# filling all nulls with 0
# new_emp_df = emp_df.fillna(0)

# filling salary, age, rating with 0
# new_emp_df = emp_df.fillna(0, subset=["salary", "age", "rating"])

# filling nulls with different values
# first will return the first row of the df
null_cols = {"salary": 0, "age": 0, "rating": 0}

for key in null_cols:
    null_cols[key] = emp_df.select(avg(key).cast("int")).first()[0]    

# null_cols = {'salary': 91822, 'age': 38, 'rating': 4}
new_emp_df = new_emp_df.fillna(null_cols)


# COMMAND ----------

new_emp_df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in emp_df.columns  
]).display()

# COMMAND ----------

emp_df = new_emp_df

# COMMAND ----------

# check dups
# emp_df.groupBy("id").count().filter(col("count") > 1).display()

# COMMAND ----------

# # distinct rows
# emp_df.distinct().display()

# # remove all dups 
# emp_df.dropDuplicates().display()

# # remove dups based on single cosl
# emp_df.dropDuplicates(["id"]).display()

# # remove dups based on specific columns
# emp_df.dropDuplicates(["id", "name"]).display()    
# # ["id", "name"] combination must be unique


# COMMAND ----------

# String functions
data = [("  ayush kumar  ", "ayush@example.com", "abc123")]
df = spark.createDataFrame(data, ["name", "email", "code"])
df.display()

df.select(
    col("name").alias("orignial_name"),
    trim(col("name")).alias("trimed_name"),
    lower(col("name")).alias("lower_name"),
    upper(col("name")).alias("upper_name"),
    initcap(col("name")).alias("initcap_name"),
    regexp_replace(col("name"), "kumar", "verma").alias("right_name"),
    length(col("name")).alias("len_name"),
    substring(col("name"), 1, 3).alias("sub_name"),
    instr(col("email"), "@").alias("pos_@"),
    concat_ws(" - ", col("name"), col("email")).alias("name_email"),
    split(col("email"), "@").alias("email")
).display()

# COMMAND ----------

# window_spec = Window.partitionBy("department").orderBy("salary")

# # get department wise salary rank
# emp_df.select("name", "department", "salary", rank().over(window_spec).alias("rank"))

# # when we dont define partitionBy, it will whole table / df as one window
# winodw_spec = Window.orderBy(col("age").desc())
# emp_df.withColumn("age_rank", dense_rank().over(winodw_spec))

# # sum
# window_spec = Window.partitionBy("department").orderBy("salary")
# emp_df.select("department", "salary", sum(col("salary")).over(window_spec).alias("sum_salary"))


# # lead and lag
# window_spec = Window.partitionBy("department").orderBy("salary")
# emp_df.select("department", "salary",
#             lead(col("salary")).over(window_spec).alias("lead_salary"),
#               lag(col("salary")).over(window_spec).alias("lag_salary"))

# COMMAND ----------

# MAGIC %md
# MAGIC SPARK SQL

# COMMAND ----------

# %sql
# -- creating a database
# CREATE DATABASE IF NOT EXISTS demodb;
# -- we did not provide a location for the database, so it will be created in the default location (warehouse)

# -- we can check it using DESCRIBE DATABASE command
# DESCRIBE DATABASE EXTENDED demodb;

# COMMAND ----------

# %sql
# -- creating a managed table inside demodb database
# CREATE TABLE IF NOT EXISTS demodb.managed_table (
#   id INT, 
#   name STRING
# )
# USING parquet;    -- it will store the actual data in parquet format.

# -- we did not provide any loc. so, it is a managed table

# -- we can check it using DESCRIBE TABLE command
# DESCRIBE TABLE EXTENDED demodb.managed_table;

# -- Type:	MANAGED
# -- Location	dbfs:/user/hive/warehouse/demodb.db/managed_table

# -- insert data inside managed_table
# INSERT INTO demodb.managed_table VALUES (1, "Aman"), (2, "Ayush");

# -- SELECT the data
# SELECT * FROM demodb.managed_table;

# -- DROP the table command will drop table schema as well as data
# DROP TABLE IF EXISTS demodb.managed_table;

# COMMAND ----------

# # saving dataframe as table in parquet format
# emp_df.write.format("parquet").mode("overwrite").saveAsTable("demodb.employees")

# COMMAND ----------

# %sql
# SELECT * FROM demodb.employees LIMIT 2;

# COMMAND ----------

# %sql
# -- creating a external table inside demodb database
# CREATE TABLE IF NOT EXISTS demodb.external_table (
#   id INT, 
#   name STRING
# )
# USING parquet   -- it will store the actual data in parquet format.

# -- we provided a location for the table, so it is an external table
# LOCATION 'abfss://demo@formula1strgaccav.dfs.core.windows.net/external_table';

# -- we can check it using DESCRIBE TABLE command
# DESCRIBE TABLE EXTENDED demodb.external_table;

# -- Type	EXTERNAL
# -- Location	'abfss://demo@formula1strgaccav.dfs.core.windows.net/external_table'

# -- insert data inside external_table
# INSERT INTO demodb.external_table VALUES (1, 'Aman'), (2, 'Ayush');

# -- SELECT the data
# SELECT * FROM demodb.external_table;

# -- DROP the table command will drop table schema only, not the data
# DROP TABLE IF EXISTS demodb.external_table;


# COMMAND ----------

# # Ḥere we store a df as external table in parquet format
# emp_df.write.format("parquet").mode("overwrite").option("path", 'abfss://demo@formula1strgaccav.dfs.core.windows.net/external_employees').saveAsTable("demodb.external_employees")


# COMMAND ----------

# %sql
# SELECT * FROM demodb.external_employees LIMIT 2;

# COMMAND ----------

# # We can also read it as parquet file
# emp_df_external_table = spark.read.parquet('abfss://demo@formula1strgaccav.dfs.core.windows.net/external_employees')

# display_data(emp_df_external_table)

# COMMAND ----------

# %sql
# DROP DATABASE IF EXISTS demodb CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS demodb_loc 
# MAGIC LOCATION 'abfss://demo@formula1strgaccav.dfs.core.windows.net/demodb_loc';

# COMMAND ----------

# %sql
# -- creating table inside deomdb_loc database
# CREATE TABLE IF NOT EXISTS demodb_loc.managed_table (
#   id INT, 
#   name STRING
# )
# USING parquet;

# -- we did not provide any loc. so, it is a managed table

# -- we can check it using DESCRIBE TABLE command
# DESCRIBE TABLE EXTENDED demodb_loc.managed_table;

# -- Type:	MANAGED
# -- Location abfss://demo@formula1strgaccav.dfs.core.windows.net/demodb_loc/managed_table

# -- insert data inside managed_table
# INSERT INTO demodb_loc.managed_table VALUES (1, "Aman"), (2, "Ayush");

# -- SELECT the data
# SELECT * FROM demodb_loc.managed_table;

# -- DROP the table command will drop table schema as well as data even it is stored in ADLS loc.
# DROP TABLE IF EXISTS demodb_loc.managed_table;

# COMMAND ----------

# %sql
# -- creating table inside deomdb_loc database
# CREATE TABLE IF NOT EXISTS demodb_loc.external_table (
#   id INT, 
#   name STRING
# )
# USING parquet
# LOCATION 'abfss://demo@formula1strgaccav.dfs.core.windows.net/external_loc/external_table';

# -- we provide adls loc. so, it is a external table

# -- we can check it using DESCRIBE TABLE command
# DESCRIBE TABLE EXTENDED demodb_loc.external_table;

# -- Type:	EXTERNAL
# -- Location abfss://demo@formula1strgaccav.dfs.core.windows.net/external_loc/external_table

# -- insert data inside external_table
# INSERT INTO demodb_loc.external_table VALUES (1, "Aman"), (2, "Ayush");

# -- SELECT the data
# SELECT * FROM demodb_loc.external_table;

# -- DROP the table command will only drop table schema not data which is stored ADLS loc.
# DROP TABLE IF EXISTS demodb_loc.external_table;

# COMMAND ----------

# %sql
# DROP DATABASE IF EXISTS demodb_loc CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC SPARK VIEWS

# COMMAND ----------

# temperory view
emp_df.createOrReplaceTempView("temp_emp_df_v")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM temp_emp_df_v LIMIT 5;

# COMMAND ----------

# global temp view
emp_df.createGlobalTempView("global_temp_emp_df_v")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.global_temp_emp_df_v LIMIT 5;

# COMMAND ----------

emp_df.write.format("parquet").mode("overwrite").saveAsTable("demodb_loc.emp_managed_table")

# COMMAND ----------

# %sql
# -- creating temp view
# CREATE OR REPLACE TEMPORARY VIEW emp_temp_v AS (
#   SELECT * FROM demodb_loc.emp_managed_table
#   WHERE department = 'HR'
# );

# -- creating global temp view
# CREATE OR REPLACE GLOBAL TEMPORARY VIEW emp_global_temp_v AS (
#   SELECT * FROM demodb_loc.emp_managed_table
#   WHERE department = 'IT'
# );

# -- creating permanent view
# CREATE OR REPLACE VIEW demodb_loc.emp_permanent_v AS (
#   SELECT * FROM demodb_loc.emp_managed_table
#   WHERE department = 'Sales'
# );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.emp_global_temp_v LIMIT 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demodb_loc.emp_permanent_v LIMIT 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN demodb_loc;

# COMMAND ----------

# MAGIC %md
# MAGIC DELTA TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- -- creating a delta table
# MAGIC -- CREATE TABLE IF NOT EXISTS demodb_loc.managed_demo_delta_table (
# MAGIC --   id INT, name STRING
# MAGIC -- )
# MAGIC -- USING DELTA;
# MAGIC
# MAGIC
# MAGIC -- -- inserting data
# MAGIC -- INSERT INTO demodb_loc.managed_demo_delta_table VALUES (1, "John"), (2, "Mary"), (3, "Mike"), (4, "Sue"), (5, "Raj");
# MAGIC
# MAGIC -- SELECT * FROM demodb_loc.managed_demo_delta_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- -- in parquet format we can't delete or update the data but in delta format we can delete or update the data
# MAGIC
# MAGIC -- -- updating the data
# MAGIC -- UPDATE demodb_loc.managed_demo_delta_table 
# MAGIC -- SET name = "Ayush" WHERE id = 1;
# MAGIC
# MAGIC -- -- deleting data
# MAGIC -- DELETE FROM demodb_loc.managed_demo_delta_table WHERE id = 5;
# MAGIC
# MAGIC -- SELECT * FROM demodb_loc.managed_demo_delta_table;

# COMMAND ----------

# stroing as delta file
emp_df.write.format("delta").mode("overwrite").save("abfss://demo@formula1strgaccav.dfs.core.windows.net/demodb_loc/emp_managed_delta_table")

# stroing as delta table
emp_df.write.format("delta").mode("overwrite").saveAsTable("demodb_loc.emp_managed_delta_table")

# COMMAND ----------

target_emp_df = spark.read.format("delta").load("abfss://demo@formula1strgaccav.dfs.core.windows.net/demodb_loc/emp_managed_delta_table")

display_data(target_emp_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demodb_loc.emp_managed_delta_table LIMIT 5;

# COMMAND ----------

from delta.tables import DeltaTable

# using forPath (file path) method to get the delta table
target_emp_df = DeltaTable.forPath(spark, "abfss://demo@formula1strgaccav.dfs.core.windows.net/demodb_loc/emp_managed_delta_table")



# COMMAND ----------

from delta.tables import DeltaTable

# using forPath (file path) method to get the delta table
target_emp_df = DeltaTable.forPath(spark, "abfss://demo@formula1strgaccav.dfs.core.windows.net/demodb_loc/emp_managed_delta_table")

data = [
    (1, "Ayush", 34, 74231.55, True, "2017-03-12", "IT", 4.2, "User_1@example.com"),
    (101, "User_101", 38, 98321.77, True, "2016-11-05", "Finance", 4.5, "User_101@example.com")
]

columns = ["id", "name", "age", "salary", "is_active", "join_date", "department", "rating", "email"]

source_df = spark.createDataFrame(data, columns)

source_df.createOrReplaceTempView("source_df")


# COMMAND ----------

# MAGIC %md
# MAGIC MERGE

# COMMAND ----------

# merge using pyspark

target_emp_df.alias('t').merge(
  source_df.alias('s'),
  "t.id = s.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()


# COMMAND ----------

# MAGIC %sql 
# MAGIC MERGE INTO demodb_loc.emp_managed_delta_table AS t
# MAGIC USING source_df AS s
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT * 

# COMMAND ----------

df = spark.read.format("delta").load("abfss://demo@formula1strgaccav.dfs.core.windows.net/demodb_loc/emp_managed_delta_table")

display_data(df)

# COMMAND ----------

# delta_table.alias("t").merge(
#     df_source.alias("s"),
#     "t.id = s.id"
# ).whenMatchedUpdate(
#     condition="s.salary > t.salary",
#     set={"salary": "s.salary"}
# ).whenNotMatchedInsertAll() \
#  .execute()


# COMMAND ----------

from delta.tables import DeltaTable

emp_delta_table = DeltaTable.forName(spark, "demodb_loc.emp_managed_delta_table")

# history using pyspark 
emp_delta_table.history().select("*").display()


# COMMAND ----------

# MAGIC %sql 
# MAGIC -- using SQL
# MAGIC DESCRIBE HISTORY demodb_loc.emp_managed_delta_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

