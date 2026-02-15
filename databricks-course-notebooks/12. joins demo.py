# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %run "../formula1-notebooks/includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{adls_path_processed}/races").withColumnRenamed("name", "race_name").filter("race_year = 2019")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{adls_path_processed}/circuits").withColumnRenamed("name", "circuit_name")

# COMMAND ----------

races_join_circuits_df = races_df.join(circuits_df, races_df["circuit_id"] == circuits_df["circuit_id"], "inner")

# COMMAND ----------

races_join_circuits_df.select("circuit_name", "location", "country", "race_name", "round").display()

# COMMAND ----------

races_left_join_circuits_df = races_df.join(circuits_df, races_df["circuit_id"] == circuits_df["circuit_id"], "left")

# COMMAND ----------

races_left_join_circuits_df.select("circuit_name", "location", "country", "race_name", "round").display()

# COMMAND ----------

races_right_join_circuits_df = races_df.join(circuits_df, races_df["circuit_id"] == circuits_df["circuit_id"], "right")

# COMMAND ----------

races_right_join_circuits_df.select("circuit_name", "location", "country", "race_name", "round").display()

# COMMAND ----------

# Customers DataFrame
data_customers = [
    (1, "Alice", "USA"),
    (2, "Bob", "UK"),
    (3, "Charlie", "USA"),
    (4, "David", "India"),
    (5, "Eva", "Canada")
]

columns_customers = ["CID", "NAME", "COUNTRY"]

df_customers = spark.createDataFrame(data_customers, columns_customers)

df_customers.display()

# COMMAND ----------

# Orders DataFrame
data_orders = [
    (101, 1, "Laptop"),
    (102, 1, "Mouse"),
    (103, 3, "Chair"),
    (104, 6, "Desk"),  # No matching customer
    (105, 2, "Tablet")
]

columns_orders = ["OID", "CID", "ITEM"]

df_orders = spark.createDataFrame(data_orders, columns_orders)

df_orders.display()

# COMMAND ----------

# inner join
df_orders.join(df_customers, df_orders["CID"] == df_customers["CID"], "inner").display()

# COMMAND ----------

df_orders.join(df_customers, df_orders["CID"] == df_customers["CID"], "semi").display()

# COMMAND ----------

df_orders.join(df_customers, df_orders["CID"] == df_customers["CID"], "anti").display()

# COMMAND ----------



# COMMAND ----------

df1 = spark.createDataFrame(
    [(1, "Aman", "IT"),
     (2, "Ravi", "HR"),
     (2, "Ravi", "HR")],   # duplicate row
    ["id", "name", "dept"]
)

df2 = spark.createDataFrame(
    [(2, "Ravi", "HR"),
     (2, "Ravi", "HR"),
     (3, "Ankit", "IT")],
    ["id", "name", "dept"]
)

print("1. UNION")
df1.union(df2).show()


print("2. UNION BY NAME")
df1.unionByName(df2).show()

print("3. INTERSECT")
df1.intersect(df2).show()

print("4. INTERSECT ALL")
df1.intersectAll(df2).show()


print("5. UNION BY NAME WITH MISSING COLUMN")
df2_missing = df2.drop("dept")
df1.unionByName(df2_missing, allowMissingColumns=True).show()


# COMMAND ----------

# d6 = d1.intersect(d2)

d7 = d1.intersectAll(d2)

d7.display()

# COMMAND ----------

