# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %run "../formula1-notebooks/includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{adls_path_processed}/races")

# COMMAND ----------

drivers_df = spark.read.parquet(f"{adls_path_processed}/drivers")

# COMMAND ----------

drivers_df.display()

# COMMAND ----------

races_df.display()

# COMMAND ----------

races_df.filter((col("race_year") == 2019) & (races_df["round"] <= 5)).display()

# COMMAND ----------

