# Databricks notebook source
# MAGIC %run "../formula1-notebooks/includes/configuration"

# COMMAND ----------

results_df = spark.read.parquet(f"{adls_path_presentation}/race_results")

# COMMAND ----------

results_df.createOrReplaceTempView("v_results_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from v_results_df where race_year = 2020

# COMMAND ----------

race_year = 2019

# COMMAND ----------

sql_df = spark.sql(f"select * from v_results_df where race_year = {race_year}")

display_data(sql_df)

# COMMAND ----------

results_df.createOrReplaceGlobalTempView("gv_results_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_results_df where race_year = 2020;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demodb_loc.emp_permanent_v LIMIT 2;

# COMMAND ----------

