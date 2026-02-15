# Databricks notebook source
from pyspark.sql.functions import sum, count, countDistinct, avg, max, min, col, round, rank;
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run "../formula1-notebooks/includes/configuration"

# COMMAND ----------

results_df = spark.read.parquet(f"{adls_path_presentation}/race_results").filter("race_year = 2020")

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Without groupBy

# COMMAND ----------

a = results_df.select(sum("points").alias("total_points"), count("points"))

# COMMAND ----------

a.select()

# COMMAND ----------

# MAGIC %md
# MAGIC ### With group by

# COMMAND ----------

results_df.groupBy("driver_nationality")\
    .agg(
        count("driver_name").alias("driver_count"),\
        sum("points").alias("total_points")\
).display()

# COMMAND ----------

window_spec = Window.orderBy(col("points").desc(), col("driver_name").asc())

new_rank = results_df.withColumn("driver_rank", rank().over(window_spec))

# COMMAND ----------

new_rank.select("driver_name", "points", "driver_rank").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_results_df where race_year = 2020;

# COMMAND ----------

display_data(spark.sql("select * from global_temp.gv_results_df where race_year = 2020;"))

# COMMAND ----------

emp_df.select(count("id"), max("salary"), min("age"), avg("bonus")).display()