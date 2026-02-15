# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using Access key
# MAGIC
# MAGIC ## Steps
# MAGIC 1. Set access key in cluster
# MAGIC 2. Test access by listing files
# MAGIC 3. Read data from ADLS

# COMMAND ----------

# List the files of the container
container_name = "demo"
storage_account_name = "databrickscourseav"
adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"

display(dbutils.fs.ls(adls_path))

# COMMAND ----------

# Read the data from ADLS path
display(spark.read.csv(adls_path + 'circuits.csv', header=True))

# COMMAND ----------

