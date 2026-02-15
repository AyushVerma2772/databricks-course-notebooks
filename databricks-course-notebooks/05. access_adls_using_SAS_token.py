# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using SAS token
# MAGIC
# MAGIC ## Steps
# MAGIC 1. set up sas token in spark
# MAGIC 2. Test access by listing files
# MAGIC 3. Read data from ADLS

# COMMAND ----------

# set up sas token in spark
storage_account_name = "databrickscourseav"
container_name = "demo"
sas_token = "sp=rl&st=2026-01-05T07:37:29Z&se=2026-01-05T15:52:29Z&spr=https&sv=2024-11-04&sr=c&sig=EPdPenzCrXGLsjqCYH5LprvIJSERYW5A2mhkdMEi6LQ%3D" 

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", sas_token)



# COMMAND ----------

# List the files of the container
container_name = "demo"
adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"

dbutils.fs.ls(adls_path)


# COMMAND ----------

# Read the data from ADLS path
display(spark.read.csv(adls_path + 'circuits.csv', header=True))


# COMMAND ----------

