# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using Access key
# MAGIC
# MAGIC ## Steps
# MAGIC 1. Set access key in Spark
# MAGIC 2. Test access by listing files
# MAGIC 3. Read data from ADLS

# COMMAND ----------

# set up access key in spark
storage_account_name = "databrickscourseav"
storage_account_key = "JFXBA4TYgEmWNlsMlb43z8mc0yyYJV5RevK89WDi7C2orVeHi8F1mpXchX/j2+7ghf4c1kA9zskH+ASt/zRRhw=="

spark.conf.set(
  f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
  storage_account_key
)


# COMMAND ----------


# List the files of the container
container_name = "demo"
adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"

display(dbutils.fs.ls(adls_path))

# COMMAND ----------

# Read the data from ADLS path
display(spark.read.csv(adls_path + 'circuits.csv', header=True))

# COMMAND ----------

