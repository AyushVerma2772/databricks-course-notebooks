# Databricks notebook source
storage_acc_access_key = dbutils.secrets.get("databrickscourse-secret-scope", "databrickscourseav-storage-acc-access-key")

# COMMAND ----------

storage_account_name = "databrickscourseav"

spark.conf.set(
  f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
  storage_acc_access_key
)


# COMMAND ----------

container_name = "demo"
storage_account_name = "databrickscourseav"
adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"

display(dbutils.fs.ls(adls_path))


# COMMAND ----------

display(spark.read.csv(adls_path + "circuits.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### using dbsecrets for sas token

# COMMAND ----------

display(dbutils.secrets.list("databrickscourse-secret-scope"))

# COMMAND ----------

sas_token = dbutils.secrets.get(scope="databrickscourse-secret-scope", key="databrickscourseavdemo-sas-token")


# COMMAND ----------

storage_account_name = "databrickscourseav"
container_name = "demo"
sas_token = dbutils.secrets.get(scope="databrickscourse-secret-scope", key="databrickscourseavdemo-sas-token")

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", sas_token)


# COMMAND ----------

# List the files of the container
container_name = "demo"
adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"

dbutils.fs.ls(adls_path)

# COMMAND ----------

display(spark.read.csv(adls_path + "circuits.csv"))

# COMMAND ----------

