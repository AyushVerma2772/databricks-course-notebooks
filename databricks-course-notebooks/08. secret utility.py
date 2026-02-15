# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

display(dbutils.secrets.listScopes())

# COMMAND ----------

display(dbutils.secrets.list("databrickscourse-secret-scope"))

# COMMAND ----------

dbutils.secrets.get("databrickscourse-secret-scope", "databrickscourseav-storage-acc-access-key")

# COMMAND ----------

