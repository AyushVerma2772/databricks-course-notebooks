# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Utilities
# MAGIC Databricks Utilities, also called dbutils, are built-in helper tools that make it easier to work with files, secrets, notebooks, and widgets in Databricks. They help you perform common tasks without writing complex code.
# MAGIC
# MAGIC - dbutils.fs – used to list, read, write, and manage files in DBFS or mounted storage.
# MAGIC - dbutils.secrets – used to securely read passwords, keys, or tokens from secret scopes.
# MAGIC - dbutils.notebook – used to call and run another notebook from the current notebook.
# MAGIC - dbutils.widgets – used to create and read parameters passed to a notebook.

# COMMAND ----------

dbutils.help()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

# MAGIC %run "./1. Notebook Introduction"

# COMMAND ----------

dbutils.notebook.run("./1. Notebook Introduction", 3600)

# COMMAND ----------

dbutils.notebook.help('run')

# COMMAND ----------

