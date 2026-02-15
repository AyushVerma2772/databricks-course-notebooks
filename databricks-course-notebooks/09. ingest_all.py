# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("my_text_wedgit", "")

# COMMAND ----------

my_text_wedgit_value = dbutils.widgets.get("my_text_wedgit")
my_text_wedgit_value

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

dbutils.notebook.run("./02. ingest_races_file", 0)

# COMMAND ----------

