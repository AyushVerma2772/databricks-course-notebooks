# Databricks notebook source
# MAGIC %md
# MAGIC # Magic commands:
# MAGIC Magic commands in Databricks are special commands that tell the notebook how to interpret the code in a cell. They start with a percent sign (%) and are written at the beginning of a cell.
# MAGIC
# MAGIC The most common magic commands are:
# MAGIC - %run: runs another notebook inside the current notebook.
# MAGIC - %fs: lets you work with files and folders in Databricks File System (DBFS).
# MAGIC - %md: used to write text, headings, and notes in a notebook.
# MAGIC - %python: runs the cell as Python (PySpark) code.
# MAGIC - %sql: runs the cell as SQL code.
# MAGIC - %pip: installs Python libraries directly inside the notebook cluster.
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /

# COMMAND ----------

# MAGIC %run "./1. Notebook Introduction"
# MAGIC
# MAGIC %pip list

# COMMAND ----------

# MAGIC %pip list

# COMMAND ----------

