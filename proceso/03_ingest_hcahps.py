# Databricks notebook source
# MAGIC %run ./00_funciones

# COMMAND ----------
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("storage", "azdatasmart03")
amb = dbutils.widgets.get("ambiente")
sa = dbutils.widgets.get("storage")

# COMMAND ----------
archivo = "HCAHPS-Hospital.csv"
df = normalizar_columnas(leer_csv(spark, f"abfss://raw@{sa}.dfs.core.windows.net/{archivo}"))
escribir_delta(agregar_auditoria(df, archivo), f"salud_{amb}", "bronze",
               "hcahps", "bronze", sa, amb)

# COMMAND ----------
display(spark.table(f"salud_{amb}.bronze.hcahps").limit(5))

# COMMAND ----------
spark.table(f"salud_{amb}.bronze.hcahps").printSchema()
