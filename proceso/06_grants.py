# Databricks notebook source
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("grupo", "account users")
amb = dbutils.widgets.get("ambiente")
cat = f"salud_{amb}"
grupo = dbutils.widgets.get("grupo")

# COMMAND ----------
spark.sql(f"GRANT USE CATALOG ON CATALOG {cat} TO `{grupo}`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {cat}.gold TO `{grupo}`")
for t in ["fact_admisiones", "kpi_costos_condicion", "kpi_aseguradoras",
          "kpi_hospitales_estado", "kpi_calidad_hospitalaria"]:
    spark.sql(f"GRANT SELECT ON TABLE {cat}.gold.{t} TO `{grupo}`")
