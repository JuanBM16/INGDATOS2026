# Databricks notebook source
# MAGIC %run ./00_funciones

# COMMAND ----------
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("storage", "azdatasmart03")
amb = dbutils.widgets.get("ambiente")
sa = dbutils.widgets.get("storage")
cat = f"salud_{amb}"

adm = spark.table(f"{cat}.silver.admisiones")
hos = spark.table(f"{cat}.silver.hospitales")
hcahps_s = spark.table(f"{cat}.silver.hcahps_resumen")

# COMMAND ----------
kpi_cond = (adm.groupBy("condicion", "admission_type")
    .agg(F.count("*").alias("n_admisiones"),
         F.round(F.avg("monto_facturado"), 2).alias("costo_promedio"),
         F.round(F.avg("dias_estancia"), 1).alias("estancia_promedio"))
    .withColumn("ranking_costo", F.dense_rank().over(
        Window.partitionBy("admission_type").orderBy(F.desc("costo_promedio")))))
escribir_delta(kpi_cond, cat, "gold", "kpi_costos_condicion", "gold", sa, amb)

# COMMAND ----------
kpi_aseg = (adm.groupBy("aseguradora")
    .agg(F.count("*").alias("n_admisiones"),
         F.round(F.sum("monto_facturado"), 2).alias("facturacion_total"),
         F.round(F.avg("monto_facturado"), 2).alias("costo_promedio")))
escribir_delta(kpi_aseg, cat, "gold", "kpi_aseguradoras", "gold", sa, amb)

# COMMAND ----------
kpi_hos = (hos.groupBy("state", "hospital_ownership")
    .agg(F.count("*").alias("n_hospitales"),
         F.round(F.avg("calificacion"), 2).alias("calificacion_promedio"),
         F.round(F.avg(F.col("tiene_emergencia").cast("int")), 2).alias("pct_emergencia")))
escribir_delta(kpi_hos, cat, "gold", "kpi_hospitales_estado", "gold", sa, amb)

# COMMAND ----------
kpi_calidad = (hos.join(hcahps_s, on="facility_id", how="left")
    .select("facility_id", "facility_name", "state", "hospital_ownership",
            "calificacion", "puntaje_experiencia_promedio",
            "tasa_respuesta_promedio", "encuestas_completadas"))
escribir_delta(kpi_calidad, cat, "gold", "kpi_calidad_hospitalaria", "gold", sa, amb)

# COMMAND ----------
escribir_delta(adm, cat, "gold", "fact_admisiones", "gold", sa, amb)

# COMMAND ----------
display(spark.table(f"{cat}.gold.kpi_costos_condicion"))
display(spark.table(f"{cat}.gold.kpi_aseguradoras"))
display(spark.table(f"{cat}.gold.kpi_hospitales_estado"))
display(spark.table(f"{cat}.gold.kpi_calidad_hospitalaria").limit(10))
