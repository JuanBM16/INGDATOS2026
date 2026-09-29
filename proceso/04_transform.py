# Databricks notebook source
# MAGIC %run ./00_funciones

# COMMAND ----------
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("storage", "azdatasmart03")
amb = dbutils.widgets.get("ambiente")
sa = dbutils.widgets.get("storage")
cat = f"salud_{amb}"

# COMMAND ----------
# --- Admisiones ---
adm = spark.table(f"{cat}.bronze.admisiones")
sin_audit = [c for c in adm.columns if c not in ("fecha_ingesta", "archivo_origen")]

adm_s = (adm.dropDuplicates(sin_audit)
    .withColumn("paciente_id", F.sha2(F.upper(F.trim("name")), 256))
    .withColumn("edad", F.col("age").cast("int"))
    .withColumn("fecha_ingreso", F.to_date("date_of_admission", "yyyy-MM-dd"))
    .withColumn("fecha_alta", F.to_date("discharge_date", "yyyy-MM-dd"))
    .withColumn("monto_facturado", F.col("billing_amount").cast("double"))
    .withColumn("dias_estancia", F.datediff("fecha_alta", "fecha_ingreso"))
    .withColumn("condicion", F.initcap(F.trim("medical_condition")))
    .withColumn("aseguradora", F.trim("insurance_provider"))
    .withColumn("monto_negativo", F.col("monto_facturado") < 0)
    .filter(F.col("monto_facturado").isNotNull() & F.col("fecha_ingreso").isNotNull())
    .drop("name", "age", "date_of_admission", "discharge_date",
          "billing_amount", "medical_condition", "insurance_provider"))

escribir_delta(adm_s, cat, "silver", "admisiones", "silver", sa, amb)

# COMMAND ----------
# --- Hospitales (CMS) ---
hos = spark.table(f"{cat}.bronze.hospitales")
hos_s = (hos.select("facility_id", "facility_name", "city_town", "state",
                    "hospital_type", "hospital_ownership",
                    "emergency_services", "hospital_overall_rating")
    .withColumn("tiene_emergencia", F.col("emergency_services") == "Yes")
    .withColumn("calificacion",
        F.when(F.col("hospital_overall_rating").rlike("^[1-5]$"),
               F.col("hospital_overall_rating").cast("int")))
    .drop("emergency_services", "hospital_overall_rating"))

escribir_delta(hos_s, cat, "silver", "hospitales", "silver", sa, amb)

# COMMAND ----------
# --- HCAHPS: pivotear de "una fila por pregunta" a "una fila por hospital" ---
hcahps = spark.table(f"{cat}.bronze.hcahps")

resumen = (hcahps
    .filter(F.col("hcahps_measure_id").rlike("(?i)linear_score"))
    .groupBy("facility_id")
    .agg(
        F.round(F.avg(F.col("hcahps_linear_mean_value").cast("double")), 1)
            .alias("puntaje_experiencia_promedio"),
        F.round(F.avg(F.col("survey_response_rate_percent").cast("double")), 1)
            .alias("tasa_respuesta_promedio"),
        F.max(F.col("number_of_completed_surveys").cast("int"))
            .alias("encuestas_completadas")
    ))

escribir_delta(resumen, cat, "silver", "hcahps_resumen", "silver", sa, amb)

# COMMAND ----------
display(spark.table(f"{cat}.silver.admisiones").limit(5))
display(spark.table(f"{cat}.silver.hospitales").limit(5))
display(spark.table(f"{cat}.silver.hcahps_resumen").limit(5))
