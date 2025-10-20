import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, explode, posexplode, lit, to_timestamp, col, year, month
from pyspark.sql.functions import pow, avg, date_trunc

# =========================================================================
# === CONFIGURACIÓN DE SPARK Y AWS (PARA EC2 CON ROL IAM) ================
# =========================================================================

spark = SparkSession.builder \
    .appName("WeatherETL") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .getOrCreate()

print("Spark Session creada. Usando Rol de IAM para la autenticación en S3.")
s3_bucket = "s3a://weatherlytics-datalake-dev-us-east-2"

# =========================================================================
# === COMIENZO DEL PROCESO ETL ============================================
# =========================================================================
print("Iniciando el procesamiento de datos...")

# =========================================================================
# === PASO 1: LEER DATOS DE LA CAPA BRONZE ================================
# =========================================================================

print("Leyendo datos históricos en formato JSON...")
# Spark está leyendo un esquema plano, no el 'hourly' anidado.
df_historical_json = spark.read.json(f"{s3_bucket}/bronze/historical_weather_data/*.json")
print(f"Se leyeron {df_historical_json.count()} registros de los archivos JSON.")
df_historical_json.printSchema()
df_historical_json.show(5)

print("\nLeyendo datos de la API del clima en formato Parquet...")
df_api_parquet = spark.read.parquet(f"{s3_bucket}/bronze/weather_api_data/")
print(f"Se leyeron {df_api_parquet.count()} registros de la API.")
df_api_parquet.printSchema()
df_api_parquet.show(5)

# =========================================================================
# === PASO 2: TRANSFORMAR Y UNIFICAR DATOS ================================
# =========================================================================
df_historical_final = None
df_api_processed = None

# --- 2.1 Procesar Datos Históricos (JSON) ---
print("\nProcesando datos históricos JSON...")
# *** ¡INICIO DE LA CORRECCIÓN! ***
# El log mostró que el esquema leído es plano (city_name, dt, main.temp, etc.)
# Vamos a usar ese esquema directamente.
try:
    df_historical_final = df_historical_json.select(
        col("city_name").alias("location"),
        to_timestamp(col("dt")).alias("time"), # 'dt' parece ser un timestamp epoch
        col("main.temp").cast("float").alias("temperature_c"),
        col("main.humidity").cast("float").alias("humidity_percent"),
        col("main.pressure").cast("float").alias("pressure_hpa"), # Asumiendo hPa
        col("wind.speed").cast("float").alias("wind_speed_kph"), # Asumiendo km/h
        col("wind.deg").cast("float").alias("wind_direction_deg")
    )
    print("Esquema final de datos históricos procesados (basado en esquema plano):")
    df_historical_final.printSchema()

except Exception as e:
    print(f"WARN: No se pudo procesar los datos históricos. Error: {e}")
# *** ¡FIN DE LA CORRECCIÓN! ***


# --- 2.2 Procesar Datos de la API (Parquet) ---
print("\nProcesando datos de la API Parquet...")
try:
    df_api_processed = df_api_parquet.select(
        col("location.name").alias("location"),
        to_timestamp(col("current.last_updated_epoch")).alias("time"),
        col("current.temp_c").cast("float").alias("temperature_c"),
        col("current.humidity").cast("float").alias("humidity_percent"),
        col("current.pressure_mb").cast("float").alias("pressure_hpa"),
        col("current.wind_kph").cast("float").alias("wind_speed_kph"),
        col("current.wind_degree").cast("float").alias("wind_direction_deg")
    ).na.drop(subset=["time"])
    print("Esquema final de datos de API procesados:")
    df_api_processed.printSchema()
except Exception as e:
    print(f"WARN: No se pudieron procesar los datos de la API. Error: {e}")

# --- 2.3 Unificar Todos los Datos ---
print("\nUnificando datos...")
df_final_unified = None
try:
    if df_historical_final is not None and df_api_processed is not None:
        # Asegurarnos de que las columnas coincidan
        cols_hist = set(df_historical_final.columns)
        cols_api = set(df_api_processed.columns)
        if cols_hist != cols_api:
             print(f"WARN: Los esquemas no coinciden. Histórico: {cols_hist}. API: {cols_api}")
             # Intentar unir solo columnas comunes (simple)
             common_cols = list(cols_hist.intersection(cols_api))
             df_final_unified = df_historical_final.select(common_cols).unionByName(df_api_processed.select(common_cols))
        else:
             df_final_unified = df_historical_final.unionByName(df_api_processed)

    elif df_historical_final is not None:
        print("WARN: Solo se procesaron datos históricos.")
        df_final_unified = df_historical_final
    elif df_api_processed is not None:
        print("WARN: Solo se procesaron datos de la API.")
        df_final_unified = df_api_processed
    else:
        print("ERROR: No se pudieron procesar datos ni históricos ni de la API.")
        # Crear DataFrame vacío para evitar fallos posteriores
        schema = spark.createDataFrame([], "location string, time timestamp, temperature_c float, humidity_percent float, pressure_hpa float, wind_speed_kph float, wind_direction_deg float").schema
        df_final_unified = spark.createDataFrame([], schema)

except Exception as e:
      print(f"ERROR: Falló la unión de DataFrames. Error: {e}")
      schema = spark.createDataFrame([], "location string, time timestamp, temperature_c float, humidity_percent float, pressure_hpa float, wind_speed_kph float, wind_direction_deg float").schema
      df_final_unified = spark.createDataFrame([], schema)

# Asegurarse de que el DataFrame no esté vacío antes de continuar
if df_final_unified.rdd.isEmpty():
    print("ERROR: El DataFrame unificado está vacío. Deteniendo el script.")
else:
    print(f"Total de registros unificados: {df_final_unified.count()}")
    print("Esquema unificado final:")
    df_final_unified.printSchema()
    df_final_unified.show(10)

    df_final_unified = df_final_unified.dropDuplicates(["location", "time"])
    print(f"Registros después de eliminar duplicados: {df_final_unified.count()}")

    # =========================================================================
    # === PASO 3: ESCRIBIR DATOS EN LA CAPA SILVER ============================
    # =========================================================================
    print("\nEscribiendo datos unificados en la capa Silver...")

    df_to_write = df_final_unified.withColumn("year", year(col("time"))) \
                                  .withColumn("month", month(col("time")))

    silver_path = f"{s3_bucket}/silver/weather_data"

    df_to_write.write \
        .partitionBy("year", "month", "location") \
        .mode("overwrite") \
        .parquet(silver_path)

    print(f"Datos escritos exitosamente en formato Parquet y particionados en: {silver_path}")

    # =========================================================================
# === PASO 4: CREAR CAPA GOLD (AGREGACIONES DE NEGOCIO) ===================
# =========================================================================
print("\nIniciando creación de la capa Gold...")

# Leemos los datos limpios que acabamos de escribir en la capa Silver
silver_path = f"{s3_bucket}/silver/weather_data"
df_silver = spark.read.parquet(silver_path)

print(f"Se leyeron {df_silver.count()} registros de la capa Silver.")

# 1. Calcular el Potencial Eólico (proxy)
# Potencia es proporcional al cubo de la velocidad del viento
df_con_potencial = df_silver.withColumn("potencial_eolico", pow(col("wind_speed_kph"), 3))

# 2. Agregar por día y ubicación para responder preguntas de negocio
df_gold_diario = df_con_potencial.groupBy(
    col("location"),
    date_trunc("day", col("time")).alias("dia")
).agg(
    avg("potencial_eolico").alias("potencial_eolico_promedio"),
    avg("wind_speed_kph").alias("velocidad_viento_promedio"),
    avg("temperature_c").alias("temperatura_promedio")
).orderBy(col("dia").desc())

print("Esquema de la capa Gold (agregado diario):")
df_gold_diario.printSchema()
df_gold_diario.show(10)

# 3. Escribir la capa Gold en S3
gold_path = f"{s3_bucket}/gold/potencial_eolico_diario"

df_gold_diario.write \
    .mode("overwrite") \
    .parquet(gold_path)

print(f"Datos de la capa Gold escritos exitosamente en: {gold_path}")

# =========================================================================
# === FIN DEL PROCESO ETL =================================================
# =========================================================================
print("Procesamiento de datos finalizado.")
spark.stop()