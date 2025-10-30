from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, round, when, lit

# 1. Inicializar Spark Session
spark = SparkSession.builder \
    .appName("CovidBatchAnalysis") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Configuración de la ruta y carga de datos
CSV_PATH = 'casos_covid.csv'

# Cargar el DataFrame de Lote con inferencia de esquema y manejo de encabezados
try:
    df_covid = spark.read.csv(
        CSV_PATH, 
        header=True, 
        inferSchema=True, 
        encoding='latin1' # Usamos 'latin1' para manejar caracteres especiales del español
    )
except Exception as e:
    print(f"Error al cargar el CSV: {e}")
    spark.stop()
    exit()

# 3. Transformación Básica y Limpieza (Columna clave 'es_fallecido')
df_limpio = df_covid.withColumn(
    # Crear una columna binaria (0 o 1) para facilitar los cálculos de la tasa de letalidad
    "es_fallecido",
    when(col("Estado").cast("string") == "Fallecido", 1).otherwise(0)
)

print("\n---------------------------------------------------------")
print("--- ANÁLISIS DE DATOS COVID (BATCH) - 5 TABLAS ---")
print("---------------------------------------------------------\n")


# ----------------------------------------------------------------------
# TABLA 1: Tasa de Letalidad por Departamento (Principal)
# ----------------------------------------------------------------------
df_letalidad_depto = df_limpio \
    .groupBy("Nombre departamento") \
    .agg(
        count("ID de caso").alias("Total_Casos"),
        spark_sum("es_fallecido").alias("Total_Fallecidos")
    ) \
    .withColumn(
        "Tasa_Letalidad_Porcentaje",
        round((col("Total_Fallecidos") / col("Total_Casos")) * 100, 2)
    ) \
    .orderBy(col("Tasa_Letalidad_Porcentaje").desc())

print("--- 1. Top 10 Tasa de Letalidad por Departamento ---")
df_letalidad_depto.show(10, truncate=False)


# ----------------------------------------------------------------------
# TABLA 2: Conteo de Casos por Estado (Estadísticas Básicas)
# ----------------------------------------------------------------------
df_estadisticas_estado = df_limpio \
    .groupBy("Estado") \
    .agg(
        count("ID de caso").alias("Total_Casos")
    ) \
    .orderBy(col("Total_Casos").desc())

print("\n--- 2. Conteo Total de Casos por Estado (Recuperado vs. Fallecido) ---")
df_estadisticas_estado.show(5, truncate=False)


# ----------------------------------------------------------------------
# TABLA 3: Top 10 Municipios con Mayor Número de Casos
# ----------------------------------------------------------------------
df_top_municipios = df_limpio \
    .groupBy("Nombre municipio") \
    .agg(
        count("ID de caso").alias("Total_Casos")
    ) \
    .orderBy(col("Total_Casos").desc())

print("\n--- 3. Top 10 Municipios con Mayor Número de Casos ---")
df_top_municipios.show(10, truncate=False)


# ----------------------------------------------------------------------
# TABLA 4: Tasa de Letalidad por Género
# ----------------------------------------------------------------------
df_letalidad_genero = df_limpio \
    .groupBy("Sexo") \
    .agg(
        count("ID de caso").alias("Casos_Totales"),
        spark_sum("es_fallecido").alias("Fallecidos_Totales")
    ) \
    .withColumn(
        "Tasa_Letalidad_Genero",
        round((col("Fallecidos_Totales") / col("Casos_Totales")) * 100, 2)
    ) \
    .orderBy(col("Tasa_Letalidad_Genero").desc())

print("\n--- 4. Tasa de Letalidad por Género ---")
df_letalidad_genero.show(5, truncate=False)


# ----------------------------------------------------------------------
# TABLA 5: Distribución de Casos por Tipo de Contagio
# ----------------------------------------------------------------------
df_tipo_contagio = df_limpio \
    .groupBy("Tipo de contagio") \
    .agg(
        count("ID de caso").alias("Total_Casos")
    ) \
    .withColumn(
        # Calcula el porcentaje que representa cada tipo de contagio
        "Porcentaje",
        round((col("Total_Casos") / df_limpio.count()) * 100, 2)
    ) \
    .orderBy(col("Total_Casos").desc())

print("\n--- 5. Distribución de Casos por Tipo de Contagio ---")
df_tipo_contagio.show(5, truncate=False)


# ----------------------------------------------------------------------
# FINALIZACIÓN Y ALMACENAMIENTO (Opcional)
# ----------------------------------------------------------------------

# Opcional: Almacenar la tabla principal para uso posterior
OUTPUT_PATH = "/tmp/spark_output/covid_letalidad_batch.parquet"
df_letalidad_depto.write.mode("overwrite").parquet(OUTPUT_PATH)
print(f"\nResultados del Batch almacenados en: {OUTPUT_PATH}")

# Detener la Spark Session
spark.stop()