from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum as spark_sum, window, round, when, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# 1. Inicializar Spark Session
spark = SparkSession.builder \
    .appName("CovidSparkStreamingDefinitivo") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Definir el esquema (Alineado al payload de salida del Producer)
json_schema = StructType([
    StructField("ID de caso", IntegerType(), True),
    StructField("Nombre departamento", StringType(), True),
    StructField("Estado", StringType(), True), 
    StructField("timestamp_reporte", StringType(), True) 
])

# 3. Leer el stream de Kafka
TEMA_KAFKA = "casos_covid_reporte"
KAFKA_BROKERS = "192.172.15.98:9092"
CHECKPOINT_PATH = "/tmp/covid_checkpoint" # Ruta obligatoria para el estado

df_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", TEMA_KAFKA) \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Procesamiento del Stream: Deserialización
parsed_df = df_stream.selectExpr("CAST(value AS STRING) as json_payload", "timestamp as kafka_timestamp") \
    .select(
        from_json(col("json_payload"), json_schema).alias("data"),
        col("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")

# 5. Transformación: Columna de marca de tiempo
df_with_time = parsed_df.withColumn(
    "timestamp",
    col("timestamp_reporte").cast("timestamp")
)

# 6. Lógica de Negocio: Tasa de Letalidad en Streaming
df_procesado = df_with_time.withColumn(
    "es_fallecido",
    when(col("Estado").cast("string") == "Fallecido", 1).otherwise(0)
)

# 6.2. Agregación: Ventana y Watermark optimizados para pruebas
df_agg = df_procesado \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(
        window(col("timestamp"), "2 minutes", "1 minute"), 
        col("Nombre departamento") 
    ) \
    .agg(
        count("ID de caso").alias("Casos_Recibidos"),
        spark_sum("es_fallecido").alias("Fallecidos_Recibidos")
    ) \
    .withColumn(
        "Tasa_Letalidad_Stream",
        round((col("Fallecidos_Recibidos") / col("Casos_Recibidos")) * 100, 2)
    )

# 7. Iniciar la Consulta de Streaming (Output)
query = df_agg.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start()

print("--- Spark Streaming Consumer Definitivo Iniciado (Baja Latencia) ---")

# 8. Esperar a la terminación
query.awaitTermination()