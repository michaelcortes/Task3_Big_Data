import pandas as pd
import json
import time
from kafka import KafkaProducer
from datetime import datetime

# 1. Configuración de Kafka
KAFKA_BROKERS = ['192.172.15.98:9092']
TEMA_KAFKA = 'casos_covid_reporte'
CSV_PATH = 'casos_covid.csv' 
# Tamaño de bloque para evitar que la VM se quede sin memoria
CHUNK_SIZE = 10000 

# Crear el productor
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(0, 10, 1) 
)

# 2. Columnas Estandarizadas
# El script limpiará los encabezados del CSV para que coincidan con esta lista.
COLUMNAS_CODIGO = [
    "id de caso", 
    "nombre departamento", 
    "estado", 
    "fecha de notificacion"
]

# 3. Función para enviar un bloque de datos
def enviar_chunk_a_kafka(df_chunk):
    """Procesa un bloque de filas (chunk) y las envía a Kafka."""
    
    # 4. Construir el Payload 
    for index, row in df_chunk.iterrows():
        
        try:
            # Asegurarse de que 'id de caso' sea un entero
            id_caso = int(row["id de caso"])
        except ValueError:
            # Si el valor no es un número (fila de encabezado o corrupta)
            continue
            
        payload = {
            "ID de caso": id_caso, # Usa la columna limpia 'id de caso'
            "Nombre departamento": str(row["nombre departamento"]),
            "Estado": str(row["estado"]),
            # Genera el tiempo actual para simular el evento de streaming
            "timestamp_reporte": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        try:
            producer.send(TEMA_KAFKA, value=payload)
            # print(f"Sent: {payload}")
        except Exception as e:
            print(f"Error sending to Kafka: {e}")

# 5. Lectura Principal: Leer el CSV por Bloques y Limpiar
print(f"--- Iniciando Producción de Kafka. Leyendo CSV por bloques de {CHUNK_SIZE} filas ---")

total_chunks = 0
try:
    csv_iterator = pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE, encoding='latin1', dtype=str)
    
    # Procesar todos los bloques
    for chunk in csv_iterator:
        # 1. Eliminar espacios alrededor, 2. Convertir a minúsculas
        chunk.columns = chunk.columns.str.strip().str.lower()
        
        # 3. Seleccionar solo las columnas estandarizadas
        chunk_limpio = chunk[COLUMNAS_CODIGO].copy()
        
        enviar_chunk_a_kafka(chunk_limpio)
        total_chunks += 1
        print(f"Bloque #{total_chunks} de {CHUNK_SIZE} filas enviado.")

except FileNotFoundError:
    print(f"ERROR: Archivo CSV no encontrado en {CSV_PATH}")
    exit()
except Exception as e:
    print(f"Error grave durante la lectura/envío: {e}")
    
producer.flush()
print("--- Producción de Kafka Finalizada ---")
