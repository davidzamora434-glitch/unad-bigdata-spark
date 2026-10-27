import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

def generar_datos_sensor():
    # Línea 6 corregida con la indentación correcta (4 espacios)
    return {
        "sensor_id": random.randint(1, 5),
        "temperature": round(random.uniform(20.0, 35.0), 2),
        "humidity": round(random.uniform(40.0, 70.0), 2),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

# Configuración del productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("🚀 Productor de Kafka iniciado. Enviando datos cada 1 segundo... (Ctrl+C para salir)")

try:
    while True:
        datos = generar_datos_sensor()
        producer.send('sensor_data', value=datos)
        print(f"📦 Enviado: {datos}")
        time.sleep(1)
except KeyboardInterrupt:
    print("\n🛑 Productor detenido.")
finally:
    producer.close()
