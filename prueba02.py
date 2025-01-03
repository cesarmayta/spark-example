import os
from pyspark.sql import SparkSession


# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("RandomUser Processing") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()
    
import requests
import json

# Función para obtener datos de RandomUser
def fetch_random_users(batch_size=5000, total_batches=20):
    users = []
    for _ in range(total_batches):
        response = requests.get(f"https://randomuser.me/api/?results={batch_size}")
        if response.status_code == 200:
            data = response.json()
            users.extend(data['results'])
    return users

# Descargar 100,000 registros
random_users = fetch_random_users()

current_dir = os.path.dirname(os.path.abspath(__file__))

target_path = os.path.join("/","opt","spark","random_users.json")

# Guardar los datos en un archivo JSON
with open(target_path, "w") as f:
    json.dump(random_users, f)
    
# Leer datos JSON en un DataFrame de PySpark
df = spark.read.json("/opt/spark/random_users.json")

# Mostrar los primeros registros
df.show(5)