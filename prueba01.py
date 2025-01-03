
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Colab PySpark Example") \
    .getOrCreate()

# Crear un DataFrame de ejemplo y realizar operaciones
data = [("Juan", 30), ("Ana", 25), ("Luis", 35)]
columns = ["Nombre", "Edad"]

df = spark.createDataFrame(data, columns)

# Mostrar el DataFrame
print(df.show())

# Realizar una operación de filtrado
print(df.filter(df["Edad"] > 30).show())