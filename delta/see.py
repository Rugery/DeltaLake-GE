import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Crear SparkSession
builder = SparkSession.builder \
    .appName("DeltaLakeViewer") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Calcular ruta relativa basada en la ubicación de este script
current_dir = os.path.dirname(os.path.abspath(__file__))
delta_path = os.path.join(current_dir, "../delta-tables/telefonos")

df = spark.read.format("delta").load(delta_path)
df.show()
