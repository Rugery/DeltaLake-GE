from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import random

# Crear sesión de Spark
builder = SparkSession.builder \
    .appName("DeltaLakeExampleConDosTablas") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Ruta base común
base_path = "./delta-tables"

# ========================
# Tabla: telefonos (50 registros)
# ========================
telefonos_data = [
    (i, f"3{random.randint(0,9)}{random.randint(10000000, 99999999)}", random.randint(1, 20))
    for i in range(1, 51)
]
telefonos_columns = ["id", "telefono", "cod_municipio"]
telefonos_df = spark.createDataFrame(telefonos_data, schema=telefonos_columns)

telefonos_path = f"{base_path}/telefonos"
telefonos_df.write.format("delta").mode("overwrite").save(telefonos_path)

spark.sql("DROP TABLE IF EXISTS telefonos")
spark.sql(f"""
    CREATE TABLE telefonos
    USING DELTA
    LOCATION '{telefonos_path}'
""")

# ========================
# Tabla: clientes (50 registros con ciudad)
# ========================

nombres = ["Ana", "Luis", "María", "Carlos", "Laura", "Pedro", "Sandra", "Jorge", "Lucía", "Andrés"]
apellidos = ["Torres", "Gómez", "Ruiz", "López", "Méndez", "Díaz", "Villa", "Pérez", "Ramírez", "Martínez"]

# Mapeo ficticio cod_municipio -> ciudad
municipio_a_ciudad = {
    i: f"Ciudad{i}" for i in range(1, 21)
}

clientes_data = []
for i in range(1, 51):
    cod_municipio = random.randint(1, 20)
    ciudad = municipio_a_ciudad[cod_municipio]
    clientes_data.append((
        i,
        f"{random.choice(nombres)} {random.choice(apellidos)}",
        f"user{i}@example.com",
        cod_municipio,
        ciudad
    ))

clientes_columns = ["id_cliente", "nombre", "email", "cod_municipio", "ciudad"]
clientes_df = spark.createDataFrame(clientes_data, schema=clientes_columns)

clientes_path = f"{base_path}/clientes"
clientes_df.write.format("delta").mode("overwrite").save(clientes_path)

spark.sql("DROP TABLE IF EXISTS clientes")
spark.sql(f"""
    CREATE TABLE clientes
    USING DELTA
    LOCATION '{clientes_path}'
""")
