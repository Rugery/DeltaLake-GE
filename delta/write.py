from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("DeltaLakeLocalExample") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Establecer el nivel de log a ERROR
spark.sparkContext.setLogLevel("ERROR")

data = [
    (1, "6013820000", 1),
    (2, "6019998888", 1),
    (3, "6044444444", 2),
    (4, "3001234567", 4),
    (5, "3027654321", 3),
    (6, "6011234567", 2),
    (7, "6048889999", 3),
    (8, "3009991111", 1),
    (9, "3021112222", 2),
    (10, "6015550000", 4),
]

columns = ["id", "telefono", "cod_municipio"]

df = spark.createDataFrame(data, schema=columns)

delta_table_path = "./delta-table"

df.write.format("delta").mode("overwrite").save(delta_table_path)

# Si ya existe la tabla, la borramos para recrearla
spark.sql("DROP TABLE IF EXISTS telefonos")

# Ahora creamos la tabla Delta apuntando a ese directorio
spark.sql(f"""
    CREATE TABLE telefonos
    USING DELTA
    LOCATION '{delta_table_path}'
""")


