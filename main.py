import great_expectations as gx
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from great_expectations.checkpoint import UpdateDataDocsAction
from ge_logic.checkpoint import create_or_update_checkpoint
from ge_logic.indicadores import generar_indicadores
from ge_logic.cuarentena import procesar_filas_fallidas
from ge_logic.suite import create_or_update_suite
from ge_logic.validation import create_validation_definition
from config_tablas.tablas import tables_config
from ge_logic.helpers import (
    get_or_create_datasource,
    get_or_create_asset,
    get_or_create_batch_definition,
    generate_run_identifier,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
logging.getLogger("great_expectations").setLevel(logging.WARNING)
logging.getLogger("py4j").setLevel(logging.ERROR)


def create_spark_session():
    builder = (
        SparkSession.builder.appName("GE Validation")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def validate_table_spark(spark, context, table_name, suite_expectations, delta_table_path):
    suite_name = f"suite_{table_name}"

    try:
        df = spark.read.format("delta").load(delta_table_path)
    except Exception as e:
        logger.error(f"❌ Error al leer la tabla Delta '{table_name}': {str(e)}")
        return

    datasource = get_or_create_datasource(context)
    data_asset = get_or_create_asset(datasource, table_name)  # Aquí usa solo table_name sin _asset si quieres
    batch_definition = get_or_create_batch_definition(data_asset)

    suite = create_or_update_suite(context, suite_name, suite_expectations)
    logger.info(f"Suite '{suite_name}' actualizada o creada.")

    validation_def = create_validation_definition(context, batch_definition, suite, suite_name)
    checkpoint_name = f"checkpoint_{suite_name}"
    checkpoint = create_or_update_checkpoint(
        context,
        checkpoint_name,
        [validation_def],
        actions=[UpdateDataDocsAction(name="update_all_data_docs")]
    )

    run_id = generate_run_identifier(prefix=f"run_{table_name}")
    logger.info(f"Ejecutando checkpoint '{checkpoint_name}' para '{suite_name}'...")
    result = checkpoint.run(batch_parameters={"dataframe": df}, run_id=run_id)


    if hasattr(result, 'run_results'):
        run_results = result.run_results
        generar_indicadores(run_results, table_name)
        logger.info(f"Indicador generado para: {table_name}")

    procesar_filas_fallidas(df, result, table_name)
    logger.info(f"Cuarentena generada para: {table_name}")

    context.build_data_docs()
    logger.info(f"Documentación de resultados generada para '{table_name}'.")
    logger.info(f"✅ Validación finalizada para tabla '{table_name}'.")


if __name__ == "__main__":
    spark = create_spark_session()
    context = gx.get_context(mode="file", project_root_dir="./resultados/ge_documentation")
    for config in tables_config:
        suite_list = config["suite"]()
        validate_table_spark(
            spark=spark,
            context=context,
            table_name=config["table_name"],
            suite_expectations=suite_list,
            delta_table_path=config["delta_table_path"],
        )

    spark.stop()
    logger.info("🛑 Spark session detenida.")
