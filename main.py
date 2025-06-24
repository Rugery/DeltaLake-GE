import great_expectations as gx
from datetime import datetime
from great_expectations.checkpoint import UpdateDataDocsAction
from great_expectations.core.run_identifier import RunIdentifier
import pandas as pd
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pathlib import Path
from checkpoint import create_or_update_checkpoint
from expectation import expectations_Telefono
from suite import create_or_update_suite
from validation import create_validation_definition
import csv


def validate_table_spark(table_name, expectations, delta_table_path, parameters=None):
    context = gx.get_context(mode="file")
    datasource_name = "spark_datasource"

    # Configura la sesión Spark con Delta Lake
    builder = (
        SparkSession.builder.appName("GE Validation")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Carga la tabla Delta en DataFrame Spark (en memoria)
    df = spark.read.format("delta").load(delta_table_path)

    # Crear o recuperar el SparkDatasource (sin pasar configuración)
    try:
        datasource = context.data_sources.get(datasource_name)
        print(f"Datasource '{datasource_name}' ya existe.")
    except KeyError:
        datasource = context.data_sources.add_spark(name=datasource_name)
        print(f"Datasource '{datasource_name}' creado.")

    asset_name = f"{table_name}_asset"

    # Crear o recuperar DataframeAsset
    if asset_name not in [a.name for a in datasource.assets]:
        data_asset = datasource.add_dataframe_asset(name=asset_name)
        print(f"DataAsset '{asset_name}' creado.")
    else:
        data_asset = datasource.get_asset(asset_name)
        print(f"DataAsset '{asset_name}' ya existe.")

    # Crear o recuperar batch definition para todo el DataFrame
    batch_definition_name = "full_table_batch"
    if batch_definition_name not in [bd.name for bd in data_asset.batch_definitions]:
        batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)
        print(f"BatchDefinition '{batch_definition_name}' creado.")
    else:
        batch_definition = data_asset.get_batch_definition(batch_definition_name)
        print(f"BatchDefinition '{batch_definition_name}' ya existe.")

    # Crear suite con expectativas
    suite = create_or_update_suite(context, asset_name, expectations)

    # Crear definición de validación
    validation_def = create_validation_definition(context, batch_definition, suite, asset_name)

    # Crear checkpoint
    checkpoint_name = f"checkpoint_{asset_name}"
    checkpoint = create_or_update_checkpoint(
        context,
        checkpoint_name,
        [validation_def],
        actions=[UpdateDataDocsAction(name="update_all_data_docs")]
    )

    run_id = RunIdentifier(run_name=f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}")

    # Ejecutar validación pasando el DataFrame runtime como parámetro batch
    result = checkpoint.run(
        batch_parameters={"dataframe": df},
        expectation_parameters=parameters or {},
        run_id=run_id
    )

    context.build_data_docs()

    # Extraer filas fallidas
    all_failed_rows = []

    for res in result.run_results.values():
        for r in res["results"]:
            if not r["success"]:
                config = r["expectation_config"]
                kwargs = config.kwargs
                col = kwargs.get("column")
                meta = config.meta or {}

                expectation_name = meta.get("name", "Sin nombre")
                expectation_description = meta.get("description", "Sin descripción")

                # 1. unexpected_index_list
                unexpected_idx = r["result"].get("unexpected_index_list", [])
                if unexpected_idx:
                    failed_rows = df.where(df.id.isin(unexpected_idx)).toPandas()
                    failed_rows["failed_column"] = col
                    failed_rows["Nombre_Regla"] = expectation_name
                    failed_rows["Descripcion"] = expectation_description
                    all_failed_rows.append(failed_rows)

                # 2. unexpected_list
                unexpected_list = r["result"].get("unexpected_list", [])
                if unexpected_list:
                    failed_rows = df.where(df[col].isin(unexpected_list)).toPandas()
                    failed_rows["failed_column"] = col
                    failed_rows["Nombre_Regla"] = expectation_name
                    failed_rows["Descripcion"] = expectation_description
                    all_failed_rows.append(failed_rows)

                # 3. unexpected_rows (para SQL Expectations)
                unexpected_rows = r["result"].get("details", {}).get("unexpected_rows", [])
                if unexpected_rows:
                    failed_rows = pd.DataFrame(unexpected_rows)
                    failed_rows["failed_column"] = col or "varios"
                    failed_rows["Nombre_Regla"] = expectation_name
                    failed_rows["Descripcion"] = expectation_description
                    all_failed_rows.append(failed_rows)

    if all_failed_rows:
        failed_df = pd.concat(all_failed_rows).drop_duplicates()
        data_cols = [c for c in failed_df.columns if c not in ["failed_column", "Nombre_Regla", "Descripcion"]]

        def clean_failed_columns(cols):
            cols = list(cols.dropna())
            return ' | '.join(sorted(set(cols)))

        agg_dict = {
            "failed_column": clean_failed_columns,
            "Nombre_Regla": lambda x: ' | '.join(sorted(set(x.dropna()))),
            "Descripcion": lambda x: ' | '.join(sorted(set(x.dropna())))
        }

        grouped = failed_df.groupby(data_cols, as_index=False).agg(agg_dict)

        output_dir = Path("quarentine")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{table_name}_quarentine.csv"

        grouped.to_csv(output_path, index=False, quoting=csv.QUOTE_ALL)
        print(f"Guardadas filas fallidas en {output_path}")
    else:
        print("No hay filas fallidas para la expectativa.")


        spark.stop()


if __name__ == "__main__":
    tables_config = [
        {
            "table_name": "telefonos",
            "delta_table_path": "./delta-table",
            "expectations": expectations_Telefono,
            "parameters": {}
        },
    ]

    for config in tables_config:
        validate_table_spark(
            table_name=config["table_name"],
            expectations=config["expectations"](),
            delta_table_path=config["delta_table_path"],
            parameters=config["parameters"]
        )
