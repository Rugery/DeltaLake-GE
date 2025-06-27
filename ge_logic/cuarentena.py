from datetime import datetime
import pandas as pd
from pathlib import Path
import csv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def procesar_filas_fallidas(df, result, table_name: str):
    all_failed_rows = []

    def append_failed_rows(spark_df, extra_info):
        try:
            pdf = spark_df.toPandas()
            pdf = pdf.loc[:, ~pdf.columns.duplicated()]
            pdf.reset_index(drop=True, inplace=True)
            for k, v in extra_info.items():
                pdf[k] = v
            return pdf
        except Exception as e:
            logger.error(f"Fallo al convertir Spark DataFrame a Pandas: {e}")
            return pd.DataFrame()

    for res in result.run_results.values():
        for r in res["results"]:
            if not r["success"]:
                config = r["expectation_config"]
                kwargs = config.kwargs
                meta = config.meta or {}

                expectation_name = meta.get("name", "Sin nombre")
                expectation_description = meta.get("description", "Sin descripción")

                unexpected_list = r["result"].get("unexpected_list", [])
                unexpected_rows = r["result"].get("details", {}).get("unexpected_rows", [])

                if "column" in kwargs and unexpected_list:
                    col = kwargs["column"]
                    failed_rows_spark = df.where(df[col].isin(unexpected_list))
                    all_failed_rows.append(append_failed_rows(failed_rows_spark, {
                        "failed_column": col,
                        "Nombre_Regla": expectation_name,
                        "Descripcion": expectation_description
                    }))

                elif "column_A" in kwargs and "column_B" in kwargs and unexpected_list:
                    col_a = kwargs["column_A"]
                    col_b = kwargs["column_B"]
                    cond = None
                    for val_a, val_b in unexpected_list:
                        row_cond = (df[col_a] == val_a) & (df[col_b] == val_b)
                        cond = row_cond if cond is None else cond | row_cond
                    failed_rows_spark = df.where(cond)
                    all_failed_rows.append(append_failed_rows(failed_rows_spark, {
                        "failed_column": f"{col_a}, {col_b}",
                        "Nombre_Regla": expectation_name,
                        "Descripcion": expectation_description
                    }))

                elif unexpected_rows:
                    failed_rows = pd.DataFrame(unexpected_rows)
                    failed_rows = failed_rows.loc[:, ~failed_rows.columns.duplicated()]
                    failed_rows["failed_column"] = "varios"
                    failed_rows["Nombre_Regla"] = expectation_name
                    failed_rows["Descripcion"] = expectation_description
                    all_failed_rows.append(failed_rows)

    if all_failed_rows:
        failed_df = pd.concat(all_failed_rows, ignore_index=True).drop_duplicates()
        data_cols = [c for c in failed_df.columns if c not in ["failed_column", "Nombre_Regla", "Descripcion"]]

        def clean_failed_columns(cols):
            return ' | '.join(sorted(set(cols.dropna())))

        grouped = failed_df.groupby(data_cols, as_index=False).agg({
            "failed_column": clean_failed_columns,
            "Nombre_Regla": lambda x: ' | '.join(sorted(set(x.dropna()))),
            "Descripcion": lambda x: ' | '.join(sorted(set(x.dropna())))
        })

        # Ruta dinámica con tabla
        output_dir = Path("resultados") / "cuarentena" / table_name
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"cuarentena_{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        grouped.to_csv(output_path, index=False, quoting=csv.QUOTE_ALL)
        logger.info(f"Filas fallidas guardadas en {output_path}")
    else:
        logger.info("✅ No hay filas fallidas para la expectativa.")
