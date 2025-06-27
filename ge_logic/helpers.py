import logging
from datetime import datetime
from great_expectations.core.run_identifier import RunIdentifier

logger = logging.getLogger(__name__)

# Constantes (puedes moverlas aquí o mantenerlas en otro archivo config.py)
DATASOURCE_NAME = "spark_datasource"
BATCH_DEFINITION_NAME = "full_table_batch"


def get_or_create_datasource(context, name=DATASOURCE_NAME):
    """
    Recupera o crea un datasource con el nombre dado dentro del contexto de Great Expectations.
    """
    try:
        datasource = context.data_sources.get(name)
        logger.info(f"Datasource '{name}' ya existe.")
    except KeyError:
        datasource = context.data_sources.add_spark(name=name)
        logger.info(f"Datasource '{name}' creado.")
    return datasource


def get_or_create_asset(datasource, asset_name):
    """
    Recupera o crea un asset (dataframe asset) dentro del datasource.
    """
    if asset_name not in [a.name for a in datasource.assets]:
        data_asset = datasource.add_dataframe_asset(name=asset_name)
        logger.info(f"DataAsset '{asset_name}' creado.")
        return data_asset
    data_asset = datasource.get_asset(asset_name)
    logger.info(f"DataAsset '{asset_name}' ya existe.")
    return data_asset


def get_or_create_batch_definition(data_asset, batch_definition_name=BATCH_DEFINITION_NAME):
    """
    Recupera o crea un batch definition whole dataframe dentro del data asset.
    """
    if batch_definition_name not in [bd.name for bd in data_asset.batch_definitions]:
        batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)
        logger.info(f"BatchDefinition '{batch_definition_name}' creado.")
        return batch_definition
    batch_definition = data_asset.get_batch_definition(batch_definition_name)
    logger.info(f"BatchDefinition '{batch_definition_name}' ya existe.")
    return batch_definition


def generate_run_identifier(prefix="run"):
    """
    Genera un RunIdentifier para identificar la ejecución de validación con prefijo y timestamp.
    """
    return RunIdentifier(run_name=f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
