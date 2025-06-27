import csv
from pathlib import Path
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generar_indicadores(result_dict, table_name=None):
    raiz_proyecto = Path.cwd()
    carpeta_indicadores = raiz_proyecto / 'resultados' / 'indicadores'

    if table_name:
        carpeta_indicadores = carpeta_indicadores / table_name

    carpeta_indicadores.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    nombre_archivo = f"indicadores_{table_name}_{timestamp}.csv" if table_name else f"indicadores_{timestamp}.csv"
    archivo_path = carpeta_indicadores / nombre_archivo

    indicadores = {}

    validation_key = next(iter(result_dict))
    validation_data = result_dict[validation_key]

    if 'results' not in validation_data:
        logger.warning("No se encuentra 'results' en el diccionario.")
        return indicadores

    total_validaciones = validation_data['statistics']['evaluated_expectations']
    validaciones_exitosas = validation_data['statistics']['successful_expectations']
    validaciones_fallidas = validation_data['statistics']['unsuccessful_expectations']
    porcentaje_exito = validation_data['statistics']['success_percent']

    element_count = 0
    for result in validation_data['results']:
        if 'element_count' in result['result']:
            element_count = result['result']['element_count']
            if element_count > 0:
                break

    indicadores['total_validaciones'] = total_validaciones
    indicadores['validaciones_exitosas'] = validaciones_exitosas
    indicadores['validaciones_fallidas'] = validaciones_fallidas
    indicadores['porcentaje_exito'] = porcentaje_exito
    indicadores['element_count'] = element_count

    try:
        with archivo_path.open(mode='w', newline='', encoding='utf-8') as archivo_csv:
            writer = csv.DictWriter(archivo_csv, fieldnames=[
                "Estado", "Expectativa", "Descripción", "Recuento Inesperados", "Porcentaje Inesperado", "Total de Filas"
            ])

            writer.writeheader()

            writer.writerow({
                'Estado': 'Estadísticas Generales',
                'Expectativa': '',
                'Descripción': f"Total de validaciones: {total_validaciones}, Exitosas: {validaciones_exitosas}, Fallidas: {validaciones_fallidas}, Porcentaje de éxito: {porcentaje_exito}%",
                'Recuento Inesperados': '',
                'Porcentaje Inesperado': '',
                'Total de Filas': element_count
            })

            for result in validation_data['results']:
                expectation = result['expectation_config']
                success = result['success']
                expectation_name = expectation['meta'].get('name', 'Sin nombre')
                description = expectation['meta'].get('description', 'Sin descripción')
                unexpected_count = result['result'].get('unexpected_count')

                if unexpected_count is None:
                    unexpected_count = result['result'].get('observed_value')

                if unexpected_count is None:
                    unexpected_rows = result['result'].get('details', {}).get('unexpected_rows')
                    if unexpected_rows is not None:
                        unexpected_count = len(unexpected_rows)

                if unexpected_count is None:
                    unexpected_count = 'N/A'

                if isinstance(unexpected_count, int) and element_count > 0:
                    unexpected_percent = (unexpected_count / element_count) * 100
                else:
                    unexpected_percent = 'N/A'

                row = {
                    'Estado': 'Éxito' if success else 'Fallido',
                    'Expectativa': expectation_name,
                    'Descripción': description,
                    'Recuento Inesperados': unexpected_count,
                    'Porcentaje Inesperado': unexpected_percent,
                    'Total de Filas': element_count
                }

                writer.writerow(row)

        logger.info(f"CSV de indicadores guardado")
    except Exception as e:
        logger.error(f"Error al generar CSV de indicadores: {e}")

    return indicadores
