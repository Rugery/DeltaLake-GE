import great_expectations as gx
from great_expectations.exceptions import DataContextError

def create_validation_definition(context, batch_definition, suite, validation_name):
    """
    Crea o recupera una definición de validación en Great Expectations.

    Parámetros:
        context: Contexto de Great Expectations.
        batch_definition: Definición del lote de datos.
        suite: Suite de expectativas.
        validation_name: Nombre de la validación.

    Retorna:
        La definición de validación.
    """
    try:
        validation_def = context.validation_definitions.get(validation_name)
    except DataContextError:
        validation_def = gx.ValidationDefinition(
            data=batch_definition,
            suite=suite,
            name=validation_name
        )
        context.validation_definitions.add(validation_def)
    return validation_def

