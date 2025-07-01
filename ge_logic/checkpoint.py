import great_expectations as gx
from great_expectations.exceptions import DataContextError

def create_or_update_checkpoint(context, checkpoint_name, validation_definitions, actions):
    """
    Crea o actualiza un checkpoint en Great Expectations.

    Parámetros:
        context: Contexto de Great Expectations.
        checkpoint_name: Nombre del checkpoint.
        validation_definitions: Lista de definiciones de validación.
        actions: Acciones a ejecutar tras la validación (ej: actualizar docs).

    Retorna:
        El checkpoint creado o actualizado.
    """
    try:
        # Si existe, actualiza el checkpoint
        checkpoint = context.checkpoints.get(checkpoint_name)
        checkpoint.validation_definitions = validation_definitions
        checkpoint.actions = actions
        checkpoint.save()
    except DataContextError:
        # Si no existe, crea uno nuevo
        checkpoint = gx.Checkpoint(
            name=checkpoint_name,
            validation_definitions=validation_definitions,
            actions=actions,
            result_format={"result_format": "COMPLETE"}
        )
        context.checkpoints.add(checkpoint)
    return checkpoint
