import great_expectations as gx
from great_expectations.exceptions import DataContextError

def create_or_update_checkpoint(context, checkpoint_name, validation_definitions, actions):
    try:
        # Obtener el checkpoint si ya existe
        checkpoint = context.checkpoints.get(checkpoint_name)
        
        # Actualizar las definiciones de validación y las acciones
        checkpoint.validation_definitions = validation_definitions
        checkpoint.actions = actions
        checkpoint.save()
        
    except DataContextError:
        # Crear un nuevo checkpoint si no existe
        checkpoint = gx.Checkpoint(
            name=checkpoint_name,
            validation_definitions=validation_definitions,
            actions=actions,
            result_format={"result_format": "COMPLETE"}  # Asegúrate de que este parámetro se pase
        )
        context.checkpoints.add(checkpoint)
    
    return checkpoint
