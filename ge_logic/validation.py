import great_expectations as gx
from great_expectations.exceptions import DataContextError

def create_validation_definition(context, batch_definition, suite, validation_name):
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

