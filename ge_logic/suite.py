import great_expectations as gx
from great_expectations.exceptions import DataContextError

def create_or_update_suite(context, suite_name, expectations):
    try:
        suite = context.suites.get(suite_name)
        suite.expectations = []
        print(f"Suite '{suite_name}' actualizada.")
    except DataContextError:
        suite = gx.ExpectationSuite(name=suite_name)
        print(f"Suite '{suite_name}' creada.")

    for exp in expectations:
        suite.add_expectation(exp)

    context.suites.add_or_update(suite)
    return suite