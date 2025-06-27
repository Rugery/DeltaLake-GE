from expectations.expectativas_clientes import suite_Clientes
from expectations.expectativas_telefono import suite_Telefonos


tables_config = [
        {
            "table_name": "clientes",
            "delta_table_path": "./delta-tables/clientes",
            "suite": suite_Clientes,
        },
        {
            "table_name": "telefonos",
            "delta_table_path": "./delta-tables/telefonos",
            "suite": suite_Telefonos,
        },
    ]