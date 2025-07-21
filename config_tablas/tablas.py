from expectativas.expectativas_clientes import suite_Clientes
from expectativas.expectativas_nombre import suite_nombre
from expectativas.expectativas_telefono import suite_Telefonos


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
        # {
        #     "table_name": "nombre",
        #     "delta_table_path": "./delta-tables/nombre",
        #     "suite": suite_nombre,
        # },
    ]