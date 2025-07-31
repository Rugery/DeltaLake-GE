from expectativas.expectativas_clientes import suite_Clientes
from expectativas.expectativas_nombre import suite_nombre
from expectativas.expectativas_telefono import suite_Telefonos


tables_config = [
        {
            "table_name": "clientes",
            "delta_table_path": "./delta-tables/clientes",
            "suite": suite_Clientes,
        },
    ]