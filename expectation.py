import pandas as pd
import great_expectations as ge

# Cargar las tablas desde los archivos Excel
equivalencias_df = pd.read_excel("./ref_y_equivalencias/equivalencia.xlsx")
ref_telefono_df = pd.read_excel("./ref_y_equivalencias/ref_telefono.xlsx")

# Convertir la columna 'telefono' a cadena de texto
equivalencias_df['telefono'] = equivalencias_df['telefono'].astype(str)
ref_telefono_df['telefono'] = ref_telefono_df['telefono'].astype(str)

# Obtener los primeros 3 dígitos de los teléfonos en ref_telefono_df
prefijos_validos = ref_telefono_df['telefono'].apply(lambda x: x[:3]).unique()


# Convertir equivalencias_df a tuplas: prefijo_telefono, cod_municipio
equivalencias_set = set(
    (telefono[:3], cod_municipio)
    for telefono, cod_municipio in equivalencias_df[['telefono', 'cod_municipio']].itertuples(index=False, name=None)
)

# Generar los valores SQL para la subconsulta
equivalencias_values = ",\n        ".join(
    [f"('{prefijo}', {cod_municipio})" for prefijo, cod_municipio in equivalencias_set]
)

def expectations_Telefono():
    # 1. Expectativa usando LEFT ANTI JOIN
    sql_query_equivalencias = f"""
    SELECT
        t.id,
        t.telefono,
        t.cod_municipio
    FROM
        {{batch}} AS t
    LEFT ANTI JOIN (
        VALUES
        {equivalencias_values}
    ) AS v(prefijo, cod_municipio)
    ON SUBSTRING(t.telefono, 1, 3) = v.prefijo AND t.cod_municipio = v.cod_municipio
    """

    expectation_equivalencias = ge.expectations.UnexpectedRowsExpectation(
        unexpected_rows_query=sql_query_equivalencias,
        meta={"name": "expectations_Equivalencias", "description": "Las combinaciones de teléfono (prefijo) y cod_municipio deben coincidir con las equivalencias válidas."},
        description="Validación de equivalencias de teléfono y código de municipio",
    )

    # 2. Validación de prefijos válidos
    sql_query_prefijos = f"""
    SELECT
        id,
        telefono,
        cod_municipio
    FROM
        {{batch}}
    WHERE
        SUBSTRING(telefono, 1, 3) NOT IN ({', '.join([f"'{prefijo}'" for prefijo in prefijos_validos])})
    """

    expectation_prefijos = ge.expectations.UnexpectedRowsExpectation(
        unexpected_rows_query=sql_query_prefijos,
        meta={"name": "expectations_Prefijos", "description": "Los teléfonos deben comenzar con uno de los prefijos válidos."},
        description="Validación de prefijos de teléfono",
    )

    return [
        expectation_equivalencias,
        expectation_prefijos,
        ge.expectations.ExpectColumnValuesToBeBetween(
            column="cod_municipio",
            min_value=1,
            max_value=4,
            meta={"name": "cod_municipio_rango", "description": "El código de municipio debe estar entre 1 y 4."},
            description="El código de municipio debe estar entre 1 y 4."
        ),
        ge.expectations.ExpectColumnValuesToMatchRegex(
            column="telefono", 
            regex=r"^60\d{8}$",
            meta={"name": "formato_telefono", "description": "El número de teléfono debe comenzar con 60 y tener exactamente 10 dígitos."},
            description="El número de teléfono debe comenzar con 60 y tener exactamente 10 dígitos."
        )
]

