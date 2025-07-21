import great_expectations as ge

from expectativas.expectativas_comunes import expectativa_valor_unico

def suite_nombre():
    
    def expectativa_apellido_nombre_condicional(NAME1, NAME3, NAME4, condicion_sql=None):
        """
        Valida que NAME1 tenga la estructura 'apellido nombre' concatenando NAME4 y NAME3,
        transformando comas en espacios para apellido y nombre,
        solo si se cumple la condición SQL pasada en condicion_sql.

        Parámetros:
            NAME1: columna con apellido y nombre concatenado (ej. "NAME1")
            NAME3: columna con nombre(s) separado por comas (ej. "NAME3")
            NAME4: columna con apellido(s) separado por comas (ej. "NAME4")
            condicion_sql: condición SQL como string, ej. "FITYP = 'PN'"

        Retorna:
            Expectation personalizada (UnexpectedRowsExpectation)
        """

        if condicion_sql:
            where_clause = f"WHERE {condicion_sql} AND {NAME1} != CONCAT(REPLACE({NAME4}, ',', ' '), ' ', REPLACE({NAME3}, ',', ' '))"
        else:
            where_clause = f"WHERE {NAME1} != CONCAT(REPLACE({NAME4}, ',', ' '), ' ', REPLACE({NAME3}, ',', ' '))"

        sql = f"""
        SELECT *
        FROM {{batch}} AS t
        {where_clause}
        """

        return ge.expectations.UnexpectedRowsExpectation(
            unexpected_rows_query=sql,
            meta={
                "name": "Validar estructura apellido nombre condicional",
                "description": f"Verifica que {NAME1} sea la concatenación de {NAME4} + espacio + {NAME3} (reemplazando comas por espacios)" +
                                (f" cuando se cumple: {condicion_sql}" if condicion_sql else "")
            },
            description=f"Validar que {NAME1} tenga la estructura 'apellido nombre' (comas reemplazadas por espacios)" +
                        (f" si {condicion_sql}" if condicion_sql else "")
        )
     
    return [
        expectativa_apellido_nombre_condicional(
            NAME1="NAME1",
            NAME3="NAME3",
            NAME4="NAME4",
        ),
        expectativa_valor_unico(columna="id"),
    ]