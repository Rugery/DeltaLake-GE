import great_expectations as ge

def expectativa_valores_por_referencia_caracteres(columna=None, referencia=None,caracteres=None):
    ref = referencia[columna].dropna().unique().tolist()
    prefijos_cortos = list({str(p)[:caracteres] for p in ref})
    regex_pattern = f"^({'|'.join(prefijos_cortos)})"
    return ge.expectations.ExpectColumnValuesToMatchRegex(
        column=columna,
        regex=regex_pattern,
        meta={
            "name": "Expectativas de Referencia de Caracteres",
            "description": f"Verifica que los valores de `{columna}` comiencen con uno de los prefijos válidos: {', '.join(prefijos_cortos)}."
        },
        description=f"Verifica que los valores de `{columna}` comiencen con uno de los prefijos válidos: {', '.join(prefijos_cortos)}."
    )

def expectativa_valor_unico(columna=None):
    return ge.expectations.ExpectColumnValuesToBeUnique(
        column=columna,
        meta={
            "name": "Expectativas valor único",
            "description": f"Verifica que los valores de `{columna}` sean únicos."
        },
        description=f"Verifica que los valores de `{columna}` sean únicos."
    )

def expectativa_valores_por_referencia(columna, referencia):
    ref_cod = referencia[columna].dropna().unique().tolist()
    return ge.expectations.ExpectColumnValuesToBeInSet(
        column=columna,
        value_set=ref_cod,
        meta={
            "name": "Expectativa Valores por referencia",
            "description": f"Verifica que los valores de la columna `{columna}` estén en la lista de referencia."
        },
        description=f"Verifica que los valores de la columna `{columna}` estén en la lista de referencia."
    )

def expectativa_valores_con_patron(columna=None, patron=None):
    return ge.expectations.ExpectColumnValuesToMatchRegex(
        column=columna,
        regex=patron,
        meta={
            "name": "Expectativas de Valores con Patrón",
            "description": f"Verifica que los valores de `{columna}` coincidan con el patrón `{patron}`."
        },
        description=f"Verifica que los valores de `{columna}` coincidan con el patrón `{patron}`."
    )

def expectativa_valores_tabla_equivalencia(columnas, tabla_equivalencia, separador="|"):

    # Crear la expresión SQL para concatenar columnas, ej: concat_ws('|', col1, col2, col3)
    columnas_concat = f"concat_ws('{separador}', {', '.join(columnas)})"
    tabla_equivalencia_tuplas = list(
    tabla_equivalencia[columnas]
    .dropna()
    .drop_duplicates()
    .itertuples(index=False, name=None)
    )

    # Concatenar cada tupla de valores permitidos usando el mismo separador y envolver en ''
    valores_formateados = []
    for tupla in tabla_equivalencia_tuplas:
        valor_concat = separador.join(str(v) for v in tupla)
        valores_formateados.append(f"'{valor_concat}'")

    valores_sql = ", ".join(valores_formateados)

    # Consulta SQL para buscar filas que NO estén en los valores permitidos
    sql = f"""
    SELECT *
    FROM {{batch}} AS t
    WHERE {columnas_concat} NOT IN ({valores_sql})
    """

    return ge.expectations.UnexpectedRowsExpectation(
        unexpected_rows_query=sql,
        meta={
            "name": "Validación combinaciones múltiples con SQL",
            "description": f"Verifica que las combinaciones de columnas {', '.join(columnas)} estén en la lista de valores permitidos"
        },
        description=f"Verifica que las combinaciones de columnas {', '.join(columnas)} estén en la lista de valores permitidos"
    )

def expectativa_valores_no_nullos(columna=None):
    return ge.expectations.ExpectColumnValuesToNotBeNull(
        column=columna,
        meta={
            "name": "Expectativas de Valores No Nulos",
            "description": f"Verifica que los valores de `{columna}` no sean nulos."
        },
        description=f"Verifica que los valores de `{columna}` no sean nulos."
    )

# def expectativa_valores_tipo(columna=None,tipo=None):
#     return ge.expectations.ExpectColumnValuesToBeOfType(
#         column=columna,
#         type_=tipo,
#         meta={
#             "name": "Expectativas de Valores de Tipo",
#             "description": f"Verifica que los valores de `{columna}` sean del tipo `{tipo}`."
#         },
#         description=f"Verifica que los valores de `{columna}` sean del tipo `{tipo}`."
#     )

def expectativa_valores_longitud_entre(columna=None, min_valor=None, max_valor=None):
    return ge.expectations.ExpectColumnValueLengthsToBeBetween(
    column=columna,
    min_value=min_valor,
    max_value=max_valor,
    meta={
        "name": "Expectativas de Longitud de Valores",
        "description": f"Verifica que la longitud de los valores de `{columna}` esté entre {min_valor} y {max_valor}."
    },
    description=f"Verifica que la longitud de los valores de `{columna}` esté entre {min_valor} y {max_valor}."
)

def expectativa_valores_longitud_igual(columna=None, longitud=None):
    return ge.expectations.ExpectColumnValueLengthsToEqual(
        column=columna,
        value=longitud,
        meta={
            "name": "Expectativas de Longitud de Valores",
            "description": f"Verifica que la longitud de los valores de `{columna}` sea exactamente {longitud}."
        },
        description=f"Verifica que la longitud de los valores de `{columna}` sea exactamente {longitud}."
    )