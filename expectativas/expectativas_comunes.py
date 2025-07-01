import great_expectations as ge

# =========================
# Expectativas Comunes GE
# =========================

def expectativa_valores_por_referencia_caracteres(columna=None, referencia=None, caracteres=None):
    """
    Espera que los valores de una columna comiencen con ciertos caracteres (prefijos) válidos,
    extraídos de una referencia.

    Parámetros:
        columna: Nombre de la columna a validar.
        referencia: DataFrame de referencia con los valores válidos.
        caracteres: Número de caracteres a tomar como prefijo.

    Retorna:
        Expectation de tipo ExpectColumnValuesToMatchRegex.
    """
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
    """
    Espera que todos los valores de la columna sean únicos.

    Parámetros:
        columna: Nombre de la columna a validar.

    Retorna:
        Expectation de tipo ExpectColumnValuesToBeUnique.
    """
    return ge.expectations.ExpectColumnValuesToBeUnique(
        column=columna,
        meta={
            "name": "Expectativas valor único",
            "description": f"Verifica que los valores de `{columna}` sean únicos."
        },
        description=f"Verifica que los valores de `{columna}` sean únicos."
    )

def expectativa_valores_por_referencia(columna, referencia):
    """
    Espera que los valores de la columna estén en una lista de referencia.

    Parámetros:
        columna: Nombre de la columna a validar.
        referencia: DataFrame de referencia con los valores válidos.

    Retorna:
        Expectation de tipo ExpectColumnValuesToBeInSet.
    """
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
    """
    Espera que los valores de la columna coincidan con un patrón regex.

    Parámetros:
        columna: Nombre de la columna a validar.
        patron: Expresión regular a cumplir.

    Retorna:
        Expectation de tipo ExpectColumnValuesToMatchRegex.
    """
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
    """
    Espera que la combinación de valores de varias columnas esté en una tabla de equivalencias.

    Parámetros:
        columnas: Lista de nombres de columnas a validar.
        tabla_equivalencia: DataFrame con las combinaciones válidas.
        separador: Separador para concatenar los valores.

    Retorna:
        Expectation personalizada (UnexpectedRowsExpectation).
    """
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
    """
    Espera que los valores de la columna no sean nulos.

    Parámetros:
        columna: Nombre de la columna a validar.

    Retorna:
        Expectation de tipo ExpectColumnValuesToNotBeNull.
    """
    return ge.expectations.ExpectColumnValuesToNotBeNull(
        column=columna,
        meta={
            "name": "Expectativas de Valores No Nulos",
            "description": f"Verifica que los valores de `{columna}` no sean nulos."
        },
        description=f"Verifica que los valores de `{columna}` no sean nulos."
    )

# #Ejemplo de expectativa de tipo (comentada)
# def expectativa_valores_tipo(columna=None, tipo=None):
#     """
#     Espera que los valores de la columna sean de un tipo específico.
#     """
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
    """
    Espera que la longitud de los valores de la columna esté entre dos valores.

    Parámetros:
        columna: Nombre de la columna a validar.
        min_valor: Longitud mínima.
        max_valor: Longitud máxima.

    Retorna:
        Expectation de tipo ExpectColumnValueLengthsToBeBetween.
    """
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
    """
    Espera que la longitud de los valores de la columna sea igual a un valor específico.

    Parámetros:
        columna: Nombre de la columna a validar.
        longitud: Longitud exacta requerida.

    Retorna:
        Expectation de tipo ExpectColumnValueLengthsToEqual.
    """
    return ge.expectations.ExpectColumnValueLengthsToEqual(
        column=columna,
        value=longitud,
        meta={
            "name": "Expectativas de Longitud de Valores",
            "description": f"Verifica que la longitud de los valores de `{columna}` sea exactamente {longitud}."
        },
        description=f"Verifica que la longitud de los valores de `{columna}` sea exactamente {longitud}."
    )