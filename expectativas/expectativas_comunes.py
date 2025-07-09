import great_expectations as ge

# =========================
# Expectativas Comunes GE
# =========================

def expectativa_valores_por_referencia_caracteres_con_condicion(condicion, columna, referencia, caracteres):
    """
    Espera que los valores de la columna comiencen con uno de los prefijos válidos
    extraídos de una lista de referencia, considerando solo los primeros 'caracteres' caracteres.
    Parámetros:
        condicion: Condición a evaluar en formato SQL (ej: 'col("ciudad") == "Ciudad8"').
        columna: Nombre de la columna a validar.
        referencia: DataFrame de referencia con los valores válidos.
        caracteres: Cantidad de caracteres a considerar del prefijo.
    Retorna:
        Expectation de tipo ExpectColumnValuesToMatchRegex con condición.   
    """
    ref = referencia[columna].dropna().unique().tolist()
    prefijos_cortos = list({str(p)[:caracteres] for p in ref})
    regex_pattern = f"^({'|'.join(prefijos_cortos)})"
    return ge.expectations.ExpectColumnValuesToMatchRegex(
        column=columna,
        regex=regex_pattern,
        row_condition=condicion,
         condition_parser="great_expectations",
        meta={
            "name": "Expectativa Valores por Referencia con Prefijos",
            "description": (
                f"Si se cumple `{condicion}`, entonces los valores de `{columna}` deben comenzar con uno de los prefijos válidos: "
                f"{', '.join(prefijos_cortos)}."
            )   
        },
        description=(
            f"Si se cumple `{condicion}`, entonces los valores de `{columna}` deben comenzar con uno de los prefijos válidos: "
            f"{', '.join(prefijos_cortos)}."
        )   
    )

def expectativa_valor_unico(columna):
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

def expectativa_valores_por_referencia_caracteres(columna, referencia, caracteres):
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

def expectativa_valores_con_patron(columna, patron):
    """
    Espera que los valores de la columna coincidan con un patrón regex.
    Parámetros:
        columna: Nombre de la columna a validar.
        patron: Expresión regular que los valores deben cumplir.
    Retorna:
        Expectation de tipo ExpectColumnValuesToMatchRegex.
    """
    return ge.expectations.ExpectColumnValuesToMatchRegex(
        column=columna,
        regex=patron,
        meta={
            "name": "Expectativa Valores con Patrón",
            "description": f"Verifica que los valores de `{columna}` coincidan con el patrón `{patron}`."
        },
        description=f"Verifica que los valores de `{columna}` coincidan con el patrón `{patron}`."
    )

def expectativa_valores_tabla_equivalencia(columnas, tabla_equivalencia, separador="|", condicion=None):
    columnas_concat = f"concat_ws('{separador}', {', '.join(columnas)})"
    tabla_equivalencia_tuplas = list(
        tabla_equivalencia[columnas]
        .dropna()
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )

    valores_formateados = []
    for tupla in tabla_equivalencia_tuplas:
        valor_concat = separador.join(str(v) for v in tupla)
        valores_formateados.append(f"'{valor_concat}'")

    valores_sql = ", ".join(valores_formateados)

    where_clauses = []
    if condicion:
        where_clauses.append(condicion)
    where_clauses.append(f"{columnas_concat} NOT IN ({valores_sql})")

    where_sql = " AND ".join(where_clauses)

    sql = f"""
    SELECT *
    FROM {{batch}} AS t
    WHERE {where_sql}
    """

    meta_data = {
        "name": "Validación combinaciones múltiples con SQL",
        "description": f"Verifica que las combinaciones de columnas {', '.join(columnas)} estén en la lista de valores permitidos",
        "condicion": condicion
    }

    return ge.expectations.UnexpectedRowsExpectation(
        unexpected_rows_query=sql,
        meta=meta_data,
        description=meta_data["description"]
    )

def expectativa_valores_no_nulos(columna):
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

def expectativa_valores_tipo(columna, tipo):
    """
    Espera que los valores de la columna sean de un tipo específico.
    """
    return ge.expectations.ExpectColumnValuesToBeOfType(
        column=columna,
        type_=tipo,
        meta={
            "name": "Expectativas de Valores de Tipo",
            "description": f"Verifica que los valores de `{columna}` sean del tipo `{tipo}`."
        },
        description=f"Verifica que los valores de `{columna}` sean del tipo `{tipo}`."
    )

def expectativa_valores_longitud_entre(columna, min_valor, max_valor):
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

def expectativa_valores_longitud_igual(columna, longitud):
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

def expectativa_columna_valores_entre(columna, min_valor, max_valor):
    """
    Espera que los valores de la columna estén entre dos valores.

    Parámetros:
        columna: Nombre de la columna a validar.
        min_valor: Valor mínimo permitido.
        max_valor: Valor máximo permitido.

    Retorna:
        Expectation de tipo ExpectColumnValuesToBeBetween.
    """
    return ge.expectations.ExpectColumnValuesToBeBetween(
        column=columna,
        min_value=min_valor,
        max_value=max_valor,
        meta={
            "name": "Expectativas de Valores Entre",
            "description": f"Verifica que los valores de `{columna}` estén entre {min_valor} y {max_valor}."
        },
        description=f"Verifica que los valores de `{columna}` estén entre {min_valor} y {max_valor}."
    )

def expectativa_columnas_esperadas(columnas):
    """
    Espera que las columnas esperadas existan en el DataFrame.

    Parámetros:
        columnas: Lista de nombres de columnas esperadas.

    Retorna:
        Expectation de tipo ExpectTableColumnsToMatchSet.
    """
    return ge.expectations.ExpectTableColumnsToMatchSet(
        column_set=columnas,
        meta={
            "name": "Expectativas de Columnas Esperadas",
            "description": f"Verifica que las columnas esperadas {', '.join(columnas)} existan en el DataFrame."
        },
        description=f"Verifica que las columnas esperadas {', '.join(columnas)} existan en el DataFrame."
    )

def expectativa_combinacion_columnas_unicas(columnas):
    """
    Espera que la combinación de valores en las columnas especificadas sea única en el DataFrame.

    Parámetros:
        columnas: Lista de nombres de columnas a validar como combinación única.

    Retorna:
        Expectation de tipo ExpectCompoundColumnsToBeUnique.
    """
    return ge.expectations.ExpectCompoundColumnsToBeUnique(
        column_list=columnas,
        meta={
            "name": "Combinación única de columnas",
            "description": f"Verifica que la combinación de columnas {', '.join(columnas)} sea única en todas las filas."
        },
        description=f"Verifica que la combinación de columnas {', '.join(columnas)} sea única en todas las filas."
    )

# def expectativa_valores_no_coinciden_con_patron(condicion, columna, patron):
    """
    Espera que los valores de la columna no coincidan con un patrón regex.
    Parámetros:
        condicion: Condición a evaluar en formato SQL (ej: 'col("ciudad") == "Ciudad8"').
        columna: Nombre de la columna a validar.
        patron: Expresión regular que los valores no deben cumplir.
    Retorna:
        Expectation de tipo ExpectColumnValuesToNotMatchRegex con condición.
    """
    return ge.expectations.ExpectColumnValuesToNotMatchRegex(
        column=columna,
        regex=patron,
        row_condition=condicion,
        condition_parser="great_expectations",
        meta={
            "name": "Expectativa Valores No Coinciden con Patrón",
            "description": f"Verifica que los valores de `{columna}` no coincidan con el patrón `{patron}` si se cumple la condición `{condicion}`."    
        },
        description=f"Verifica que los valores de `{columna}` no coincidan con el patrón `{patron}` si se cumple la condición `{condicion}`."
    )   

def expectativa_valores_distintos_por_referencia(columna, referencia):
    """
    Espera que los valores de la columna sean distintos a los de una lista de referencia.

    Parámetros:
        columna: Nombre de la columna a validar.
        referencia: DataFrame de referencia con los valores no permitidos.

    Retorna:
        Expectation de tipo ExpectColumnValuesToNotBeInSet.
    """
    ref_cod = referencia[columna].dropna().unique().tolist()
    return ge.expectations.ExpectColumnValuesToNotBeInSet(
        column=columna,
        value_set=ref_cod,
        meta={
            "name": "Expectativa Valores Distintos por Referencia",
            "description": f"Verifica que los valores de la columna `{columna}` no estén en la lista de referencia."
        },
        description=f"Verifica que los valores de la columna `{columna}` no estén en la lista de referencia."
    )

def expectativa_cantidad_filas_entre(min_valor, max_valor):
    """
    Espera que la cantidad de filas del DataFrame esté entre dos valores.

    Parámetros:
        min_valor: Cantidad mínima de filas esperadas.
        max_valor: Cantidad máxima de filas esperadas.

    Retorna:
        Expectation de tipo ExpectTableRowCountToBeBetween.
    """
    return ge.expectations.ExpectTableRowCountToBeBetween(
        min_value=min_valor,
        max_value=max_valor,
        meta={
            "name": "Expectativas de Cantidad de Filas",
            "description": f"Verifica que la cantidad de filas esté entre {min_valor} y {max_valor}."
        },
        description=f"Verifica que la cantidad de filas esté entre {min_valor} y {max_valor}."
    )

def expectativa_valores_no_vacios(columna):
    """
    Espera que los valores de la columna no sean cadenas vacías.

    Parámetros:
        columna: Nombre de la columna a validar.

    Retorna:
        Expectation personalizada que verifica que los valores no sean cadenas vacías.
    """
    return ge.expectations.ExpectColumnValuesToNotMatchRegex(
        column=columna,
        regex="^$",
        meta={
            "name": "Expectativa valores no vacíos",
            "description": f"Verifica que los valores de `{columna}` no sean cadenas vacías."
        },
        description=f"Verifica que los valores de `{columna}` no sean cadenas vacías."
    )

def expectativa_con_condicional_y_patron(condicion,columna, patron):
    """
    Espera que los valores de una columna cumplan una condición y coincidan con un patrón regex.
    Parámetros:
        condicion: Condición a evaluar en formato SQL (ej: 'col("ciudad") == "Ciudad8"').
        columna: Nombre de la columna a validar.
        patron: Expresión regular que los valores deben cumplir.
    Retorna:
        Expectation de tipo ExpectColumnValuesToMatchRegex con condición.
    """
    return ge.expectations.ExpectColumnValuesToMatchRegex(
        column=columna,
        regex=patron,
        condition_parser="great_expectations",
        row_condition=condicion,
        meta={
            "name": "Expectativa con Condicional y Patrón",
            "description": f"Verifica que los valores de `{columna}` cumplan la condición `{condicion}` y coincidan con el patrón `{patron}`."
        },
        description=f"Verifica que los valores de `{columna}` cumplan la condición `{condicion}` y coincidan con el patrón `{patron}`."
    )

# def expectativa_prefijo_ciudad_para_municipio_8(referencia, columna="ciudad", campo_condicional="cod_municipio"):
    """
    Si `cod_municipio == 8`, valida que los primeros 3 caracteres de la columna `ciudad`
    estén en una lista de prefijos válidos proporcionada por una tabla de referencia.

    Parámetros:
        referencia: DataFrame de referencia con valores válidos de `ciudad`.
        columna: Nombre de la columna a validar (default "ciudad").
        campo_condicional: Campo que activa la condición (default "cod_municipio").

    Retorna:
        Expectation de tipo ExpectColumnValuesToMatchRegex con condición.
    """
    # Extraer prefijos válidos de la referencia
    prefijos_validos = list({str(valor)[:3] for valor in referencia[columna].dropna().unique()})
    patron_regex = f"^({'|'.join(prefijos_validos)})"

    return ge.expectations.ExpectColumnValuesToMatchRegex(
        column=columna,
        regex=patron_regex,
        row_condition=f'col("{campo_condicional}") == 8',
        condition_parser="great_expectations__experimental__",
        meta={
            "name": "Prefijos válidos de ciudad para cod_municipio 8",
            "description": (
                f"Si `{campo_condicional}` es 8, entonces `{columna}` debe iniciar con uno de los siguientes "
                f"prefijos válidos: {', '.join(prefijos_validos)}."
            )
        },
        description=(
            f"Si `{campo_condicional}` es 8, entonces `{columna}` debe iniciar con uno de los siguientes "
            f"prefijos válidos: {', '.join(prefijos_validos)}."
        )
    )

def expectativa_condicional_tabla_referencia(condicion, columna, referencia):
    """
    Espera que los valores de una columna estén en una lista de referencia si se cumple una condición.
    Parámetros:
        condicion: Condición a evaluar en formato SQL (ej: 'col("ciudad") == "Ciudad8"').
        columna: Nombre de la columna a validar.
        referencia: DataFrame de referencia con los valores válidos.
    Retorna:
        Expectation de tipo ExpectColumnValuesToBeInSet con condición.
    """
    ref_cod = referencia[columna].dropna().unique().tolist()
    return ge.expectations.ExpectColumnValuesToBeInSet(
        column=columna,
        value_set=ref_cod,
        row_condition=condicion,
        condition_parser="great_expectations",
        meta={
            "name": "Expectativa Condicional con Tabla de Referencia",
            "description": f"Verifica que los valores de `{columna}` estén en la lista de referencia si se cumple la condición `{condicion}`."
        },
        description=f"Verifica que los valores de `{columna}` estén en la lista de referencia si se cumple la condición `{condicion}`."
    )

def expectativa_condicional_valores_en_lista(condicion,columna, lista):
    """
    Espera que los valores de una columna estén en una lista de valores si se cumple una condición.
    Parámetros:
        condicion: Condición a evaluar en formato SQL (ej: 'col("ciudad") == "Ciudad8"').
        columna: Nombre de la columna a validar.
        lista: Lista de valores válidos.
    Retorna:
        Expectation de tipo ExpectColumnValuesToBeInSet con condición.
        
    """
    return ge.expectations.ExpectColumnValuesToBeInSet(
        column=columna,
        value_set=lista,
        row_condition=condicion,
        condition_parser="great_expectations",
        meta={
            "name": "Expectativa Condicional con Valores en Lista",
            "description": f"Verifica que los valores de `{columna}` estén en la lista de valores si se cumple la condición `{condicion}`."
        },
        description=f"Verifica que los valores de `{columna}` estén en la lista de valores si se cumple la condición `{condicion}`."
    )

def expectativa_primera_palabra_en_referencia_con_condicion(condicion, columna, referencia):
    """
    Espera que la primera palabra de los valores de una columna esté en una lista de referencia
    si se cumple una condición.

    Parámetros:
        condicion: Condición a evaluar en formato SQL (ej: 'col("ciudad") == "Ciudad8"').
        columna: Nombre de la columna a validar.
        referencia: DataFrame de referencia con los valores válidos.

    Retorna:
        Expectation de tipo ExpectColumnValuesToMatchRegex con condición.
    """
    ref = referencia[columna].dropna().unique().tolist()
    palabras_validas = list({str(valor).split()[0] for valor in ref})
    patron_regex = f"^({'|'.join(palabras_validas)})"

    return ge.expectations.ExpectColumnValuesToMatchRegex(
        column=columna,
        regex=patron_regex,
        row_condition=condicion,
        condition_parser="great_expectations",
        meta={
            "name": "Primera palabra en referencia con condición",
            "description": (
                f"Si se cumple `{condicion}`, entonces la primera palabra de `{columna}` debe estar en "
                f"la lista de palabras válidas: {', '.join(palabras_validas)}."
            )
        },
        description=(
            f"Si se cumple `{condicion}`, entonces la primera palabra de `{columna}` debe estar en "
            f"la lista de palabras válidas: {', '.join(palabras_validas)}."
        )
    )

def expectativa_valores_no_permitidos(columna, valores):
    """
    Espera que los valores de la columna no estén en una lista de valores no permitidos.

    Parámetros:
        columna: Nombre de la columna a validar.
        valores: Lista de valores no permitidos.

    Retorna:
        Expectation de tipo ExpectColumnValuesToNotBeInSet.
    """
    return ge.expectations.ExpectColumnValuesToNotBeInSet(
        column=columna,
        value_set=valores,
        meta={
            "name": "Expectativa Valores No Permitidos",
            "description": f"Verifica que los valores de `{columna}` no estén en la lista de valores no permitidos."
        },
        description=f"Verifica que los valores de `{columna}` no estén en la lista de valores no permitidos."
    )
