import great_expectations as ge

# =========================
# Expectativas Comunes GE
# =========================

def expectativa_valores_por_referencia_caracteres_con_condicion(condicion, columna, referencia, caracteres, name=None):
    """
    Verifica que los valores de una columna comiencen con uno de los prefijos válidos
    de una lista de referencia, si se cumple una condición.
    Parámetros:
        condicion: Condición a evaluar en formato SQL (ej: 'col("ciudad") == "Ciudad8"').
        columna: Nombre de la columna a validar.
        referencia: DataFrame de referencia con los valores válidos.
        caracteres: Cantidad de caracteres del prefijo a considerar.
        name: Nombre de la expectativa (opcional).
    Retorna:
        Expectation de tipo ExpectColumnValuesToMatchRegex con condición.
    """
    name = name or "Expectativa Valores por Referencia con Prefijos"
    ref = referencia[columna].dropna().unique().tolist()
    prefijos_cortos = list({str(p)[:caracteres] for p in ref})
    regex_pattern = f"^({'|'.join(prefijos_cortos)})"
    descripcion = (
        f"Si se cumple `{condicion}`, entonces los valores de `{columna}` deben comenzar con uno de los prefijos válidos: "
        f"{', '.join(prefijos_cortos)}."
    )
    return ge.expectations.ExpectColumnValuesToMatchRegex(
        column=columna,
        regex=regex_pattern,
        row_condition=condicion,
        condition_parser="great_expectations",
        meta={
            "name": name,
            "description": descripcion
        },
        description=descripcion
    )

def expectativa_valor_unico(columna, name=None):
    """
    Verifica que los valores de una columna sean únicos.
    Parámetros:
        columna: Nombre de la columna a validar.
        name: Nombre de la expectativa (opcional).
        Retorna:
            Expectation de tipo ExpectColumnValuesToBeUnique.
    """

    name = name or "Expectativas valor único"
    descripcion = f"Verifica que los valores de `{columna}` sean únicos."
    return ge.expectations.ExpectColumnValuesToBeUnique(
        column=columna,
        meta={
            "name": name,
            "description": descripcion
        },
        description=descripcion
    )

def expectativa_valores_por_referencia(columna, referencia, name=None):
    """
    Verifica que los valores de una columna estén en una lista de referencia.
    Parámetros:
        columna: Nombre de la columna a validar.
        referencia: DataFrame de referencia con los valores válidos.
        name: Nombre de la expectativa (opcional).
        Retorna:
            Expectation de tipo ExpectColumnValuesToBeInSet.
    """

    name = name or "Expectativa Valores por referencia"
    ref_cod = referencia[columna].dropna().unique().tolist()
    descripcion = f"Verifica que los valores de la columna `{columna}` estén en la lista de referencia."
    return ge.expectations.ExpectColumnValuesToBeInSet(
        column=columna,
        value_set=ref_cod,
        meta={
            "name": name,
            "description": descripcion
        },
        description=descripcion
    )

def expectativa_valores_por_referencia_caracteres(columna, referencia, caracteres, name=None):
    """
    Espera que los valores de una columna comiencen con ciertos caracteres (prefijos) válidos,
    extraídos de una referencia.
    Parámetros:
        columna: Nombre de la columna a validar.
        referencia: DataFrame de referencia con los valores válidos.
        caracteres: Cantidad de caracteres del prefijo a considerar.
        name: Nombre de la expectativa (opcional).
    Retorna:
        Expectation de tipo ExpectColumnValuesToMatchRegex.
    """

    name = name or "Expectativas de Referencia de Caracteres"
    ref = referencia[columna].dropna().unique().tolist()
    prefijos_cortos = list({str(p)[:caracteres] for p in ref})
    regex_pattern = f"^({'|'.join(prefijos_cortos)})"
    descripcion = f"Verifica que los valores de `{columna}` comiencen con uno de los prefijos válidos: {', '.join(prefijos_cortos)}."
    return ge.expectations.ExpectColumnValuesToMatchRegex(
        column=columna,
        regex=regex_pattern,
        meta={
            "name": name,
            "description": descripcion
        },
        description=descripcion
    )

def expectativa_valores_con_patron(columna, patron, name=None):
    """
    Verifica que los valores de una columna coincidan con un patrón regex.
    Parámetros:
        columna: Nombre de la columna a validar.
        patron: Patrón regex a validar.
        name: Nombre de la expectativa (opcional).
        Retorna:
            Expectation de tipo ExpectColumnValuesToMatchRegex.
    """

    name = name or "Expectativa Valores con Patrón"
    descripcion = f"Verifica que los valores de `{columna}` coincidan con el patrón `{patron}`."
    return ge.expectations.ExpectColumnValuesToMatchRegex(
        column=columna,
        regex=patron,
        meta={
            "name": name,
            "description": descripcion
        },
        description=descripcion
    )

def expectativa_valores_tabla_equivalencia(columnas, tabla_equivalencia, separador="|", condicion=None, name=None):
    """
    Verifica que las combinaciones de valores en varias columnas estén en una tabla de equivalencia
    usando una cláusula WHERE opcional.
    Parámetros:
        columnas: Lista de nombres de columnas a combinar.
        tabla_equivalencia: DataFrame con las combinaciones válidas.
        separador: Separador para concatenar los valores de las columnas.
        condicion: Condición SQL opcional para filtrar filas.
        name: Nombre de la expectativa (opcional).
        Retorna:
            Expectation de tipo UnexpectedRowsExpectation.
    """

    name = name or "Validación combinaciones múltiples con SQL"

    # Crear la concatenación de columnas para SQL
    columnas_concat = f"concat_ws('{separador}', {', '.join(columnas)})"

    # Extraer combinaciones únicas permitidas como tuplas
    tabla_equivalencia_tuplas = list(
        tabla_equivalencia[columnas]
        .dropna()
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )

    # Formatear valores para la cláusula IN del SQL
    valores_formateados = [f"'{separador.join(str(v) for v in tupla)}'" for tupla in tabla_equivalencia_tuplas]
    valores_sql = ", ".join(valores_formateados)

    # Construir cláusula WHERE combinando condición y exclusión de valores fuera del conjunto válido
    where_clauses = []
    if condicion:
        where_clauses.append(condicion)
    where_clauses.append(f"{columnas_concat} NOT IN ({valores_sql})")
    where_sql = " AND ".join(where_clauses)

    # Consulta SQL para detectar filas inesperadas
    sql = f"""
    SELECT *
    FROM {{batch}} AS t
    WHERE {where_sql}
    """

    meta_data = {
        "name": name,
        "description": f"Verifica que las combinaciones de columnas {', '.join(columnas)} estén en la lista de valores permitidos",
        "condicion": condicion
    }

    return ge.expectations.UnexpectedRowsExpectation(
        unexpected_rows_query=sql,
        meta=meta_data,
        description=meta_data["description"]
    )

def expectativa_valores_no_nulos(columna, name=None):
    """
    Verifica que los valores de una columna no sean nulos.
    Parámetros:
        columna: Nombre de la columna a validar.
        name: Nombre de la expectativa (opcional).
        Retorna:
            Expectation de tipo ExpectColumnValuesToNotBeNull.
    """

    name = name or "Expectativas de Valores No Nulos"
    descripcion = f"Verifica que los valores de `{columna}` no sean nulos."
    return ge.expectations.ExpectColumnValuesToNotBeNull(
        column=columna,
        meta={
            "name": name,
            "description": descripcion
        },
        description=descripcion
    )

def expectativa_valores_tipo(columna, tipo, name=None):
    """
    Verifica que los valores de una columna sean de un tipo específico.
    Parámetros:
        columna: Nombre de la columna a validar.
        tipo: Tipo de dato esperado (ej. "StringType", "IntegerType").
        name: Nombre de la expectativa (opcional).
        Retorna:
            Expectation de tipo ExpectColumnValuesToBeOfType.
    """

    name = name or "Expectativas de Valores de Tipo"
    descripcion = f"Verifica que los valores de `{columna}` sean del tipo `{tipo}`."
    return ge.expectations.ExpectColumnValuesToBeOfType(
        column=columna,
        type_=tipo,
        meta={
            "name": name,
            "description": descripcion
        },
        description=descripcion
    )

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

def expectativa_valores_longitud_entre(columna, min_valor, max_valor, name=None):
    """
    Verifica que la longitud de los valores de una columna esté entre dos valores.
    Parámetros:
        columna: Nombre de la columna a validar.
        min_valor: Valor mínimo de longitud.
        max_valor: Valor máximo de longitud.
        name: Nombre de la expectativa (opcional).
        Retorna:
            Expectation de tipo ExpectColumnValueLengthsToBeBetween.
    """

    name = name or "Expectativas de Longitud de Valores"
    descripcion = f"Verifica que la longitud de los valores de `{columna}` esté entre {min_valor} y {max_valor}."
    return ge.expectations.ExpectColumnValueLengthsToBeBetween(
        column=columna,
        min_value=min_valor,
        max_value=max_valor,
        meta={
            "name": name,
            "description": descripcion
        },
        description=descripcion
    )

def expectativa_valores_longitud_igual(columna, longitud, name=None):
    """
    Verifica que la longitud de los valores de una columna sea exactamente igual a un valor.
    Parámetros:
        columna: Nombre de la columna a validar.
        longitud: Longitud exacta esperada.
        name: Nombre de la expectativa (opcional).
        Retorna:
            Expectation de tipo ExpectColumnValueLengthsToEqual.
    """

    name = name or "Expectativas de Longitud de Valores"
    descripcion = f"Verifica que la longitud de los valores de `{columna}` sea exactamente {longitud}."
    return ge.expectations.ExpectColumnValueLengthsToEqual(
        column=columna,
        value=longitud,
        meta={
            "name": name,
            "description": descripcion
        },
        description=descripcion
    )

def expectativa_columna_valores_entre(columna, min_valor, max_valor, name=None):
    """
    Verifica que los valores de una columna estén entre dos valores específicos.
    Parámetros:
        columna: Nombre de la columna a validar.
        min_valor: Valor mínimo esperado.
        max_valor: Valor máximo esperado.
        name: Nombre de la expectativa (opcional).
        Retorna:
            Expectation de tipo ExpectColumnValuesToBeBetween.
    """

    name = name or "Expectativas de Valores Entre"
    descripcion = f"Verifica que los valores de `{columna}` estén entre {min_valor} y {max_valor}."
    return ge.expectations.ExpectColumnValuesToBeBetween(
        column=columna,
        min_value=min_valor,
        max_value=max_valor,
        meta={
            "name": name,
            "description": descripcion
        },
        description=descripcion
    )

def expectativa_columnas_esperadas(columnas, name=None):
    """
    Verifica que las columnas esperadas existan en el DataFrame.
    Parámetros:
        columnas: Lista de nombres de columnas esperadas.
        name: Nombre de la expectativa (opcional).
        Retorna:
            Expectation de tipo ExpectTableColumnsToMatchSet.
    """

    name = name or "Expectativas de Columnas Esperadas"
    descripcion = f"Verifica que las columnas esperadas {', '.join(columnas)} existan en el DataFrame."
    return ge.expectations.ExpectTableColumnsToMatchSet(
        column_set=columnas,
        meta={
            "name": name,
            "description": descripcion
        },
        description=descripcion
    )

def expectativa_combinacion_columnas_unicas(columnas, name=None):
    """
    Verifica que la combinación de valores en varias columnas sea única en todas las filas.
    Parámetros:
        columnas: Lista de nombres de columnas a combinar.
        name: Nombre de la expectativa (opcional).
        Retorna:
            Expectation de tipo ExpectCompoundColumnsToBeUnique.
    """

    name = name or "Combinación única de columnas"
    descripcion = f"Verifica que la combinación de columnas {', '.join(columnas)} sea única en todas las filas."
    return ge.expectations.ExpectCompoundColumnsToBeUnique(
        column_list=columnas,
        meta={
            "name": name,
            "description": descripcion
        },
        description=descripcion
    )


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

def expectativa_valores_distintos_por_referencia(columna, referencia, name=None):
    """
    Verifica que los valores de una columna no estén en una lista de referencia.
    Parámetros:
        columna: Nombre de la columna a validar.
        referencia: DataFrame de referencia con los valores válidos.
        name: Nombre de la expectativa (opcional).
    Retorna:
            Expectation de tipo ExpectColumnValuesToNotBeInSet.
    """

    name = name or "Expectativa Valores Distintos por Referencia"
    descripcion = f"Verifica que los valores de la columna `{columna}` no estén en la lista de referencia."
    ref_cod = referencia[columna].dropna().unique().tolist()
    return ge.expectations.ExpectColumnValuesToNotBeInSet(
        column=columna,
        value_set=ref_cod,
        meta={"name": name, "description": descripcion},
        description=descripcion
    )

def expectativa_cantidad_filas_entre(min_valor, max_valor, name=None):
    """
    Verifica que la cantidad de filas en el DataFrame esté entre dos valores específicos.
    Parámetros:
        min_valor: Valor mínimo de filas esperado.
        max_valor: Valor máximo de filas esperado.
        name: Nombre de la expectativa (opcional).
    Retorna:
            Expectation de tipo ExpectTableRowCountToBeBetween.
    """
    name = name or "Expectativa de Cantidad de Filas"
    descripcion = f"Verifica que la cantidad de filas esté entre {min_valor} y {max_valor}."
    return ge.expectations.ExpectTableRowCountToBeBetween(
        min_value=min_valor,
        max_value=max_valor,
        meta={"name": name, "description": descripcion},
        description=descripcion
    )

def expectativa_valores_no_vacios(columna, name=None):
    """
    Verifica que los valores de una columna no sean cadenas vacías.
    Parámetros:
        columna: Nombre de la columna a validar.
        name: Nombre de la expectativa (opcional).
    Retorna:
            Expectation de tipo ExpectColumnValuesToNotMatchRegex.
    """

    name = name or "Expectativa Valores No Vacíos"
    descripcion = f"Verifica que los valores de `{columna}` no sean cadenas vacías."
    return ge.expectations.ExpectColumnValuesToNotMatchRegex(
        column=columna,
        regex="^$",
        meta={"name": name, "description": descripcion},
        description=descripcion
    )

def expectativa_con_condicional_y_patron(condicion, columna, patron, name=None):
    """
    Verifica que los valores de una columna cumplan una condición y coincidan con un patrón
    si se cumple la condición.
    Parámetros:
        condicion: Condición a evaluar en formato SQL (ej: 'col("ciudad")
        columna: Nombre de la columna a validar.
        patron: Patrón regex a validar.
        name: Nombre de la expectativa (opcional).
    Retorna:
        Expectation de tipo ExpectColumnValuesToMatchRegex con condición.
    """
    name = name or "Expectativa con Condicional y Patrón"
    descripcion = f"Verifica que los valores de `{columna}` cumplan la condición `{condicion}` y coincidan con el patrón `{patron}`."
    return ge.expectations.ExpectColumnValuesToMatchRegex(
        column=columna,
        regex=patron,
        row_condition=condicion,
        condition_parser="great_expectations",
        meta={"name": name, "description": descripcion},
        description=descripcion
    )

def expectativa_condicional_tabla_referencia(condicion, columna, referencia, name=None):
    """
    Verifica que los valores de una columna estén en una lista de referencia
    si se cumple una condición.
    Parámetros:
        condicion: Condición a evaluar en formato SQL (ej: 'col("ciudad")
        columna: Nombre de la columna a validar.
        referencia: DataFrame de referencia con los valores válidos.
        name: Nombre de la expectativa (opcional).
    Retorna:
        Expectation de tipo ExpectColumnValuesToBeInSet con condición.
    """
    name = name or "Expectativa Condicional con Tabla de Referencia"
    descripcion = f"Verifica que los valores de `{columna}` estén en la lista de referencia si se cumple la condición `{condicion}`."
    ref_cod = referencia[columna].dropna().unique().tolist()
    return ge.expectations.ExpectColumnValuesToBeInSet(
        column=columna,
        value_set=ref_cod,
        row_condition=condicion,
        condition_parser="great_expectations",
        meta={"name": name, "description": descripcion},
        description=descripcion
    )

def expectativa_condicional_valores_en_lista(condicion, columna, lista, name=None):
    """
    Verifica que los valores de una columna estén en una lista de valores
    si se cumple una condición.
    Parámetros:
        condicion: Condición a evaluar en formato SQL (ej: 'col("ciudad") == "Ciudad8"').
        columna: Nombre de la columna a validar.
        lista: Lista de valores válidos.
        name: Nombre de la expectativa (opcional).
    Retorna:
        Expectation de tipo ExpectColumnValuesToBeInSet con condición.
    """

    name = name or "Expectativa Condicional con Valores en Lista"
    descripcion = f"Verifica que los valores de `{columna}` estén en la lista de valores si se cumple la condición `{condicion}`."
    return ge.expectations.ExpectColumnValuesToBeInSet(
        column=columna,
        value_set=lista,
        row_condition=condicion,
        condition_parser="great_expectations",
        meta={"name": name, "description": descripcion},
        description=descripcion
    )


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

def expectativa_primera_palabra_en_referencia_con_condicion(condicion, columna, referencia, name=None):
    """
    Verifica que la primera palabra de los valores de una columna esté en una lista de referencia
    si se cumple una condición.
    Parámetros:
        condicion: Condición a evaluar en formato SQL (ej: 'col("ciudad") == "Ciudad8"').
        columna: Nombre de la columna a validar.
        referencia: DataFrame de referencia con los valores válidos.
        name: Nombre de la expectativa (opcional).
    Retorna:
        Expectation de tipo ExpectColumnValuesToMatchRegex con condición.
    """
    name = name or "Expectativa Primera Palabra en Referencia con Condición"
    ref = referencia[columna].dropna().unique().tolist()
    palabras_validas = list({str(valor).split()[0] for valor in ref})
    patron_regex = f"^({'|'.join(palabras_validas)})"

    descripcion = (
        f"Si se cumple `{condicion}`, entonces la primera palabra de `{columna}` debe estar en "
        f"la lista de palabras válidas: {', '.join(palabras_validas)}."
    )

    return ge.expectations.ExpectColumnValuesToMatchRegex(
        column=columna,
        regex=patron_regex,
        row_condition=condicion,
        condition_parser="great_expectations",
        meta={"name": name, "description": descripcion},
        description=descripcion
    )

def expectativa_valores_no_permitidos(columna, valores, condicion=None, name=None):
    """
    Verifica que los valores de una columna no estén en una lista de valores no permitidos,
    solo si se cumple la condición SQL (opcional).

    Parámetros:
        columna: Nombre de la columna a validar.
        valores: Lista de valores no permitidos.
        condicion: Condición SQL para filtrar filas (opcional).
        name: Nombre de la expectativa (opcional).

    Retorna:
        Expectation de tipo ExpectColumnValuesToNotBeInSet con condición.
    """
    name = name or "Expectativa Valores No Permitidos Condicional"
    descripcion = f"Verifica que los valores de `{columna}` no estén en la lista de valores no permitidos."
    if condicion:
        descripcion += f" Se aplica solo si se cumple la condición: {condicion}"

    return ge.expectations.ExpectColumnValuesToNotBeInSet(
        column=columna,
        value_set=valores,
        row_condition=condicion,
        condition_parser="great_expectations",
        meta={"name": name, "description": descripcion},
        description=descripcion
    )

def expectativa_valores_nulos_con_condicion(condicion, columna, name=None):
    """
    Verifica que los valores de una columna sean nulos si se cumple una condición.
    Parámetros:
        condicion: Condición a evaluar en formato SQL (ej: 'col("ciudad") == "Ciudad8"').
        columna: Nombre de la columna a validar.
        name: Nombre de la expectativa (opcional).
    Retorna:
        Expectation de tipo ExpectColumnValuesToBeNull con condición.
    """
    
    name = name or "Expectativa Valores Nulos con Condición"
    descripcion = f"Verifica que los valores de `{columna}` sean nulos si se cumple la condición `{condicion}`."
    
    return ge.expectations.ExpectColumnValuesToBeNull(
        column=columna,
        row_condition=condicion,
        condition_parser="great_expectations",
        meta={"name": name, "description": descripcion},
        description=descripcion
    )