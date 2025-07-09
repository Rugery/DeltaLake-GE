import pandas as pd
import great_expectations as ge

from expectativas.expectativas_comunes import (
    expectativa_cantidad_filas_entre,
    expectativa_columna_valores_entre,
    expectativa_columnas_esperadas,
    expectativa_combinacion_columnas_unicas,
    expectativa_con_condicional_y_patron,
    expectativa_condicional_tabla_referencia,
    expectativa_condicional_valores_en_lista,
    expectativa_primera_palabra_en_referencia_con_condicion,
    expectativa_valor_unico,
    expectativa_con_condicional_y_patron,
    expectativa_valores_con_patron,
    expectativa_valores_distintos_por_referencia,
    expectativa_valores_longitud_entre,
    expectativa_valores_longitud_igual,
    expectativa_valores_no_nulos,
    expectativa_valores_no_permitidos,
    expectativa_valores_no_vacios,
    expectativa_valores_por_referencia,
    expectativa_valores_por_referencia_caracteres,
    expectativa_valores_por_referencia_caracteres_con_condicion,
    expectativa_valores_tabla_equivalencia,
    expectativa_valores_tipo,
)

archivo_referencias = "./ref_datos/clientes/referencias_clientes.xlsx"
archivo_equivalencias = "./ref_datos/clientes/equivalencias_ciudad.xlsx"
archivo_referencias_comunes =  "./ref_datos/comun/municipio.xlsx"

ref_Municipio = pd.read_excel(archivo_referencias_comunes, sheet_name="cod_municipio")
ref_nombre = pd.read_excel(archivo_referencias, sheet_name="nombre")
ref_Ciudad = pd.read_excel(archivo_referencias, sheet_name="ciudad")
ref_Email = pd.read_excel(archivo_referencias, sheet_name="email")
ref_ECiudad = pd.read_excel(archivo_equivalencias, sheet_name="municipio_ciudad")
ref_CiudadDistina = pd.read_excel(archivo_referencias, sheet_name="ciudad_distinta")

def suite_Clientes():
    def expectativa_apellido_nombre_condicional(NAME1, NAME3, NAME4, condicion_sql=None):
        """
        Valida que name1_col tenga la estructura 'apellido nombre' concatenando name4_col y name3_col,
        solo si se cumple la condición SQL pasada en condicion_sql.

        Parámetros:
            name1_col: columna con apellido nombre concatenado (ej. "NAME1")
            name3_col: columna con nombre(s) (ej. "NAME3")
            name4_col: columna con apellido(s) (ej. "NAME4")
            condicion_sql: condición SQL como string, ej. "ciudad = 'Ciudad8'"

        Retorna:
            Expectation personalizada (UnexpectedRowsExpectation)
        """

        # Si se proporciona condición, agregarla a la cláusula WHERE junto con la validación
        if condicion_sql:
            where_clause = f"WHERE {condicion_sql} AND {NAME1} != CONCAT({NAME4}, ' ', {NAME3})"
        else:
            # Validar en todas las filas si no hay condición
            where_clause = f"WHERE {NAME1} != CONCAT({NAME4}, ' ', {NAME3})"

        sql = f"""
        SELECT *
        FROM {{batch}} AS t
        {where_clause}
        """

        return ge.expectations.UnexpectedRowsExpectation(
            unexpected_rows_query=sql,
            meta={
                "name": "Validar estructura apellido nombre condicional",
                "description": f"Verifica que {NAME1} sea la concatenación de {NAME4} + espacio + {NAME3}" +
                            (f" cuando se cumple: {condicion_sql}" if condicion_sql else "")
            },
            description=f"Validar que {NAME1} tenga la estructura 'apellido nombre'" +
                        (f" si {condicion_sql}" if condicion_sql else "")
        )

    return [
        expectativa_valores_por_referencia_caracteres(columna="email", referencia=ref_Email, caracteres=4),
        expectativa_valor_unico(columna="id_cliente"),
        expectativa_valores_por_referencia(columna="cod_municipio", referencia=ref_Municipio),
        expectativa_valores_por_referencia(columna="ciudad", referencia=ref_Ciudad),
        expectativa_valores_con_patron(columna="email", patron=r"^.*@.*\.com$"),
        expectativa_valores_tabla_equivalencia(condicion="cod_municipio='8'",columnas=["email","ciudad"],tabla_equivalencia=ref_ECiudad),
        expectativa_valores_no_nulos(columna="id_cliente"),
        expectativa_valores_tipo(columna="ciudad", tipo="StringType"),
        expectativa_valores_longitud_entre(columna="email",min_valor=5, max_valor=50),
        expectativa_valores_longitud_igual(columna="ciudad", longitud=8),
        expectativa_columna_valores_entre(columna="cod_municipio", min_valor=1, max_valor=20),
        expectativa_columnas_esperadas(columnas=["id_cliente", "nombre", "email", "cod_municipio", "ciudad",]),
        expectativa_combinacion_columnas_unicas(columnas=["id_cliente", "nombre"]),
        expectativa_valores_distintos_por_referencia(columna="ciudad", referencia=ref_CiudadDistina),
        expectativa_cantidad_filas_entre(min_valor=1,max_valor=50),
        expectativa_valores_no_vacios(columna="email"),
        expectativa_con_condicional_y_patron(condicion='col("ciudad")== "Ciudad8"', columna="id_cliente",patron=r"^(3|7|4)"),
        expectativa_condicional_tabla_referencia(condicion='col("ciudad")=="Ciudad8" or col("ciudad")=="Ciudad14" or col("ciudad")=="Ciudad9"',columna="cod_municipio", referencia=ref_Municipio),
        expectativa_condicional_valores_en_lista(condicion='col("ciudad")=="Ciudad8"',columna="cod_municipio", lista=[10,9,14]),
        expectativa_primera_palabra_en_referencia_con_condicion(condicion='col("ciudad")=="Ciudad8" or col("ciudad")=="Ciudad11"', columna="nombre", referencia=ref_nombre),
        expectativa_valores_por_referencia_caracteres_con_condicion(condicion='col("ciudad")=="Ciudad8"', columna="email", referencia=ref_Email, caracteres=4),
        expectativa_valores_no_permitidos(columna="ciudad", valores=["Ciudad8"]),
        expectativa_condicional_valores_en_lista(condicion='col("ciudad")=="Ciudad8" and col("id_cliente")=="37"', columna="cod_municipio", lista=[9, 5, 14]),
        expectativa_condicional_valores_en_lista(condicion='col("ciudad")!= "Ciudad8" and col("id_cliente")=="37"', columna="cod_municipio", lista=[8]),
        expectativa_con_condicional_y_patron(condicion='',columna="cod_municipio", patron=r"^$"),
        
    ]