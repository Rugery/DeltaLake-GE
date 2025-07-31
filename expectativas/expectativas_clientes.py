import pandas as pd
import great_expectations as ge

from expectativas.expectativas_comunes import (
   expectativa_cantidad_filas_entre,
   expectativa_columnas_esperadas,
   expectativa_combinacion_columnas_unicas,
   expectativa_con_condicional_y_patron,
   expectativa_condicional_tabla_referencia,
   expectativa_condicional_valores_en_lista,
   expectativa_primera_palabra_en_referencia_con_condicion,
   expectativa_valor_unico,
   expectativa_valores_con_patron,
   expectativa_valores_longitud_entre,
   expectativa_valores_no_nulos,
   expectativa_valores_no_permitidos,
   expectativa_valores_no_vacios,
   expectativa_valores_nulos_con_condicion,
   expectativa_valores_por_referencia,
   expectativa_valores_por_referencia_caracteres,
   expectativa_valores_por_referencia_caracteres_con_condicion,
   expectativa_valores_tabla_equivalencia,
)

def suite_Clientes():
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
        FROM {{batch}} 
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
        # expectativa_valores_por_referencia(
        # columna="ERNAM", 
        # referencia=LR001, 
        # ),
        # expectativa_valores_por_referencia(
        # columna="cod_municipio", 
        # referencia=ref_Municipio, 
        # ),
        #---------------------------------------------------------
        # expectativa_condicional_tabla_referencia(
        # condicion='col("BEGRU")=="PSA" | col("BEGRU")=="VTC"',
        # columna="ERNAM",
        # referencia=LR001,
        # )
        #---------------------------------------------------------
        # expectativa_valores_no_nulos(
        # columna="STCD1",
        # )
        #---------------------------------------------------------
        # expectativa_valores_con_patron(
        # columna="STCD1",
        # patron=r"^\d+$",
        # ),
        #---------------------------------------------------------
        # expectativa_valores_no_nulos(
        # columna="ADRNR",
        # ),
        #---------------------------------------------------------
        # expectativa_valores_con_patron(
        # columna="ADRNR",
        # patron=r"^[^\s][A-Za-z0-9]{7,}$",
        # ),
        #---------------------------------------------------------
        # expectativa_primera_palabra_en_referencia_con_condicion(
        # condicion='col("LAND1")=="CO"',
        # columna="ADRNR",
        # referencia=LR006,
        # ),
        #---------------------------------------------------------
        # expectativa_valores_no_nulos(
        # columna="ORT01",
        # ),
        #---------------------------------------------------------
        # expectativa_valores_por_referencia(
        # columna="ORT01",
        # referencia=LR003,
        # )
        #---------------------------------------------------------
        # expectativa_valores_no_nulos(
        # columna="TELF1",
        # ),
        #---------------------------------------------------------
        # expectativa_con_condicional_y_patron(
        # condicion='col("LAND1")=="CO"',
        # columna="TELF1",
        # patron=r"^(60\d{8}|3\d{9})$",
        # ),
        #---------------------------------------------------------
        # expectativa_valores_por_referencia_caracteres_con_condicion(
        # condicion='col("LAND1")=="CO"',
        # columna="REGIO",
        # referencia=LR004,
        # caracteres=3,
        # )
        #---------------------------------------------------------
        # expectativa_condicional_valores_en_lista(
        # condicion='col("TELF1")=="9999999999"',
        # columna="KTOKD",
        # lista=["ZCLX","ZCON","ZDES","ZDUM","ZEVE","ZOBR","ZPVT","ZTRA","ZVEN"],
        # )
        #---------------------------------------------------------
        # expectativa_valores_no_permitidos(
        # columna="TELF1",
        # valores=["0000000000"],
        # )
        #---------------------------------------------------------
        # expectativa_valores_no_nulos(
        # columna="TELF2",
        # )
        #---------------------------------------------------------
        # expectativa_con_condicional_y_patron(
        # condicion='col("LAND1")=="CO"',
        # columna="TELF2",
        # patron=r"^(60\d{8}|3\d{9})$",
        # )
        #---------------------------------------------------------
        # expectativa_condicional_valores_en_lista(
        # condicion='col("LAND1")=="CO" & col("TELF2")=="0000000000"',
        # columna="KTOKD",
        # lista=["ZCLX","ZCON", "ZDES", "ZDUM", "ZEVE", "ZOBR", "ZPVT", "ZTRA", "ZVEN"],
        # )
        #---------------------------------------------------------
        # expectativa_valores_no_nulos(
        # columna="SMTP_ADDR",
        # )
        #---------------------------------------------------------
        # expectativa_valores_con_patron(
        # columna="SMTP_ADDR",
        # patron=r"^(?!\d)[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{3,}$",
        # ),
        #---------------------------------------------------------
        # expectativa_condicional_valores_en_lista(
        # condicion=''col("LAND1") == "CO" & ('
        # 'col("KTOKD") == "ZCLX" | '
        # 'col("KTOKD") == "ZCON" | '
        # 'col("KTOKD") == "ZDES" | '
        # 'col("KTOKD") == "ZDUM" | '
        # 'col("KTOKD") == "ZEVE" | '
        # 'col("KTOKD") == "ZOBR" | '
        # 'col("KTOKD") == "ZPVT" | '
        # 'col("KTOKD") == "ZTRA" | '
        # 'col("KTOKD") == "ZVEN"'
        # ')',
        # columna="SMTP_ADDR",
        # lista=["NA@NA.COM"],
        # )
        #---------------------------------------------------------
        # expectativa_condicional_valores_en_lista(
        # condicion=''col("LAND1") != "CO" & ('
        # 'col("KTOKD") == "ZCLX" | '
        # 'col("KTOKD") == "ZCON" | '
        # 'col("KTOKD") == "ZDES" | '
        # 'col("KTOKD") == "ZDUM" | '
        # 'col("KTOKD") == "ZEVE" | '
        # 'col("KTOKD") == "ZOBR" | '
        # 'col("KTOKD") == "ZPVT" | '
        # 'col("KTOKD") == "ZTRA" | '
        # 'col("KTOKD") == "ZVEN"'
        # ')',
        # columna="SMTP_ADDR",
        # lista=["NA@X.COM"],
        # )
        #---------------------------------------------------------
        # expectativa_con_condicional_y_patron(
        # condicion='col("FITYP")== "PN"',
        # columna="NAME3",
        # patron=r"^[A-Z]+(,[A-Z]+)*$",
        # ),
        #---------------------------------------------------------
        # expectativa_valores_nulos_con_condicion(
        # condicion='col("FITYP") == "PJ"',
        # columna="NAME3",
        # ),
        #---------------------------------------------------------
        # expectativa_valores_nulos_con_condicion(
        # condicion='col("FITYP") == "PJ"',
        # columna="NAME4",
        # ),
        #---------------------------------------------------------
        # expectativa_con_condicional_y_patron(
        # condicion='col("FITYP")== "PN"',
        # columna="NAME4",
        # patron=r"^[A-Z]+(,[A-Z]+)*$",
        # ),
        #---------------------------------------------------------
        # expectativa_valores_no_nulos(
        # columna="NAME1",
        # )
        #---------------------------------------------------------
        # expectativa_valores_con_patron(
        # columna="NAME1",
        # patron=r"^[A-Z]+( [A-Z]+)*$",
        # )
        #---------------------------------------------------------
        # expectativa_apellido_nombre_condicional(
        # NAME1="NAME1",
        # NAME3="NAME3",
        # NAME4="NAME4",
        # condicion_sql='FITYP = "PN"'
        # )
        #---------------------------------------------------------
        # expectativa_valores_no_nulos(
        # columna="SORTL",
        # )
        #---------------------------------------------------------
        # expectativa_con_condicional_y_patron(
        # condicion='col("STCDT")=="N" & col("LAND1")=="CO" & col("FITYP")=="PJ"',
        # columna="SORTL",
        # patron=r"^.{9}$"
        # )
        #---------------------------------------------------------
        # expectativa_valores_no_nulos(
        # columna="PSTLZ",
        # )
        #---------------------------------------------------------
        # expectativa_con_condicional_y_patron(
        # condicion='col("LAND1")=="CO"',
        # columna="PSTLZ",
        # patron=r"^\d{5}$",
        # )
        #---------------------------------------------------------
        # expectativa_valores_tabla_equivalencia(
        # columnas=["PSTLZ","ORT01"],
        # tabla_equivalencia=LR005,
        # condicion='col("LAND1")=="CO"',
        # )
        #---------------------------------------------------------
        # expectativa_condicional_valores_en_lista(
        # condicion='col("KTOKD")=="ZVTC"',
        # columna="BEGRUS",
        # lista=["ZVT"],
        # )
        #---------------------------------------------------------
        # expectativa_condicional_valores_en_lista(
        # condicion='col("KTOKD")=="ZNAL"',
        # columna="BEGRUS",
        # lista=["ZNAL","PSA"],
        # )
        #---------------------------------------------------------
        # expectativa_condicional_valores_en_lista(
        # condicion='col("KTOKD")=="ZEXT"',
        # columna="STKZN",
        # lista=["X"],
        # )
        #---------------------------------------------------------
        # expectativa_valores_no_permitidos(
        # columna="WAERS",
        # valores=["COP"],
        # condicion='col("KTOKD)=="ZEXT"',
        # )
        #---------------------------------------------------------
        # expectativa_condicional_valores_en_lista(
        # condicion='col("KTOKD")=="Z011"',
        # columna="XVERR",
        # lista=["X"],
        # )
        #---------------------------------------------------------
        # expectativa_valores_no_nulos(
        # columna="LPRIO",
        # )
        #---------------------------------------------------------
        # expectativa_valores_no_vacios(
        # columna="LPRIO",
        # )
        #---------------------------------------------------------
        # expectativa_valores_no_nulos(
        # columna="KVGR5",
        # )
        #---------------------------------------------------------
        # expectativa_valores_no_vacios(
        # columna="KVGR5",
        # )
        #---------------------------------------------------------
        # expectativa_valores_no_nulos(
        # columna="INCO1",
        # )
        #---------------------------------------------------------
        # expectativa_condicional_valores_en_lista(
        # condicion='col("KTOKD")=="ZNAL"',
        # columna="INCO1",
        # lista=["UN"],
        # )
        #---------------------------------------------------------
        # expectativa_valores_no_permitidos(
        # columna="KZAZU",
        # valores=["X"],
        # )
        #---------------------------------------------------------
        # expectativa_condicional_valores_en_lista(
        # condicion='col("KTOKD")=="ZNAL"',
        # columna="VSBED",
        # valores=["01"],
        # )
        #---------------------------------------------------------
        # expectativa_condicional_valores_en_lista(
        # condicion='col("KTOKD")=="ZEXT"',
        # columna="VSBED",
        # valores=["02"],
        # )
        #---------------------------------------------------------
        # expectativa_valores_no_vacios(
        # columna="CIIUCODE",
        # )
        #---------------------------------------------------------
    ]