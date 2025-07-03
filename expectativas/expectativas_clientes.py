import great_expectations as ge
import pandas as pd

from expectativas.expectativas_comunes import (
    expectativa_cantidad_filas_entre,
    expectativa_columna_valores_entre,
    expectativa_columnas_esperadas,
    expectativa_combinacion_columnas_unicas,
    expectativa_no_valores_null_duplicados_vacios,
    expectativa_valor_unico, 
    expectativa_valores_con_patron,
    expectativa_valores_distintos_por_referencia,
    expectativa_valores_longitud_entre,
    expectativa_valores_longitud_igual,
    expectativa_valores_no_coinciden_con_patron,
    expectativa_valores_no_nullos,
    expectativa_valores_no_vacios, 
    expectativa_valores_por_referencia, 
    expectativa_valores_por_referencia_caracteres, 
    expectativa_valores_tabla_equivalencia,
    expectativa_valores_tipo
)

archivo_referencias = "./ref_datos/clientes/referencias_clientes.xlsx"
archivo_equivalencias = "./ref_datos/clientes/equivalencias_ciudad.xlsx"
archivo_referencias_comunes =  "./ref_datos/comun/municipio.xlsx"

ref_Municipio = pd.read_excel(archivo_referencias_comunes, sheet_name="cod_municipio")
ref_Ciudad = pd.read_excel(archivo_referencias, sheet_name="ciudad")
ref_Email = pd.read_excel(archivo_referencias, sheet_name="email")
ref_ECiudad = pd.read_excel(archivo_equivalencias, sheet_name="municipio_ciudad")
ref_CiudadDistina = pd.read_excel(archivo_referencias, sheet_name="ciudad_distinta")

def suite_Clientes():
    return [
        expectativa_valores_por_referencia_caracteres(columna="email", referencia=ref_Email, caracteres=4),
        expectativa_valor_unico(columna="id_cliente"),
        expectativa_valores_por_referencia(columna="cod_municipio", referencia=ref_Municipio),
        expectativa_valores_por_referencia(columna="ciudad", referencia=ref_Ciudad),
        expectativa_valores_con_patron(columna="email", patron=r"^.*@.*\.com$"),
        expectativa_valores_tabla_equivalencia(columnas=["email","ciudad"],tabla_equivalencia=ref_ECiudad),
        expectativa_valores_no_nullos(columna="id_cliente"),
        expectativa_valores_tipo(columna="ciudad", tipo="StringType"),
        expectativa_valores_longitud_entre(columna="email",min_valor=5, max_valor=50),
        expectativa_valores_longitud_igual(columna="ciudad", longitud=8),
        expectativa_columna_valores_entre(columna="cod_municipio", min_valor=1, max_valor=20),
        expectativa_columnas_esperadas(columnas=["id_cliente", "nombre", "email", "cod_municipio", "ciudad",]),
        expectativa_combinacion_columnas_unicas(columnas=["id_cliente", "nombre"]),
        expectativa_valores_no_coinciden_con_patron(columna="cod_municipio", patron=r".*\s+.*"),
        expectativa_valores_distintos_por_referencia(columna="ciudad", referencia=ref_CiudadDistina),
        expectativa_cantidad_filas_entre(min_valor=1,max_valor=50),
        #expectativa_no_valores_null_duplicados_vacios(columna="id_cliente"),
        expectativa_valores_no_vacios(columna="email")
    ]