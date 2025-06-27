import great_expectations as ge
import pandas as pd

from expectativas.expectativas_comunes import (
    expectativa_valor_unico, 
    expectativa_valores_con_patron, 
    expectativa_valores_por_referencia, 
    expectativa_valores_por_referencia_caracteres, 
    expectativa_valores_tabla_equivalencia
)

archivo_referencias = "./ref_data/clientes/referencias_clientes.xlsx"
archivo_equivalencias = "./ref_data/clientes/equivalencias_ciudad.xlsx"
archivo_referencias_comunes =  "./ref_data/comun/municipio.xlsx"

ref_Municipio = pd.read_excel(archivo_referencias_comunes, sheet_name="cod_municipio")
ref_Ciudad = pd.read_excel(archivo_referencias, sheet_name="ciudad")
ref_Email = pd.read_excel(archivo_referencias, sheet_name="email")
ref_ECiudad = pd.read_excel(archivo_equivalencias, sheet_name="municipio_ciudad")


def suite_Clientes():
    return [
        expectativa_valor_unico(columna="id_cliente"),
        expectativa_valores_por_referencia(columna="cod_municipio", referencia=ref_Municipio),
        expectativa_valores_por_referencia(columna="ciudad", referencia=ref_Ciudad),
        expectativa_valores_con_patron(columna="email", patron=r"^.*@.*\.com$"),
        expectativa_valores_por_referencia_caracteres(columna="email", referencia=ref_Email, caracteres=4),
        expectativa_valores_tabla_equivalencia(columnas=["email","ciudad"],tabla_equivalencia=ref_ECiudad)
    ]