import great_expectations as ge
import pandas as pd

from expectativas.expectativas_comunes import (
    expectativa_valor_unico,
    expectativa_valores_longitud_igual, 
    expectativa_valores_longitud_entre, 
    expectativa_valores_no_nullos, 
    expectativa_valores_por_referencia_caracteres, 
)

archivo_referencias = "./ref_datos/telefonos/referencias_telefonos.xlsx"
ref_Telefonos = pd.read_excel(archivo_referencias, sheet_name="telefono")


def suite_Telefonos():
    return [
        expectativa_valores_por_referencia_caracteres(columna="telefono", referencia=ref_Telefonos, caracteres=3),
        expectativa_valor_unico(columna="id"),
        expectativa_valores_no_nullos(columna="telefono"),
        expectativa_valores_longitud_entre(columna="telefono", min_valor=1, max_valor=10),
        expectativa_valores_longitud_igual(columna="telefono", longitud=10),
    ]