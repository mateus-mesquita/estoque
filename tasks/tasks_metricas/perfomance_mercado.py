import pandas as pd
from prefect import task
import numpy as np
from funcoes.funcoes_axiliares import soma_ul_6_meses

@task(name = "Calculando Perfomance - Mercado")
def faturamento2025ME(dados:pd.DataFrame) -> pd.DataFrame:
    dados = soma_ul_6_meses(dados)
    
    # Criando coluna de perfomance
    filtro = (dados['UF'].isin(['CE','PI','MA']) & (dados['PRODUCT_DESC'].isin(list(dados['PRODUCT_DESC'].unique()))))
    dados['Performance (2025) - Mercado'] = np.where(filtro, dados['Soma ult.6 meses'], 0)
    return dados