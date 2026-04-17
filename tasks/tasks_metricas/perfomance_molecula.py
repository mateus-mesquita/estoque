import pandas as pd
from prefect import task
import numpy as np
from funcoes.funcoes_axiliares import soma_ul_6_meses


@task(name = "Calculando a performance por  Molécula a Nível mercado")
def performance_molecula_mercado(dados:pd.DataFrame) -> pd.DataFrame:
    dados = soma_ul_6_meses(dados)
    
    # Criando coluna de perfomance
    filtro = ((dados['UF'].isin(['CE','PI','MA'])) & (dados['MOLECULA'].isin(list(dados['MOLECULA'].unique()))))
    dados['molecula_Performance (2025) - Mercado'] = np.where(filtro, dados['Soma ult.6 meses'], 0)
    return dados

@task(name = "Calculando a performance por  Molécula e Apresentação a Nível mercado")
def performance_molecula_apr_mercado(dados:pd.DataFrame) -> pd.DataFrame:
    dados = soma_ul_6_meses(dados)
    
    # Criando coluna de perfomance
    filtro = ((dados['UF'].isin(['CE','PI','MA'])) & (dados['MOLECULA'].isin(list(dados['MOLECULA'].unique())) & (dados['apresentação'].isin(list(dados['apresentação'].unique())))))
    dados['molecula_apr_Performance (2025) - Mercado'] = np.where(filtro, dados['Soma ult.6 meses'], 0)
    return dados