import pandas as pd
from prefect import task
from funcoes.funcoes_axiliares import soma_ul_6_meses


@task(name = "Calculando a performance por  Molécula a Nível mercado")
def performance_molecula_mercado(dados:pd.DataFrame) -> pd.DataFrame:
    dados = soma_ul_6_meses(dados)

    # Calculando a performance por molécula
    dados['Performance Molecula ULT.6 meses - Mercado'] = dados.groupby('MOLECULA')['Soma ult.6 meses'].transform('sum')
    return dados

@task(name = "Calculando a performance por  Molécula e Apresentação a Nível mercado")
def performance_molecula_apr_mercado(dados:pd.DataFrame) -> pd.DataFrame:
    dados = soma_ul_6_meses(dados)

    # Calculando a performance por molécula
    dados['Performance Molecula e Apresentação ULT.6 meses - Mercado'] = dados.groupby(['MOLECULA','apresentação'])['Soma ult.6 meses'].transform('sum')
    return dados