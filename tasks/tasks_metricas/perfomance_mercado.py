import pandas as pd
from prefect import task
from funcoes.funcoes_axiliares import soma_ul_6_meses

@task(name = "Calculando Perfomance - Mercado")
def faturamento2025ME(dados:pd.DataFrame) -> pd.DataFrame:
    dados = soma_ul_6_meses(dados)
    dados['Performance (2025) - Mercado'] = dados.groupby(['UF','PRODUTO','TIPO_FARMACIA'])['Soma ult.6 meses'].transform('sum')
    return dados