import pandas as pd
from prefect import task
from funcoes.funcoes_axiliares import calcular_soma_mercado
from funcoes.funcoes_axiliares import soma_ul_6_meses

@task(name = "Calculando Perfomance - Mercado")
def faturamento2025ME(dados:pd.DataFrame) -> pd.DataFrame:
    dados = soma_ul_6_meses(dados)

    # Calculando a performance por molécula
    dados['Performance (2025) - Mercado'] = dados.groupby('PRODUTO')['Soma ult.6 meses'].transform('sum')
    return dados