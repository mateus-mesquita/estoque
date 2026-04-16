import pandas as pd
from prefect import task
from funcoes.funcoes_axiliares import calcular_soma_mercado

@task(name = "Tabela de Faturamento dos Últimos 6 meses(2025) - Mercado")
def faturamento2025ME(dados:pd.DataFrame) -> pd.DataFrame:
    dados['Faturamento (2025) - Mercado'] = dados.apply(calcular_soma_mercado, axis=1)
    return dados