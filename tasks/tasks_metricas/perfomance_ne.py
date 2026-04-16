import pandas as pd
from prefect import task
from funcoes.funcoes_axiliares import calcular_soma_ne

@task(name = "Tabela de Faturamento dos Últimos 6 meses(2025) - NE")
def faturamento2025NE(dados:pd.DataFrame) -> pd.DataFrame:
    dados['Faturamento (2025) - NE'] = dados.apply(calcular_soma_ne, axis=1)
    return dados

