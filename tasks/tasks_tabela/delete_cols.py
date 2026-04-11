# importações
import pandas as pd
from prefect import task

# Criando tarefa de seleção de variáveis
@task(name = "Seleção de colunas")
def selecionar_cols(dados:pd.DataFrame, cols:list) -> pd.DataFrame:
    dados = dados.drop(columns=cols)
    return dados
