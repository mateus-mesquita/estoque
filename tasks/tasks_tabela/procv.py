# importações
import pandas as pd
from prefect import task

# Criando tafera de junção de tabelas (IQVIA MERCADO E TABELA DE PRODUTOS)
@task(name = "realizando Junção de tabelas")
def juntar_tabelas(dados1:pd.DataFrame, dados2:pd.DataFrame) -> pd.DataFrame:
    df = pd.merge(dados1,dados2,how="left",on='EAN')
    return df
