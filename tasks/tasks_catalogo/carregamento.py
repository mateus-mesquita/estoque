# Importações
import pandas as pd
from halo import Halo
from prefect import task

# Task de carregamento de dados
@task(name = "Executando carregamento dos arquivos")
def carregar_dados(caminho:str) -> pd.DataFrame:
    with Halo(text="carregando arquivo", spinner='line'):
        try:
            dados = pd.read_excel(caminho)
            return dados
        except:
            dados = pd.read_csv(caminho, sep = ';')
            return dados