# Importações
import pandas as pd
from halo import Halo
from prefect import task, get_run_logger

# Task de carregamento de dados
@task(name = "Executando carregamento dos arquivos")
def carregar_dados(caminho:str) -> pd.DataFrame:
    logger = get_run_logger()
    with Halo(text="carregando arquivo", spinner='line'):
        try:
            logger.info(f"Carregando arquivo: {caminho} via leitor excel")
            dados = pd.read_excel(caminho)
            return dados
        except:
            logger.info(f"Carregando arquivo: {caminho} via leitor csv")
            dados = pd.read_csv(caminho, sep = ';')
            return dados
        except Exception as e:
            logger.error(f"Erro ao carregar arquivo: {caminho}")
            raise Exception(f"Erro ao carregar arquivo: {caminho}")