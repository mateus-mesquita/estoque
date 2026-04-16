# importações
import pandas as pd
from prefect import task, get_run_logger

# Criando tafera de junção de tabelas (IQVIA MERCADO E TABELA DE PRODUTOS)
@task(name = "realizando Junção de tabelas")
def juntar_tabelas(dados1:pd.DataFrame, dados2:pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    try:
        df = pd.merge(dados1,dados2,how="right",on='EAN')
        return df
    except Exception as e:
        logger.error(f"Erro ao realizar junção de tabelas: {e}")
        raise e
