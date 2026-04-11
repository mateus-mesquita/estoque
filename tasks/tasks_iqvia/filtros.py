# importações 
import pandas as pd
from prefect import task, get_run_logger

# função para filtrar dados do mercado iqvia
@task(name = "Filtrando dados com base nas moléculas do estoque a qualificar")
def filtrar_moleculas(dados:pd.DataFrame, moleculas:list) -> pd.DataFrame:
    logger = get_run_logger()
    try:
        dados = dados.query("Molecula in @moleculas")
        logger.success("Filtragem de dados concluída com sucesso")
        return dados
    except Exception as e:
        logger.error(f"Erro ao filtrar dados: {e}")
        raise e

