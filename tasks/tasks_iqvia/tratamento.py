# importações
import pandas as pd
from halo import Halo
from prefect import task, get_run_logger

# aplicando tratamento dos dados
@task(name="Tratamento dos dados de Unidades")
def tratar_dados_iqvia(dados: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    try:
        with Halo(text="Tratando colunas", spinner='line'):
            cols = [c for c in dados.columns if 'UNIDADES' in c or 'RS' in c]

            for col in cols:
                dados[col] = (
                    dados[col]
                    .astype(str)
                    .str.replace(r'\s+', '', regex=True)     # remove espaços
                    .str.replace(',', '.', regex=False)       # remove pontos
                )

                dados[col] = pd.to_numeric(dados[col], errors='coerce')
            return dados
    except Exception as e:
        logger.error(f"Erro ao tratar dados: {e}")
        raise e