# Importações
import pandas as pd
from halo import Halo
from prefect import flow

# Importações das tasks
from tasks.tasks_catalogo.carregamento import carregar_dados
from tasks.tasks_iqvia.tratamento import tratar_dados_iqvia

# Construnção do fluxo de IQVIA
@flow(name = "Executando fluxo IQVIA")
def fluxo_iqvia(caminho:str) -> pd.DataFrame:
    dados = carregar_dados(caminho)
    dados = tratar_dados_iqvia(dados)
    return dados