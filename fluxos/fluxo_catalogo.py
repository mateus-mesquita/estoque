# Importações
import pandas as pd
from halo import Halo
from prefect import flow

# Importações das tasks
from tasks.tasks_catalogo.carregamento import carregar_dados
from tasks.tasks_catalogo.atualizar_valores import melhorar_valores
from tasks.tasks_catalogo.criacao_col import criar_col_apresentcao

# Construnção do fluxo de catálogo de produtos
@flow(name ="Executando base de catálogo de produtos")
def fluxo_base_ctlg_prod(caminho:str) -> pd.DataFrame:
    dados = carregar_dados(caminho)
    dados.dropna(subset=['EAN'], inplace = True)
    dados = criar_col_apresentcao(dados)
    dados = melhorar_valores(dados)
    return dados