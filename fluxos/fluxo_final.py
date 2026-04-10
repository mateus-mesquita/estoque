# Importações
import pandas as pd
from prefect import flow

# Importações das tasks
from tasks.arquivos.carregamento import carregar_dados
from tasks.tasks_resultado.classificar import flag
from fluxos.fluxo_base import fluxo_base_final

# Fluxo de trabalho atualizado
@flow(name = "Execução dos dados finais")
def PlanilhaFinal(caminho_estoque:str, caminho_prod:str, caminho_iqvia:str) -> pd.DataFrame:
    dados_ = fluxo_base_final(caminho_prod, caminho_iqvia)
    dados_estoque = carregar_dados(caminho_estoque)

    dados_ = flag(dados_, dados_estoque)
    return dados_
