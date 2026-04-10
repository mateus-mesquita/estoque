# Importações
import pandas as pd
from prefect import flow

# Importações das tasks
from tasks.tasks_metricas.media_tri import media_trimestral
from tasks.tasks_metricas.total import volume_bruto
from tasks.tasks_tabela.delete_cols import selecionar_cols
from fluxos.fluxo_final import PlanilhaFinal

# Fluxo de métricas
@flow(name = "Executando cálculos de métricas")
def TabelaFinal(caminho_estoque:str, caminho_prod:str, caminho_iqvia:str) -> pd.DataFrame:
    dados = PlanilhaFinal(caminho_estoque,caminho_prod,caminho_iqvia)
    dados = media_trimestral(dados)
    dados = volume_bruto(dados)

    # removendo colunas
    cols = [col for col in dados.columns if 'UNIDADES' in col]
    dados = selecionar_cols(dados, cols)
    return dados