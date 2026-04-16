from fluxos.fluxo_base import fluxo_base_final
from tasks.tasks_tabela.delete_cols import selecionar_cols
from tasks.tasks_metricas.perfomance_mercado import faturamento2025ME
from tasks.tasks_metricas.perfomance_ne import faturamento2025NE
from tasks.tasks_metricas.perfomance_molecula import performance_molecula_apr_mercado, performance_molecula_mercado
from prefect import flow
import pandas as pd

## aplicando flow
@flow(name = "Executando planilha com os resultados")
def PlanilhaResultados(caminho_prod:str, caminho_iqvia:str) -> pd.DataFrame:
    dados = fluxo_base_final(
    caminho_prod,
    caminho_iqvia
)   
    dados = faturamento2025NE(dados)
    dados = faturamento2025ME(dados)
    dados = performance_molecula_mercado(dados)
    dados = performance_molecula_apr_mercado(dados)

    # deletando colunas
    cols = [c for c in dados.columns if 'UNIDADES' in c or 'RS' in c]
    dados = selecionar_cols(dados,cols+['MEDICAMENTO','AREA_FARMACIA','ETICO_POPULAR'])
    return dados
