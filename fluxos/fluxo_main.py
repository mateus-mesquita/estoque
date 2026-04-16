from fluxos.fluxo_base import fluxo_base_final
from funcoes.funcoes_axiliares import calcular_soma_ne, calcular_soma_mercado, soma_ul_6_meses
from prefect import task, flow
from tasks.tasks_tabela.delete_cols import selecionar_cols
import numpy as np
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