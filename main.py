from fluxos.fluxo_base import fluxo_base_final
from prefect import task, flow
from tasks.tasks_tabela.delete_cols import selecionar_cols
import numpy as np
import pandas as pd


@task(name = "Calculando a performance por  Molécula a Nível mercado")
def performance_molecula_mercado(dados:pd.DataFrame) -> pd.DataFrame:
    dados = soma_ul_6_meses(dados)

    # Calculando a performance por molécula
    dados['Performance Molecula ULT.6 meses - Mercado'] = dados.groupby('MOLECULA')['Soma ult.6 meses'].transform('sum')
    return dados

@task(name = "Calculando a performance por  Molécula e Apresentação a Nível mercado")
def performance_molecula_apr_mercado(dados:pd.DataFrame) -> pd.DataFrame:
    dados = soma_ul_6_meses(dados)

    # Calculando a performance por molécula
    dados['Performance Molecula e Apresentação ULT.6 meses - Mercado'] = dados.groupby(['MOLECULA','apresentação'])['Soma ult.6 meses'].transform('sum')
    return dados

# Definindo task 
@task(name = "Tabela de Faturamento dos Últimos 6 meses(2025) - NE")
def faturamento2025NE(dados:pd.DataFrame) -> pd.DataFrame:
    dados['Faturamento (2025) - NE'] = dados.apply(calcular_soma_ne, axis=1)
    return dados

@task(name = "Tabela de Faturamento dos Últimos 6 meses(2025) - Mercado")
def faturamento2025ME(dados:pd.DataFrame) -> pd.DataFrame:
    dados = soma_ul_6_meses(dados)

    # Calculando a performance por molécula
    dados['Faturamento (2025) - Mercado'] = dados.groupby('PRODUCT_DESC')['Soma ult.6 meses'].transform('sum')
    return dados


## aplicando flow
@flow(name = "teste")
def flowteste():
    dados = fluxo_base_final(
    "CATALOGO_DE_PRODUTOS.xlsx",
    "12_IQVIADEZEMBRO2025CSV.csv"
)   
    dados = faturamento2025NE(dados)
    dados = faturamento2025ME(dados)
    dados = performance_molecula_mercado(dados)
    dados = performance_molecula_apr_mercado(dados)

    # deletando colunas
    cols = [c for c in dados.columns if 'UNIDADES' in c or 'RS' in c]
    dados = selecionar_cols(dados,cols+['MEDICAMENTO','AREA_FARMACIA','ETICO_POPULAR'])
    return dados

print(flowteste().to_excel("teste2.xlsx"))