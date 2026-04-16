import pandas as pd

# deve-se replicar a função da molécula para produto individualmente

# aplicando criação de tabelas com métricas calculadas
def calcular_soma_ne(dados:pd.DataFrame) -> pd.Series:
    cols = [c for c in dados.columns if 'RS_PPP_2025' in c]
    mask = (dados['UF'].isin(['CE','PI','MA'])) & (dados['DISTRIBUIDOR'] == 'GRUPO_NORDESTE')
    return dados[cols].sum(axis=1) * mask
    
def calcular_soma_mercado(dados:pd.DataFrame) -> pd.Series:
    cols = [c for c in dados.columns if 'RS_PPP_2025' in c]
    mask = dados['UF'].isin(['CE','PI','MA'])
    return dados[cols].sum(axis=1) * mask

# Calculando o faturamento da molécula
def coletar_moleculas(dados:pd.DataFrame) -> pd.DataFrame:
    lista = list(dados['MOLECULA'].unique())
    return lista

# Definindo funções de colunas
def soma_ul_6_meses(dados:pd.DataFrame) -> pd.DataFrame:
    # Identifica as colunas de faturamento dinamicamente
    cols_faturamento = [c for c in dados.columns if 'RS_PPP_2025' in c]
    
    # Soma os valores tratando NaNs como 0, mas mantendo NaN se a linha toda for vazia
    dados['Soma ult.6 meses'] = dados[cols_faturamento].sum(axis=1, min_count=1)
    
    return dados
