import pandas as pd

# deve-se replicar a função da molécula para produto individualmente

# aplicando criação de tabelas com métricas calculadas
def calcular_soma_ne(row):
    if row['UF'] in ('CE','PI','MA') and row['DISTRIBUIDOR'] == 'GRUPO_NORDESTE':
        return (row['RS_PPP_202501']+row['RS_PPP_202502']+row['RS_PPP_202503']+
                row['RS_PPP_202504']+row['RS_PPP_202504']+row['RS_PPP_202504']+
                row['RS_PPP_202506']+row['RS_PPP_202507']+row['RS_PPP_202508']+
                row['RS_PPP_202509']+row['RS_PPP_202510']+row['RS_PPP_202511']+
                row['RS_PPP_202512'])
    else:
        return 0
    
def calcular_soma_mercado(row):
    if row['UF'] in ('CE','PI','MA'):
        return (row['RS_PPP_202501']+row['RS_PPP_202502']+row['RS_PPP_202503']+
                row['RS_PPP_202504']+row['RS_PPP_202504']+row['RS_PPP_202504']+
                row['RS_PPP_202506']+row['RS_PPP_202507']+row['RS_PPP_202508']+
                row['RS_PPP_202509']+row['RS_PPP_202510']+row['RS_PPP_202511']+
                row['RS_PPP_202512'])
    else:
        return 0

# Calculando o faturamento da molécula
def coletar_moleculas(dados:pd.DataFrame) -> pd.DataFrame:
    lista = list(dados['MOLECULA'].unique())
    return lista

# Definindo funções de colunas
def soma_ul_6_meses(dados:pd.DataFrame) -> pd.DataFrame:
    dados['Soma ult.6 meses'] = (dados['RS_PPP_202501']+dados['RS_PPP_202502']+dados['RS_PPP_202503']+
                dados['RS_PPP_202504']+dados['RS_PPP_202504']+dados['RS_PPP_202504']+
                dados['RS_PPP_202506']+dados['RS_PPP_202507']+dados['RS_PPP_202508']+
                dados['RS_PPP_202509']+dados['RS_PPP_202510']+dados['RS_PPP_202511']+
                dados['RS_PPP_202512'])
    
    return dados