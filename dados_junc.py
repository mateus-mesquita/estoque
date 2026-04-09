# importações
import pandas as pd
import tqdm
from catalogo_produto import fluxo_base_ctlg_prod
from iqvia import fluxo_iqvia
from prefect import task, flow

# Criando tarefa de seleção de variáveis
@task(name = "Seleção de colunas")
def selecionar_cols(dados:pd.DataFrame, cols:list) -> pd.DataFrame:
    dados = dados.drop(columns=cols, inplace = True)
    return dados

# Criando tafera de junção de tabelas (IQVIA MERCADO E TABELA DE PRODUTOS)
@task(name = "realizando Junção de tabelas")
def juntar_tabelas(dados1:pd.DataFrame, dados2:pd.DataFrame) -> pd.DataFrame:
    df = pd.merge(dados1,dados2,how="inner",on='EAN')
    return df

# Aplicando fluxo de tabela final
@flow(name = "Tabela final: variável de apresentação criada")
def fluxo_base_final(caminho_prod:str, caminho_iqvia:str) -> pd.DataFrame:
    dados_prod = fluxo_base_ctlg_prod(caminho_prod)
    dados_iqvia = fluxo_iqvia(caminho_iqvia)
    
    # Tratando dados 
    dados_prod = selecionar_cols(dados_prod,['FCC','NDF','CODIGO_CORPORACAO',
                                             'DESCRICAO_CORPORACAO','CODIGO_FABRICANTE',
                                             'BRAND','GMRS','SETOR_NEC','SEG_MKT_3',
                                             'CT1_CODE','CT1_DESC','CT1_DESC_LONGA',
                                             'CT3_CODE','CT3_DESC','CT3_DESC_LONGA',
                                             'CT4_CODE','CT4_DESC','CT4_DESC_LONGA',
                                             'NEC1_CODE','NEC1_DESC','NEC2_CODE',
                                             'NEC2_DESC','NEC3_CODE','NEC3_DESC',
                                             'NEC4_CODE','NEC4_DESC','APP1_CODE',
                                             'APP1_DESC','APP2_CODE','APP2_DESC',
                                             'APP3_DESC','APP3_CODE','DESCRICAO_LONGA',
                                             'SEG_MKT_3','DATA_LANCAMENTO_APRESENTACAO',
                                             'CT2_CODE', 'CT2_DESC','CT2_DESC_LONGA'
                                             ])
    
    RS_LIST = [col for col in tqdm.tqdm(dados_iqvia.columns) if 'RS' in col]
    RC_LIST = [col for col in tqdm.tqdm(dados_iqvia.columns) if 'TRIMOVEL' not in col and 'UNIDADES' in col]

    dados_iqvia = selecionar_cols(dados_iqvia,['FCC','PRODUCT_DESC','SEGMENTO_PROD',
                                               'SETOR_NEC_ABERTO','MOLECULA','ATC1',
                                               'ATC2','ATC3','ATC4','NEC1','NEC2',
                                               'NEC3','NEC4']+RS_LIST+RC_LIST)
    
    dados_final = juntar_tabelas(dados_prod, dados_iqvia)
    return dados_final