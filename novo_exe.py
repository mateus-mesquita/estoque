# importações
import pandas as pd
from prefect import task, flow, get_run_logger

# Criando task de leitura dos arquivos
@task(name = "Lendo arquivos")
def carregando_arquivos(caminho:str) -> pd.DataFrame:
    if 'csv' in caminho:
        return pd.read_csv(caminho, sep = ';')
    elif 'xlsx' in caminho:
        return pd.read_excel(caminho)

# aplicando tratamento dos dados do IQVIA
@task(name="Tratamento dos dados de Unidades")
def tratar_dados_iqvia(dados: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"Iniciando tratamento de {len(dados.columns)} colunas do IQVIA")
    try:
        cols = [c for c in dados.columns if 'UNIDADES' in c or 'RS' in c]

        for col in cols:
            dados[col] = (
                dados[col]
                .astype(str)
                .str.replace(r'\s+', '', regex=True)     
                .str.replace(',', '.', regex=False)       
            )

            dados[col] = pd.to_numeric(dados[col], errors='coerce')
        return dados
    except Exception as e:
        logger.error(f"Erro ao tratar dados: {e}")
        raise e


# Criando tafera de junção de tabelas (IQVIA MERCADO E TABELA DE PRODUTOS)
@task(name = "realizando Junção de tabelas")
def juntar_tabelas(dados1:pd.DataFrame, dados2:pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    try:
        logger.info(f"Realizando junção de tabelas via EAN (Right Join)")
        df = pd.merge(dados1,dados2,how="right",on='EAN')
        return df
    except Exception as e:
        logger.error(f"Erro ao realizar junção de tabelas: {e}")
        raise e

# Criando tarefa de seleção de variáveis
@task(name = "Seleção de colunas")
def selecionar_cols(dados:pd.DataFrame, cols:list) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"Removendo {len(cols)} colunas desnecessárias")
    dados = dados.drop(columns=cols)
    return dados

# Criando métrica de desempenho para o grupo nordeste, coluna de soma filtrada
@task(name="Coluna de Filtro Nordeste")
def filtro_nordeste(dados:pd.DataFrame) -> pd.DataFrame:
    dados['SOMA_N'] = dados['RS_PPP_YTDDEZ25'].where(dados['DISTRIBUIDOR'] == 'GRUPO_NORDESTE')
    dados['SOMA_N'].fillna(0, inplace=True)
    return dados

# Criando colunas com os resultados das métricas de desempenho
@task(name="Métricas de Desempenho")
def metricas_desempenho_prod(dados:pd.DataFrame, col:str, nome:str) -> pd.DataFrame:
    dados[nome] = dados.groupby(['UF','PRODUTO'])[col].transform('sum')
    return dados

@task(name="Métricas de Desempenho")
def metricas_desempenho_molecula(dados:pd.DataFrame, col:str, nome:str) -> pd.DataFrame:
    dados[nome] = dados.groupby(['UF','MOLECULA'],)[col].transform('sum')
    return dados

@task(name="Métricas de Desempenho")
def metricas_desempenho_apresentacao(dados:pd.DataFrame, col:str, nome:str) -> pd.DataFrame:
    dados[nome] = dados.groupby(['UF','MOLECULA','APRESENTACAO'],)[col].transform('sum')
    return dados

# aplicando tratamento em espoçamentos dos dados
@task(name="Tratamento em Espoçamentos")
def tratamento_espocamentos(dados:pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Iniciando tratamento de espoçamentos")
    try:
        dados['MOLECULA'] = dados['MOLECULA'].astype(str).str.strip().str.upper()
        return dados
    except Exception as e:
        logger.error(f"Erro ao tratar espoçamentos: {e}")
        raise e

@task(name = "Criando a coluna de apresentação dos prodtos")
def criar_col_apresentcao(dados:pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Criando coluna de apresentação combinando atributos")
    # execução
    cols = ['CONCENTRACAO', 'VOLUME', 'QUANTIDADE_APRESENTACAO']

    dados[cols] = dados[cols].fillna('')
    dados['APRESENTACAO'] = dados[cols].astype(str).agg(' '.join, axis=1)
    return dados


# Criando fluxo de execução
@flow(name = "fluxo de execução")
def fluxo_execucao():
    dados_produtos = carregando_arquivos("CATALOGO_DE_PRODUTOS.xlsx")
    dados_produtos = criar_col_apresentcao(dados_produtos)
    dados_iqvia = carregando_arquivos("12_IQVIADEZEMBRO2025CSV.csv")
    dados_iqvia = tratar_dados_iqvia(dados_iqvia)
    
    # selecção de colunas
    dados_produtos = selecionar_cols(dados_produtos,['FCC','NDF','CODIGO_CORPORACAO',
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
                                             'CT2_CODE', 'CT2_DESC','CT2_DESC_LONGA','AREA_FARMACIA',
                                             'CONCENTRACAO','VOLUME','QUANTIDADE_APRESENTACAO','MOLECULA',
                                             'PACK'
                                             ])
    RC_LIST = [col for col in dados_iqvia.columns if 'TRIMOVEL' in col]
    dados_iqvia = selecionar_cols(dados_iqvia,['FCC','SEGMENTO_PROD',
                                               'SETOR_NEC_ABERTO','ATC1',
                                               'ATC2','ATC3','ATC4','NEC1','NEC2',
                                               'NEC3','NEC4','PACK_DESC','LABORATORIO']+RC_LIST)
    
    dados_final = juntar_tabelas(dados_produtos, dados_iqvia)
    # removendo valores nulos
    dados_final.dropna(subset=['PRODUTO'], inplace=True)
    dados_final = dados_final[dados_final['MOLECULA'] != 'N/I']
    dados_final = tratamento_espocamentos(dados_final)
    dados_final = filtro_nordeste(dados_final)
    dados_final = metricas_desempenho_prod(dados_final, 'SOMA_N', 'perf_ne')
    dados_final = metricas_desempenho_prod(dados_final, 'RS_PPP_YTDDEZ25', 'perf_mercado')
    dados_final = metricas_desempenho_molecula(dados_final, 'SOMA_N', 'perf_ne_mol')
    dados_final = metricas_desempenho_molecula(dados_final, 'RS_PPP_YTDDEZ25', 'perf_mercado_mol')
    dados_final = metricas_desempenho_apresentacao(dados_final, 'SOMA_N', 'perf_ne_apres')
    dados_final = metricas_desempenho_apresentacao(dados_final, 'RS_PPP_YTDDEZ25', 'perf_mercado_apres')
    
    cols = [c for c in dados_final.columns if 'UNIDADES' in c or 'RS' in c and 'YTD' not in c]
    dados_final = selecionar_cols(dados_final,cols+['MEDICAMENTO','AREA_FARMACIA','ETICO_POPULAR'])
    return dados_final


teste = fluxo_execucao()
teste.to_excel("teste.xlsx", index=False)