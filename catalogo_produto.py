# Importações
import pandas as pd
from prefect import task, flow

# Task de carregamento de dados
@task(name = "Executando carregamento dos arquivos")
def carregar_dados(caminho:str) -> pd.DataFrame:
    try:
        dados = pd.read_excel(caminho)
        return dados
    except:
        dados = pd.read_csv(caminho, sep = ';')
        return dados

# aplicando a criação da coluna de apresentção, da base catálogo de produto IQVIA
@task(name = "Criando a coluna de apresentação dos prodtos")
def criar_col_apresentcao(dados:pd.DataFrame) -> pd.DataFrame:
    # execução
    cols = ['CONCENTRACAO', 'VOLUME', 'QUANTIDADE_APRESENTACAO']

    dados[cols] = dados[cols].fillna('')
    dados['apresentação'] = dados[cols].astype(str).agg('-'.join, axis=1)
    return dados

# Construnção do fluxo de catálogo de produtos
@flow(name ="Executando base de catálogo de produtos")
def fluxo_base_ctlg_prod(caminho:str) -> pd.DataFrame:
    dados = carregar_dados(caminho)
    dados = criar_col_apresentcao(dados)
    return dados