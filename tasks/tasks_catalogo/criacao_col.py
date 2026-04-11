import pandas as pd
from prefect import task

# aplicando a criação da coluna de apresentção, da base catálogo de produto IQVIA
@task(name = "Criando a coluna de apresentação dos prodtos")
def criar_col_apresentcao(dados:pd.DataFrame) -> pd.DataFrame:
    # execução
    cols = ['CONCENTRACAO', 'VOLUME', 'QUANTIDADE_APRESENTACAO']

    dados[cols] = dados[cols].fillna('')
    dados['apresentação'] = dados[cols].astype(str).agg(''.join, axis=1)
    return dados