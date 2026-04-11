import pandas as pd
from prefect import task


# aplicando um tratamento para os valores das variáveis CONCENTRACAO, VOLUME e QUANTIDADE_APRESENTADA
@task(name = "melhoria de valores para a variável apresentação")
def melhorar_valores(dados: pd.DataFrame) -> pd.DataFrame:

    # === Tranformando QUANTIDADE_APRESENTACAO em string ===
    dados['QUANTIDADE_APRESENTACAO'] = dados['QUANTIDADE_APRESENTACAO'].astype(str)
    
    mask = dados['QUANTIDADE_APRESENTACAO'].notna()
    dados.loc[mask, 'QUANTIDADE_APRESENTACAO'] = (
        dados.loc[mask, 'QUANTIDADE_APRESENTACAO'].astype(str) + ' QTD_APR'
    )
    
    return dados

