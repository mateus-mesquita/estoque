# importações
import pandas as pd
from catalogo_produtos import carregar_dados
from prefect import task, flow 

# aplicando tratamento dos dados
@task(name="Tratamento dos dados de Unidades")
def tratar_dados_iqvia(dados: pd.DataFrame) -> pd.DataFrame:

    cols = [c for c in dados.columns if 'UNIDADES' in c]

    for col in cols:
        dados[col] = (
            dados[col]
            .astype(str)
            .str.replace(r'\s+', '', regex=True)     # remove espaços
            .str.replace('.', '', regex=False)       # remove pontos
            .str.replace(',', '', regex=False)       # remove vírgulas
            .str.replace(r'[^0-9]', '', regex=True)  # só número
        )

        dados[col] = pd.to_numeric(dados[col], errors='coerce')

    return dados

###
@flow(name = "Executando fluxo IQVIA")
def fluxo_iqvia(caminho:str) -> pd.DataFrame:
    dados = carregar_dados(caminho)
    dados = tratar_dados_iqvia(dados)
    return dados