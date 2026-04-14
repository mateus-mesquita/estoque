# importações
import pandas as pd
from prefect import task

# Criando fluxo para flag 
# ideia principal é que a flag indique se o produto, está ou não no estoque a qualificar
@task(name = "Classificando produtos")
def flag(dados:pd.DataFrame, dados_estoque:pd.DataFrame) -> pd.DataFrame:
    try:
        dados['Estoque.qualif'] = dados['EAN'].isin(dados_estoque['EAN'])
        return dados
    except Exception as e:
        logger.error(f"Erro ao classificar produtos: {e}")
        raise e
