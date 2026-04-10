# importações 
import pandas as pd
from prefect import task
from halo import Halo

# função para filtrar dados do mercado iqvia
@task(name = "Filtrando dados com base nas moléculas do estoque a qualificar")
def filtrar_moleculas(dados:pd.DataFrame, moleculas:list) -> pd.DataFrame:
    dados = dados.query("Molecula in @moleculas")
    return dados

