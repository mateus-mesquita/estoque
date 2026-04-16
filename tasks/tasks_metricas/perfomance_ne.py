import pandas as pd
import numpy as np
from prefect import task
from funcoes.funcoes_axiliares import soma_ul_6_meses   

@task(name = "Calculando Perfomance - NE")
def faturamento2025NE(dados:pd.DataFrame) -> pd.DataFrame:
    dados = soma_ul_6_meses(dados)

    # Calculando a performance apenas para o Nordeste
    # Primero criamos um valor que é 0 para quem não é NE, para que a soma do grupo seja apenas NE
    dados['Soma_NE_temp'] = np.where(dados['DISTRIBUIDOR'] == 'GRUPO_NORDESTE', dados['Soma ult.6 meses'], 0)
    
    dados['Performance (2025) - NE'] = dados.groupby('PRODUTO')['Soma_NE_temp'].transform('sum')
    
    # Deletando coluna temporária
    dados = dados.drop(columns=['Soma_NE_temp'])
    
    return dados


