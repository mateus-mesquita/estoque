# importações
import pandas as pd
from prefect import task

# Construção da métrica de valor bruto dos dois anos
@task(name = "Calculando o volume bruto de unidades")
def volume_bruto(dados:pd.DataFrame):
    dados['Soma Total de Unidades'] = (dados['UNIDADES_202401'] + dados['UNIDADES_202402'] + dados['UNIDADES_202403'] + 
                                       dados['UNIDADES_202404'] + dados['UNIDADES_202405'] + dados['UNIDADES_202406'] +
                                       dados['UNIDADES_202407'] + dados['UNIDADES_202408'] + dados['UNIDADES_202409'] +
                                       dados['UNIDADES_202410'] + dados['UNIDADES_202411'] + dados['UNIDADES_202412'] +
                                       dados['UNIDADES_202501'] + dados['UNIDADES_202502'] + dados['UNIDADES_202503'] +
                                       dados['UNIDADES_202504'] + dados['UNIDADES_202505'] + dados['UNIDADES_202506'] +
                                       dados['UNIDADES_202507'] + dados['UNIDADES_202508'] + dados['UNIDADES_202509'] +
                                       dados['UNIDADES_202510'] + dados['UNIDADES_202511'] + dados['UNIDADES_202512'])
    
    return dados