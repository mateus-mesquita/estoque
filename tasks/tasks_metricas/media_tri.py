# importações
from prefect import task
import pandas as pd

# calculando métrica média de Unidades em cada trimestre
@task(name = "calculando métrica de média")
def media_trimestral(dados: pd.DataFrame) -> pd.DataFrame:

    # 2024
    dados['Media.Und TRI-202403'] = (dados['UNIDADES_202401'] + dados['UNIDADES_202402'] + dados['UNIDADES_202403']) / 3
    dados['Media.Und TRI-202406'] = (dados['UNIDADES_202404'] + dados['UNIDADES_202405'] + dados['UNIDADES_202406']) / 3
    dados['Media.Und TRI-202409'] = (dados['UNIDADES_202407'] + dados['UNIDADES_202408'] + dados['UNIDADES_202409']) / 3
    dados['Media.Und TRI-202412'] = (dados['UNIDADES_202410'] + dados['UNIDADES_202411'] + dados['UNIDADES_202412']) / 3

    # 2025
    dados['Media.Und TRI-202503'] = (dados['UNIDADES_202501'] + dados['UNIDADES_202502'] + dados['UNIDADES_202503']) / 3
    dados['Media.Und TRI-202506'] = (dados['UNIDADES_202504'] + dados['UNIDADES_202505'] + dados['UNIDADES_202506']) / 3
    dados['Media.Und TRI-202509'] = (dados['UNIDADES_202507'] + dados['UNIDADES_202508'] + dados['UNIDADES_202509']) / 3
    dados['Media.Und TRI-202512'] = (dados['UNIDADES_202510'] + dados['UNIDADES_202511'] + dados['UNIDADES_202512']) / 3

    # === aplicando arredondamento dos dados ===
    dados['Media.Und TRI-202403'] = dados['Media.Und TRI-202403'].round(2)
    dados['Media.Und TRI-202406'] = dados['Media.Und TRI-202406'].round(2)
    dados['Media.Und TRI-202409'] = dados['Media.Und TRI-202409'].round(2)
    dados['Media.Und TRI-202412'] = dados['Media.Und TRI-202412'].round(2)
    dados['Media.Und TRI-202503'] = dados['Media.Und TRI-202503'].round(2)

    return dados