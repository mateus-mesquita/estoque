# importações
import pandas as pd
from catalogo_produto import carregar_dados
from dados_junc import fluxo_base_final, juntar_tabelas
from prefect import flow

# Fluxo de trabalho atualizado
@flow(name = "Execução dos dados finais")
def PlanilhaFinal(caminho_estoque:str, caminho_prod:str, caminho_iqvia:str) -> pd.DataFrame:
    dados_ = fluxo_base_final(caminho_prod, caminho_iqvia)
    dados_estoque = carregar_dados(caminho_estoque)

    # Realizando uma junção de tabelas novamente
    dados_final = juntar_tabelas(dados_,dados_estoque)
    return dados_final

### testando aplicação
teste = PlanilhaFinal("dados_estoque_qualif.xlsx","CATALOGO_DE_PRODUTOS.xlsx","12_IQVIADEZEMBRO2025CSV.csv")
teste.to_excel("teste.xlsx", index = False)
print(teste)
teste.to_excel("teste.xlsx")

