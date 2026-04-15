# importações
import pandas as pd
from prefect import flow, task
from fluxos.fluxo_base import fluxo_base_final


# Função para criar a coluna total de faturamento
def faturamento_total(dados:pd.DataFrame) -> pd.DataFrame:
    dados['Faturamento Ult.6 meses'] = (dados['RS_PPP_202506']+dados['RS_PPP_202507']+dados['RS_PPP_202508']+
    dados['RS_PPP_202509']+dados['RS_PPP_202510']+dados['RS_PPP_202511']+dados['RS_PPP_202512'])
    return dados

# Para um primeiro momento, será construido uma tabela atrvés da iteração da tabela principal
@task(name = "Tabela de Faturamento Geral dos Produtos - Grupo Nordeste")
def tabela_faturamento_prod(dados:pd.DataFrame) -> pd.DataFrame:
    # criando tabela de faturamento geral por produtos
    lista = []
    # Iteração para a criação da tabela
    for idx, row in dados.iterrows():
        # definindo condição de criação da tabela de faturamento dos ultimos 6 mesmes
        if row['UF'] in ('CE','PI','MA') and row['DISTRIBUIDOR'] == 'GRUPO_NORDESTE':
            # inserindo dados
            lista.append({"id":idx,
                        "Produto":row['PRODUTO'],
                        "UF":row['UF'],
                        "Faturamento Ult.6 meses - NE":row['Faturamento Ult.6 meses']
                    })
        else:
            # inserindo dados caso a condição não seja atendida
            lista.append({"id":idx,
                        "Produto":row['PRODUTO'],
                        "UF":row['UF'],
                        "Faturamento Ult.6 meses - NE":0
                    })
    # Retornando dataframe
    return pd.DataFrame(lista)

    

# Aplicando a criação dos dados para a soma total do mercado, com base apenas no UF
@task(name = "Tabela de Faturamento Geral dos Produtos")
def tabela_faturamento_prod(dados:pd.DataFrame) -> pd.DataFrame:
    # criando tabela de faturamento geral por produtos
    lista = []
    # Iteração para a criação da tabela
    for idx, row in dados.iterrows():
        # definindo condição de criação da tabela de faturamento dos ultimos 6 mesmes
        if row['UF'] in ('CE','PI','MA') and row['DISTRIBUIDOR'] == 'GRUPO_NORDESTE':
            # inserindo dados
            lista.append({"id":idx,
                        "Produto":row['PRODUTO'],
                        "UF":row['UF'],
                        "Faturamento Ult.6 meses":row['Faturamento Ult.6 meses']
                    })
        else:
            # inserindo dados caso a condição não seja atendida
            lista.append({"id":idx,
                        "Produto":row['PRODUTO'],
                        "UF":row['UF'],
                        "Faturamento Ult.6 meses":0
                    })
    # Retornando dataframe
    return pd.DataFrame(lista)


# Executando teste de criação de tabelas
if __name__ == "__main__":
    dados = fluxo_base_final("CATALOGO_DE_PRODUTOS.xlsx","12_IQVIADEZEMBRO2025CSV.csv")
    dados = faturamento_total(dados)
    dados = tabela_faturamento_prod(dados)
    dados.to_excel("teste.xlsx")
    print(dados)