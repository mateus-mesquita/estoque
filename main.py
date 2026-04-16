from fluxos.fluxo_main import PlanilhaResultados

dados = PlanilhaResultados(
    "arquivos/CATALOGO_DE_PRODUTOS.xlsx",
    "arquivos/12_IQVIADEZEMBRO2025CSV.csv"
)

print(dados)
dados.to_excel("arquivos/resultados.xlsx", index=False)