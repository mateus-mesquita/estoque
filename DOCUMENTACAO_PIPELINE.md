# Documentação da Pipeline de Estoque

Esta documentação descreve a orquestração e as etapas de processamento de dados do sistema de estoque, utilizando **Prefect** para gerenciar o fluxo de informações entre o **Catálogo de Produtos** e os dados de mercado da **IQVIA**.

---

## 1. Visão Geral da Arquitetura

O pipeline é estruturado de forma modular, com fluxos (*flows*) que orquestram tarefas (*tasks*) específicas de limpeza, transformação e cálculo de métricas de performance.

- **Camada de Ingestão**: Carrega os arquivos brutos (Excel/CSV) e realiza limpezas iniciais.
- **Camada de Base**: Cruza as informações do Catálogo de Produtos com os dados da IQVIA.
- **Camada de Métricas**: Calcula faturamentos e performances por mercado, molécula e apresentação para o ano de 2025.
- **Saída**: Gera um arquivo consolidado em Excel com os resultados finais.

---

## 2. Fluxos Principais (Flows)

### 2.1 `PlanilhaResultados` (Arquivo: `fluxo_main.py`)
> **Ponto de Entrada (Entrypoint)**
> **Nome no Prefect:** `Executando planilha com os resultados`

Este é o fluxo principal que coordena toda a execução:
1.  **Obtenção da Base**: Chama o sub-fluxo `fluxo_base_final` para obter os dados cruzados.
2.  **Cálculo de Faturamento NE**: Aplica a task `faturamento2025NE`.
3.  **Cálculo de Faturamento ME**: Aplica a task `faturamento2025ME`.
4.  **Performance por Molécula**: Calcula a performance global da molécula no mercado via `performance_molecula_mercado`.
5.  **Performance por Apresentação**: Calcula a performance específica da apresentação via `performance_molecula_apr_mercado`.
6.  **Limpeza Final**: Filtra as colunas necessárias e exporta para `resultados.xlsx`.

### 2.2 `fluxo_base_final` (Arquivo: `fluxo_base.py`)
> **Nome no Prefect:** `Tabela final: variável de apresentação criada`

Responsável pela unificação das fontes de dados:
- Executa `fluxo_base_ctlg_prod` e `fluxo_iqvia`.
- Seleciona colunas estratégicas (FCC, NDF, Corporação, etc.).
- Realiza o cruzamento (*join*) das tabelas através da task `juntar_tabelas`.

### 2.3 `fluxo_base_ctlg_prod` (Arquivo: `fluxo_catalogo.py`)
> **Nome no Prefect:** `Executando base de catálogo de produtos`

Tratamento do catálogo:
- Remove linhas sem EAN.
- Cria colunas de apresentação amigáveis (`criar_col_apresentcao`).
- Normaliza valores (`melhorar_valores`).

### 2.4 `fluxo_iqvia` (Arquivo: `fluxoIQVIA.py`)
> **Nome no Prefect:** `Executando fluxo IQVIA`

Tratamento dos dados IQVIA:
- Carrega os arquivos brutos.
- Aplica saneamento especializado via `tratar_dados_iqvia`.

---

## 3. Principais Tarefas (Tasks)

As tarefas são funções atômicas que operam sobre DataFrames Pandas:

### 📁 Métricas de Performance (`tasks/tasks_metricas`)
- **`faturamento2025NE`**: Calcula o faturamento para o setor NE em 2025.
- **`faturamento2025ME`**: Calcula o faturamento para o mercado total em 2025.
- **`performance_molecula_mercado`**: Agrupa e soma o volume dos últimos 6 meses por Molécula.
- **`performance_molecula_apr_mercado`**: Agrupa e soma o volume dos últimos 6 meses por Molécula e Apresentação.

### 📁 Tabela e Utilidades (`tasks/tasks_tabela`)
- **`juntar_tabelas`**: Realiza o merge entre o Catálogo e a IQVIA usando a chave FCC.
- **`selecionar_cols`**: Filtra as colunas finais para o relatório.

---

## 4. Fontes de Dados (Input/Output)

- **Input**:
  - `arquivos/CATALOGO_DE_PRODUTOS.xlsx`
  - `arquivos/12_IQVIADEZEMBRO2025CSV.csv`
- **Output**:
  - `resultados.xlsx`
