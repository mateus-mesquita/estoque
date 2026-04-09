# Documentação da Pipeline de Estoque

Esta documentação fornece uma visão clara sobre a orquestração e as etapas de transformação construídas com o [Prefect](https://www.prefect.io/) para o processamento e cruzamento de informações de **Catálogo de Produtos**, dados de mercado **IQVIA** e base de **Estoque**.

---

## 1. Visão Geral da Arquitetura

O processo é desenhado em formato de **camadas** (flows compostos) e tarefas atômicas (*tasks*). 

- A camada mais profunda carrega e formata cada fonte de forma independente (`fluxo_catalogo.py` e `fluxoIQVIA.py`).
- O meio do processo faz agregações, cruzamentos e seleciona as features importantes (`fluxo_base.py` e `fluxo_final.py`).
- A camada superior e final gera os resultados analíticos exigidos p/ visualização, aplicando os cálculos estatísticos e de limpeza final (`fluxo_metricas.py`).

---

## 2. Fluxos Principais (Flows)

Os fluxos orquestram as execuções, unindo múltiplas tarefas em processos monitoráveis no Prefect.

### 2.1 `TabelaFinal` (Arquivo: `fluxo_metricas.py`)
> **Ponto de entrada (Entrypoint) de todo o pipeline.** 
> **Nome da Flow no Prefect:** `Executando cálculos de métricas`

- **O que faz:** Recebe como parâmetro os caminhos dos arquivos do estoque, produtos e IQVIA. 
- **Etapas:** 
  1. Aciona o fluxo auxiliar `PlanilhaFinal` para obter o DataFrame cruzado e completo.
  2. Aciona o cálculo de médias trimestrais pelas tarefas de negócio (`media_trimestral`).
  3. Aciona o cálculo do volume (`volume_bruto`).
  4. Realiza uma etapa final de remoção (*drop*) das colunas brutas mensais de unidades (repassando atributos com o sufixo `'UNIDADES'`).

### 2.2 `PlanilhaFinal` (Arquivo: `fluxo_final.py`)
> **Nome da Flow no Prefect:** `Execução dos dados finais`

- **O que faz:** Acopla as informações agrupando o Catálogo+IQVIA com um balanço real do **Estoque**.
- **Etapas:**
  1. Chama o fluxo `fluxo_base_final` carregando o catálogo de Produtos e IQVIA pré-tratados.
  2. Avalia a parte independente usando o `carregar_dados` para o `caminho_estoque`.
  3. Cria as flags/indicações (task `flag`) identificando se determinado produto consolidado está ou não fisicamente registrado no estoque (coluna condicional `Estoque.qualif`).

### 2.3 `fluxo_base_final` (Arquivo: `fluxo_base.py`)
> **Nome da Flow no Prefect:** `Tabela final: variável de apresentação criada`

- **O que faz:** Cuida de padronizar ambos os catálogos (Produtos e Histórico Mercado) e fazer o cruzamento.
- **Etapas:** 
  1. Levanta as features unidas executando as flows `fluxo_base_ctlg_prod` e `fluxo_iqvia`.
  2. Restringe as bases contendo o essencial usando uma bateria pesada de seleção de features (FCC, Setor de Produto, Nomes das Corporações, Códigos NEC, ATC, etc).
  3. Exclui colunas indesejadas e cruza os dados verticalmente mantendo os trimestres vigentes através da função (task) `juntar_tabelas`.

### 2.4 `fluxo_base_ctlg_prod` (Arquivo: `fluxo_catalogo.py`)
> **Nome da Flow no Prefect:** `Executando base de catálogo de produtos`

- **O que faz:** Etapa primária de extração da base da indústria.
- **Atividades:** Exclui valores Nulos/NAs de produtos não codificados no EAN (`dropna(subset=['EAN'])`). Realiza tarefas secundárias focadas em criar representações legíveis para o produto (`criar_col_apresentcao`) e limpezas textuais/numéricas gerais (`melhorar_valores`).

### 2.5 `fluxo_iqvia` (Arquivo: `fluxoIQVIA.py`)
> **Nome da Flow no Prefect:** `Executando fluxo IQVIA`

- **O que faz:** Etapa de extração primária dos dados de mercado referentes à IQVIA.
- **Atividades:** Basicamente efetua o carregamento do DataFrame e delega o saneamento pesado para uma *task* isolada (`tratar_dados_iqvia`).

---

## 3. Tarefas (Tasks)

As `@tasks` do Prefect são responsáveis pelas modificações lógicas diretas nos *DataFrames* do Pandas. O pipeline está dividido nos seguintes blocos estruturados em arquivos independentes:

### 📁 Módulo de Arquivos (`tasks/arquivos`)
Tarefas primitivas focadas no arquivo original, lidam com a ingestão e saneamento elementar da base.
- **`carregar_dados`**: Ponto em que os CSVs ou planilhas são importados com o Pandas.
- **`melhorar_valores`**: Ajustes de normalização ou tipagem de colunas do catálogo.
- **`criar_col_apresentcao`**: Manipulação de strings para formular um texto de visualização intuitiva sobre as concentrações ou as embalagens do produto.

### 📁 Módulo IQVIA (`tasks/tasks_iqvia`)
- **`tratar_dados_iqvia`**: Task orientada para sanear detalhadamente a lógica das colunas geradas pela fonte IQVIA (padronização de valores de laboratório e nomenclaturas estruturais).

### 📁 Módulo de Métricas (`tasks/tasks_metricas`)
Implementam o valor de negócio agregando históricos por períodos de tempo:
- **`media_trimestral`**: Cria médias de venda unitárias agrupando dados de três meses sequenciais de saídas (ex: _Media.Und TRI-202403_), e realiza o arredondamento visual da métrica usando `.round(2)`.
- **`volume_bruto`**: Elabora somatórias globais e contadores do volume no período analisado.

### 📁 Módulo de Resultados (`tasks/tasks_resultado`)
- **`flag`**: Classifica os itens contra a base verificada. Ex: `pd.isin()` que avalia se os EANs finais contidos nas tabelas baseadas do catálogo constam na listagem final importada de Estoque. É geradora da métrica principal `Estoque.qualif`.

### 📁 Módulo de Tabela (`tasks/tasks_tabela`)
Gerenciadores utilitários da biblioteca Pandas responsáveis por intersecção e filtros nas planilhas:
- **`juntar_tabelas`**: Ferramenta de cruzamento ou acoplamento relacional entre relatórios de Produtos e da IQVIA (Variação do conceito de PROCV / `.merge()`).
- **`selecionar_cols`**: Usado estrategicamente para descartar ou eliminar múltiplas variáveis que não vão figurar como output na exportação (Atua via `dados.drop`).
