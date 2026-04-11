# Diagrama do Fluxo de Dados

Com base na sua documentação (`DOCUMENTACAO_PIPELINE.md`), eu construí este diagrama Mermaid ilustrando a arquitetura do seu fluxo no Prefect, mostrando desde o carregamento das origens de dados até a exportação final.

```mermaid
graph TD
    %% Definição de Estilos
    classDef flow fill:#e1f5fe,stroke:#01579b,stroke-width:2px;
    classDef task fill:#fff3e0,stroke:#e65100,stroke-width:1px,color:#000;
    classDef source fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px;
    classDef output fill:#fce4ec,stroke:#880e4f,stroke-width:2px;

    %% Fontes de Dados (Inputs)
    Cat[("Planilha: Catálogo<br>(CATALOGO_DE_PRODUTOS)")]:::source
    IQV[("Planilha: IQVIA<br>(12_IQVIADEZEMBRO)")]:::source
    Est[("Planilha: Estoque<br>(dados_estoque_qualif)")]:::source

    %% Camada Inferior (Camada de Ingestão e Tratamento Primário)
    subgraph FLUXO_CATALOGO ["1. fluxo_base_ctlg_prod"]
        direction TB
        C1(carregar_dados):::task
        C2(melhorar_valores):::task
        C3(criar_col_apresentcao):::task
        C4("dropna EAN"):::task
        C1 --> C2 --> C3 --> C4
    end

    subgraph FLUXO_IQVIA ["2. fluxo_iqvia"]
        direction TB
        I1(carregar_dados):::task
        I2(tratar_dados_iqvia):::task
        I1 --> I2
    end

    Cat --> C1
    IQV --> I1

    %% Camada Intermediária (Cruzamento e Refino de Features)
    subgraph FLUXO_BASE ["3. fluxo_base_final"]
        direction TB
        B1(juntar_tabelas):::task
        B2(selecao_features_e_cols):::task
    end

    C4 --> B1
    I2 --> B1
    B1 --> B2

    %% Camada Intermediária Alta (Integração com Estoque Real)
    subgraph PLANILHA_FINAL ["4. PlanilhaFinal"]
        direction TB
        P1(carregar_dados - Estoque):::task
        P2(flag: Estoque.qualif):::task
    end

    Est --> P1
    B2 --> P2
    P1 --> P2

    %% Camada de Saída (Cálculos de Negócio e Exportação)
    subgraph METRICAS ["5. TabelaFinal (Entrypoint)"]
        direction TB
        M1(media_trimestral):::task
        M2(volume_bruto):::task
        M3(selecionar_cols: remove colunas brutas):::task
    end

    P2 --> M1
    M1 --> M2
    M2 --> M3

    %% Output Final
    OUT[("Tabela Final Gerada<br>(teste.xlsx)")]:::output
    M3 --> OUT
```

### O que o diagrama demonstra:
1. **Fontes de Dados (Verde):** A leitura das 3 planilhas principais de forma segmentada.
2. **Pipelines / Flows (Caixas grandes):** Agrupamento que mostra como cada nível é isolado, mas alimenta o fluxo superior.
3. **Tarefas (Laranja):** Detalhamento das funções atômicas gerenciadas pelas Flows em sua ordem respectiva.
4. **Produto Final (Rosa):** Ponto de exportação para a planilha excel contendo as métricas trimestrais.
