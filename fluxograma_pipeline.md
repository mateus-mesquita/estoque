# Diagrama do Fluxo de Dados (Pipeline de Estoque)

Este diagrama representa a arquitetura atualizada do fluxo de dados orquestrado pelo Prefect, conforme definido em `fluxo_main.py`.

```mermaid
graph TD
    %% Estilos
    classDef flow fill:#e1f5fe,stroke:#01579b,stroke-width:2px;
    classDef task fill:#fff3e0,stroke:#e65100,stroke-width:1px,color:#000;
    classDef source fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px;
    classDef output fill:#fce4ec,stroke:#880e4f,stroke-width:2px;

    %% Fontes de Dados
    Cat[("arquivos/CATALOGO_DE_PRODUTOS.xlsx")]:::source
    IQV[("arquivos/12_IQVIADEZEMBRO2025CSV.csv")]:::source

    %% Fluxo de Catálogo
    subgraph FLUXO_CATALOGO ["fluxo_base_ctlg_prod"]
        C1(carregar_dados):::task
        C2(dropna: EAN):::task
        C3(criar_col_apresentcao):::task
        C4(melhorar_valores):::task
        C1 --> C2 --> C3 --> C4
    end

    %% Fluxo IQVIA
    subgraph FLUXO_IQVIA ["fluxo_iqvia"]
        I1(carregar_dados):::task
        I2(tratar_dados_iqvia):::task
        I1 --> I2
    end

    Cat --> C1
    IQV --> I1

    %% Fluxo Base
    subgraph FLUXO_BASE ["fluxo_base_final"]
        B1(juntar_tabelas):::task
        B2(selecionar_cols):::task
        B1 --> B2
    end

    C4 --> B1
    I2 --> B1

    %% Fluxo Principal (Métricas)
    subgraph FLUXO_MAIN ["PlanilhaResultados (Entrypoint)"]
        direction TB
        M1(faturamento2025NE):::task
        M2(faturamento2025ME):::task
        M3(performance_molecula_mercado):::task
        M4(performance_molecula_apr_mercado):::task
        M5(selecionar_cols: Limpeza Final):::task
        
        M1 --> M2 --> M3 --> M4 --> M5
    end

    B2 --> M1

    %% Saída
    OUT[("resultados.xlsx")]:::output
    M5 --> OUT
```

### Explicação do Fluxo:
1.  **Ingestão**: Os dados do Catálogo e da IQVIA são lidos e tratados em sub-fluxos isolados.
2.  **Unificação**: O `fluxo_base_final` combina as fontes em um único DataFrame filtrado.
3.  **Processamento de Negócio**: O fluxo principal (`PlanilhaResultados`) aplica as quatro tarefas de cálculo de performance e faturamento.
4.  **Entrega**: O resultado é limpo de colunas temporárias e salvo como Excel.
