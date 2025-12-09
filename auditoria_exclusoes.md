# AUDIT LOG & DECISÕES TÉCNICAS: Dashboard SRAG (e-SUS Notifica)

**Status do Projeto:** Em Produção

## 1. REGISTRO DE EXCLUSÕES

Para garantir a qualidade e a performance do Data Warehouse, foram aplicadas as seguintes regras de exclusão de dados brutos durante o pipeline de ETL.

### 1.1. Colunas Descartadas
As seguintes colunas presentes no CSV original foram removidas do modelo final por falta de relevância analítica ou redundância técnica:

| Coluna Original | Motivo da Exclusão |

| `source_id` | Identificador interno da fonte, sem utilidade para análise epidemiológica. |

| `excluido` | Flag de sistema. Apenas registros ativos foram processados. |

| `validado` | Flag administrativa interna. |

| `outroBuscaAtivaAssintomatico` | Campo de texto livre com baixa estruturação e alta cardinalidade. |

| `outroTriagemPopulacaoEspecifica` | Campo de texto livre irrelevante para métricas quantitativas. |

| `outroLocalRealizacaoTestagem` | Campo de texto livre. A análise foca no local padronizado. |

### 1.2. Linhas Descartadas
Foram aplicados filtros restritivos (DROP) nas seguintes condições:

* **Notificações sem Data (`dataNotificacao` IS NULL):** Registros sem data de notificação foram removidos (aprox. 8 registros), pois violam a integridade temporal essencial para séries temporais e triggers de auditoria.


## 2. ALTERAÇÕES NO MODELO DE DADOS (SCHEMA EVOLUTION)

Durante a ingestão dos dados, o modelo relacional estrito (3FN) precisou ser adaptado para acomodar a realidade dos dados brutos (Data Reality).

| Tabela / Objeto | Alteração Realizada | Justificativa Técnica / Erro Corrigido |

| **`fato_notificacoes`** | Alteração de Tipo: `codigo_cbo` de `VARCHAR(10)` para **`VARCHAR(255)`** | **Erro de Truncagem:** O dado real continha a descrição completa da profissão (ex: "2235 - Enfermeiros...") e não apenas o código. |

| **`fato_notificacoes`** | Alteração de Tipo: `codigo_doses_vacina` de `SMALLINT` para **`VARCHAR(100)`** | **Erro de Domínio:** O campo continha valores múltiplos e texto (ex: "1,2,Reforço"), impossibilitando o uso de inteiros. |

| **`fato_notificacoes`** | Adição de Coluna: `codigo_estrategia_covid` | A coluna existia no ETL mas faltava no DDL inicial. |

| **`fato_testes_realizados`** | **Remoção de Constraints (FK)** | As chaves estrangeiras para `Dim_Fabricantes` e `Dim_Tipos_Testes` foram removidas. **Motivo:** O dataset de testes continha códigos que não possuíam correspondência nas tabelas de dimensão de domínio disponíveis, bloqueando a carga. |


## 3. REGRAS DE NEGÓCIO E LIMPEZA (ETL LOGIC)

Lógicas aplicadas via Python (`pipeline.py`) para sanear inconsistências nos dados.

### 3.1. Correção Cronológica (Data Quality)
* **Problema:** Registros onde `dataInicioSintomas` > `dataNotificacao`.
* **Decisão:** Forçar `dataInicioSintomas` = `dataNotificacao`.
* **Motivo:** Impossibilidade lógica de notificar uma doença antes dos sintomas começarem.

### 3.2. Tratamento de Booleanos e Nulos
* **Problema:** Colunas `profissionalSaude` e `profissionalSeguranca` com valores nulos ou "Ignorado".
* **Decisão:** Coerção para `FALSE`.
* **Motivo:** O PostgreSQL exige `BOOLEAN NOT NULL`. Assumiu-se a premissa de que "sem informação" equivale a "não é profissional da área".

### 3.3. Derivação Geográfica
* **Problema:** O CSV fornecia a sigla do estado ("PA") na coluna de código IBGE.
* **Decisão:** O código IBGE do Estado (INT) foi calculado extraindo os dois primeiros dígitos do código do município (Ex: 150100 -> 15).


## 4. CORREÇÕES EM OBJETOS ANALÍTICOS (VIEWS/FUNCTIONS)

Ajustes realizados no SQL após auditoria dos resultados do Dashboard.

| Objeto | Correção | Impacto no Dashboard |

| **View `vw_casos_por_municipio`** | Filtro de óbitos alterado de `'Óbito por SRAG'` para **`'Óbito'`**. | **Correção Crítica:** O dashboard mostrava 0 óbitos. Após ajuste para o termo exato do banco, passou a contabilizar (ex: 972 óbitos). |

| **View `vw_vacinacao_por_resultado`** | Lógica `CASE` atualizada para ler texto (`LIKE '%,%'`) em vez de números. | Permitiu a visualização do status vacinal após a alteração do tipo da coluna de doses. |

| **Func `fx_calcular_taxa_positividade`** | Referência de coluna corrigida de `codigo_resultado_teste` para **`fk_resultado_teste`**. | Permitiu o cálculo correto e a inserção na tabela `indicadores_municipais`. |

*Documento gerado automaticamente após execução do Pipeline v1.0.*