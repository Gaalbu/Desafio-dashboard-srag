Esse arquivo servirá ÚNICA e EXCLUSIVAMENTE para mantermos registro das alterações feitas nas tabelas e decisões tomadas.

### 1. DECISÕES CHAVE DE MODELAGEM RELACIONAL (3ª FN)

| Decisão | Detalhe | Justificativa |

| **SGBD Escolhido** | PostgreSQL | Robustez, suporte a recursos avançados (Views, Funções, Triggers) e popularidade em projetos de dados abertos. |

| **Chave Primária Principal** | `id_notificacao` (BIGSERIAL) na tabela `Fato_Notificacoes` | Criação de chave artificial sequencial para garantir unicidade e integridade referencial, desacoplando o modelo de um ID de origem. |

| **Modelagem N:M (Sintomas/Condições)** | Normalizada (Tabelas de Junção) | As colunas de sintomas e condições foram desmembradas em Dimensões (`Dim_Sintomas`, `Dim_Condicoes`) e Tabelas Fato de Junção (`Fato_Notificacao_Sintoma`, `Fato_Notificacao_Condicao`) para atingir a 3FN. |

| **Modelagem de Testes** | Normalizada (Tabela FATO `Fato_Testes_Realizados`) | As colunas `Teste1` a `Teste4` do CSV foram consolidadas em uma única tabela `Fato_Testes_Realizados` (1 linha = 1 teste), eliminando a redundância e permitindo N testes por notificação de forma flexível (3FN). |

### 2. REGISTRO DE COLUNAS DESCARTADAS

As seguintes colunas do CSV de origem foram **descartadas** no processo de ETL (Extração, Transformação, Carga) por baixa relevância analítica para o Dashboard final ou por serem redundantes após a normalização:

| Coluna Descartada | Categoria | Justificativa de Descarte |

| `source_id` | Metadados | ID interno da fonte de dados, sem valor analítico ou como PK. |

| `excluido` | Metadados/Flag | Flag de exclusão interna do sistema de origem. Registros excluídos serão filtrados na ETL. |

| `validado` | Metadados/Flag | Flag de validação interna do sistema. Não contribui para a análise epidemiológica do caso. |

| `outroBuscaAtivaAssintomatico` | Texto Livre | Informação de alta cardinalidade e baixo valor analítico. |

| `outroTriagemPopulacaoEspecifica` | Texto Livre | Informação de alta cardinalidade e baixo valor analítico. |

| `outroLocalRealizacaoTestagem` | Texto Livre | Informação de alta cardinalidade e baixo valor analítico. |