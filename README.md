# 🦠 Dashboard Epidemiológico SRAG (e-SUS Notifica)

Projeto de Engenharia de Dados que implementa um pipeline ETL completo, Data Warehouse normalizado e Dashboard interativo para análise de dados de Síndrome Gripal.

## 🎯 Objetivos
- Modelagem de Banco de Dados Relacional (PostgreSQL) em 3ª Forma Normal.
- Pipeline ETL em Python para limpeza e ingestão de dados massivos.
- Auditoria de Dados via Triggers e Stored Procedures.
- Visualização de Dados Interativa com Streamlit.

## 🛠️ Tecnologias
- **Linguagem:** Python 3.10+
- **Banco de Dados:** PostgreSQL
- **Bibliotecas:** Pandas, SQLAlchemy, Streamlit, Plotly
- **Ferramentas:** VSCode, PgAdmin

## 🚀 Como Rodar o Projeto

### Pré-requisitos
1. PostgreSQL instalado e rodando.
2. Criar um banco de dados chamado `esus_srag_db`.
3. Python instalado.

### Passo 1: Configuração
Clone o repositório e instale as dependências:
```bash
git clone [https://github.com/Gaalbu/esus-srag-dashboard.git](https://github.com/Gaalbu/esus-srag-dashboard.git)
pip install -r requirements.txt
```

### Passo 2: Instalar Dependências
No terminal, na pasta do projeto:
```bash
pip install -r requirements.txt
```

### Passo 3: Executar o ETL (Carga de Dados)
Certifique-se de que o arquivo `dataset_notif_sus.csv` está na pasta do projeto (ou ajuste o caminho no código).
```bash
python pipeline.py
```

Aguarde a mensagem: *"✅ Pipeline ETL concluído com sucesso!"*

### Passo 4: Abrir o Dashboard
Rode o comando do Streamlit:
```bash
streamlit run app.py
```

O navegador abrirá automaticamente com os gráficos, mapas e análises.
