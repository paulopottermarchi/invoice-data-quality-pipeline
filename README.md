<div align="center">

# Pipeline de Anomalias em Invoices

**Pipeline de qualidade de dados para monitoramento automático de invoices em operações de cobrança**

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.9-017CEE?style=flat&logo=apache-airflow&logoColor=white)](https://airflow.apache.org)
[![Apache Spark](https://img.shields.io/badge/Spark-3.5-E25A1C?style=flat&logo=apache-spark&logoColor=white)](https://spark.apache.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)](https://docker.com)
[![pytest](https://img.shields.io/badge/pytest-passing-2EA44F?style=flat&logo=pytest&logoColor=white)](tests/)

</div>

---

## O Problema

Em uma operação de cobrança de dívidas, invoices do sistema de faturamento estavam sendo **importadas duas vezes** — mesmo registro, sem nenhum controle de deduplicação. A discrepância só aparecia no fechamento mensal, quando o impacto financeiro já estava registrado.

Este pipeline detecta três classes de anomalia em invoices automaticamente, todos os dias, antes que virem problemas de reconciliação.

---

## Arquitetura

```
┌──────────────────────────────────────────────────────────────┐
│                        Docker Compose                         │
│                                                               │
│   ┌─────────────────┐        ┌──────────────────────────┐    │
│   │   Airflow 2.9   │        │    Cluster Spark 3.5      │    │
│   │  ┌───────────┐  │        │  ┌────────┐ ┌──────────┐ │    │
│   │  │ Scheduler │  │──────▶ │  │ Master │ │  Worker  │ │    │
│   │  │ Webserver │  │        │  │        │ │  2G / 2C │ │    │
│   │  └───────────┘  │        │  └────────┘ └──────────┘ │    │
│   └─────────────────┘        └──────────────────────────┘    │
│              │                           │                    │
│   ┌──────────▼───────────────────────────▼────────────────┐  │
│   │                    Volumes Compartilhados              │  │
│   │   data/raw/              →  entrada CSV / JDBC         │  │
│   │   data/processed/        →  Parquet (particionado)     │  │
│   │   data/processed/reports →  relatórios markdown diários│  │
│   └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

> **Equivalente em produção:** Azure Data Factory (orquestração) → Azure Databricks (Spark) → ADLS Gen2 / Delta Lake (armazenamento)

---

## Pipeline

```
check_sqlserver_connection        verifica conectividade TCP antes de submeter o job
          │
    run_spark_job                 PySpark via JDBC → detecta anomalias
          │
   parse_spark_output             extrai métricas do stdout via XCom
          │
   branch_on_anomalies            BranchPythonOperator
          │
    ┌─────┴──────────────────┐
    ▼                         ▼
build_email → send_alert    pipeline_success
  EmailOperator (SMTP)        EmptyOperator
```

Agendamento: **diariamente às 07h00 BRT** — anomalias identificadas antes do horário comercial.

---

## Tipos de Anomalia

| Tipo | Descrição | Causa raiz |
|------|-----------|------------|
| `EXACT_DUPLICATE` | Mesma invoice importada mais de uma vez com dados idênticos | Importação em lote sem controle de deduplicação |
| `AMOUNT_MISMATCH` | Mesma invoice com valores diferentes entre sistemas de origem | Correção enviada sem cancelar o registro original |
| `CASE_MISMATCH` | Mesma invoice vinculada a diferentes IDs de caso (devedor) | Referência de caso incorreta na reimportação |
| `CLEAN` | Nenhuma anomalia detectada | — |

Prioridade de classificação: `CASE_MISMATCH` → `AMOUNT_MISMATCH` → `EXACT_DUPLICATE` → `CLEAN`

---

## Stack Tecnológica

| Camada | Tecnologia | Destaques |
|--------|-----------|-----------|
| Orquestração | Apache Airflow 2.9 | BranchPythonOperator, EmailOperator, XCom |
| Processamento | Apache Spark 3.5 / PySpark | Window functions, pushdown JDBC |
| Conectividade | mssql-jdbc 12.6 | Conexão direta ao SQL Server via JDBC |
| Armazenamento | Parquet particionado por `anomaly_type` | Partition pruning para consumidores downstream |
| Containerização | Docker Compose | Airflow + Spark + Postgres em um comando |
| Testes | pytest + PySpark local session | 7 testes unitários, sem necessidade de cluster |
| Linguagem | Python 3.11 | |

---

## Como Rodar

**Pré-requisitos:** Docker Desktop, Python 3.11+

```bash
# 1. Clonar o repositório
git clone https://github.com/seu-usuario/pipeline-anomalias-invoices.git
cd pipeline-anomalias-invoices

# 2. Configurar o ambiente
cp .env.example .env
# Editar .env: preencher SQLSERVER_HOST, credenciais e configurações SMTP

# 3. Subir todos os serviços (Airflow + Spark + Postgres)
docker-compose up -d

# 4. Gerar dados sintéticos (~800 registros com anomalias injetadas)
python scripts/generate_invoices.py

# 5. Acessar a UI do Airflow → http://localhost:8080  (admin / admin)
#    Ativar o DAG: invoice_monitor_client_xyz
#    UI do Spark  → http://localhost:8081

# 6. Rodar os testes unitários (sem Docker)
pip install pyspark pytest pandas pyarrow
pytest tests/ -v
```

Para instruções detalhadas de configuração, consulte o [SETUP.md](SETUP.md).

---

## Saída

**Parquet particionado** — otimizado para filtragem downstream:

```
data/processed/invoice_anomalies/
├── anomaly_type=CLEAN/
├── anomaly_type=EXACT_DUPLICATE/
├── anomaly_type=AMOUNT_MISMATCH/
└── anomaly_type=CASE_MISMATCH/
```

**Relatório markdown diário:**

```
# Relatório de Anomalias em Invoices

Cliente: XYZ  |  Data: 2024-06-15  |  Total de registros: 810

| Tipo de Anomalia  | Registros | Invoices Únicas | Valor Total (R$) |
|-------------------|-----------|-----------------|------------------|
| CLEAN             | 700       | 700             | 2.450.312,00     |
| EXACT_DUPLICATE   | 40        | 20              |   143.890,50     |
| AMOUNT_MISMATCH   | 40        | 20              |    98.234,20     |
| CASE_MISMATCH     | 30        | 15              |    72.100,80     |
```

**Alerta por e-mail** — disparado automaticamente quando `anomaly_count >= threshold`:

- HTML formatado com tabela de anomalias por tipo
- Configurável via SMTP (Gmail App Password ou SMTP corporativo)
- Threshold ajustável via variável de ambiente `ANOMALY_ALERT_THRESHOLD`

---

## Decisões de Engenharia

**Pushdown query JDBC ao invés de full table scan**
O job Spark envia uma query filtrada ao SQL Server antes de transferir os dados, carregando apenas as linhas com os campos relevantes presentes. Evita trafegar dados desnecessários pela rede.

**Window functions ao invés de self-joins para deduplicação**
`COUNT OVER PARTITION BY` escala melhor que self-joins em grandes volumes de invoices. O mesmo padrão usado na query T-SQL de produção, traduzido diretamente para PySpark.

**Parquet particionado por `anomaly_type`**
Consumidores downstream (ferramentas de BI, consultas ad-hoc) quase sempre filtram por tipo de anomalia. O partition pruning elimina full scans desnecessários.

**BranchPythonOperator para o threshold de alerta**
Separa a regra de negócio (quantas anomalias justificam um alerta) das preocupações de infraestrutura (job Spark, I/O de arquivos). O threshold é configurável via `.env` sem alterar o código do DAG.

**Paridade semântica T-SQL → PySpark**
A lógica de detecção original usava funções específicas do SQL Server (`CHARINDEX`, `PATINDEX`, formato de data 104). Cada uma foi mapeada para equivalentes PySpark (`regexp_extract`, `substring`, `to_date`) preservando a lógica de negócio idêntica — incluindo uma inversão intencional de dia/mês presente no sistema legado.

---

## Estrutura do Projeto

```
pipeline-anomalias-invoices/
├── dags/
│   ├── dag_invoice_monitor_dentalpar.py          # DAG v1 — dados sintéticos (demo)
│   └── dag_description_anomaly_monitor.py        # DAG v2 — SQL Server real via JDBC
├── spark_jobs/
│   ├── invoice_anomaly_detection.py              # Spark job v1 — anomalias estruturais
│   └── invoice_description_anomaly_detection.py  # Spark job v2 — parsing de campos livres
├── scripts/
│   └── generate_invoices.py                      # Gerador de dados sintéticos (800 registros, 3 tipos de anomalia)
├── tests/
│   └── test_invoice_anomaly_detection.py         # 7 testes unitários, SparkSession local
├── data/
│   ├── raw/                                      # Entrada (gitignored)
│   └── processed/                                # Saída Parquet + relatórios (gitignored)
├── docker-compose.yml                            # Airflow 2.9 + Spark 3.5 + Postgres 15
├── .env.example                                  # Template de ambiente — copiar para .env
├── SETUP.md                                      # Guia de configuração passo a passo
└── README.md
```

---

## Caminho para Produção

| Atual (local / portfólio) | Produção (Azure) |
|---------------------------|------------------|
| Docker Compose | AKS / Azure Container Apps |
| CSV ou JDBC direto | Azure SQL / ADLS Gen2 |
| Cluster Spark standalone | Azure Databricks |
| Arquivos Parquet | Delta Lake com time travel |
| Relatório markdown | Atualização de dataset Power BI |
| Airflow EmailOperator | Databricks Workflows + ADF |
| pytest local | GitHub Actions CI |

O código do job Spark é **idêntico** nos dois ambientes — apenas o caminho de armazenamento e o destino do cluster mudam.

---

## Autor

**Paulo Ricardo Potter Marchi**
Analytics Engineer → Data Engineer

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Conectar-0A66C2?style=flat&logo=linkedin)](https://linkedin.com/in/seu-perfil)
[![GitHub](https://img.shields.io/badge/GitHub-Portfólio-181717?style=flat&logo=github)](https://github.com/seu-usuario)
