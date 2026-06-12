# SETUP.md — Guia de Configuração do Monitor de Anomalias

## Pré-requisitos

- Docker Desktop instalado e rodando
- Acesso à rede da empresa (SQL Server acessível via IP)
- Python 3.11+ (apenas para rodar os scripts locais)

---

## Passo 1 — Configurar variáveis de ambiente

```bash
# Copie o template
cp .env.example .env

# Edite com seus valores reais
notepad .env   # Windows
nano .env      # Linux/Mac
```

Preencha obrigatoriamente:

| Variável | Exemplo | Descrição |
|----------|---------|-----------|
| `SQLSERVER_HOST` | `YOUR_SQLSERVER_IP` | IP do SQL Server da empresa |
| `SQLSERVER_PORT` | `1433` | Porta padrão SQL Server |
| `SQLSERVER_DATABASE` | `your_database` | Banco de dados |
| `SQLSERVER_USER` | `your_user` | Usuário com SELECT em dbo.invoice e dbo.case |
| `SQLSERVER_PASSWORD` | `***` | Senha do usuário |
| `SMTP_HOST` | `smtp.gmail.com` | Servidor SMTP para alertas |
| `SMTP_PORT` | `587` | Porta SMTP |
| `SMTP_USER` | `seuemail@gmail.com` | Remetente dos alertas |
| `SMTP_PASSWORD` | `app_password` | Senha de app do Gmail (não a senha normal) |
| `ALERT_RECIPIENTS` | `your_email@example.com,team@example.com` | Destinatários separados por vírgula |

### Gmail — Criar App Password
1. Acesse: https://myaccount.google.com/apppasswords
2. Crie uma senha para "Mail"
3. Use essa senha em `SMTP_PASSWORD` (não sua senha normal do Google)

---

## Passo 2 — Subir o ambiente

```bash
# Na pasta do projeto:
docker-compose up -d

# Aguarde ~60 segundos para o Airflow inicializar
# Verifique os logs:
docker-compose logs airflow-webserver --tail=20
```

O serviço `jdbc-jar-downloader` faz o download automático do driver
`mssql-jdbc-12.6.1.jre11.jar` do Maven Central na primeira execução.

---

## Passo 3 — Acessar o Airflow

Abra: http://localhost:8080  
Login: `admin` / `admin`

### Ativar o DAG
1. Localize `invoice_description_anomaly_monitor`
2. Clique no toggle para ativar
3. Para rodar imediatamente: botão ▶️ (Trigger DAG)

---

## Passo 4 — Verificar conectividade com o SQL Server

A primeira task do DAG (`check_sqlserver_connection`) faz um teste TCP
antes de submeter o Spark job. Se falhar:

```
❌ Cannot reach SQL Server at YOUR_SQLSERVER_IP:1433
```

Verifique:
- Você está na rede da empresa (ou VPN ativa)
- `SQLSERVER_HOST` está correto no `.env`
- Firewall da empresa permite a porta 1433 a partir da máquina local
- O usuário do SQL Server tem permissão `SELECT` nas tabelas:
  - `dbo.invoice`
  - `dbo.[case]`

---

## Passo 5 — Acompanhar a execução

| Interface | URL | O que mostra |
|-----------|-----|--------------|
| Airflow UI | http://localhost:8080 | DAG runs, task logs, XCom values |
| Spark UI | http://localhost:8081 | Jobs Spark em execução |
| Logs do Airflow | `docker-compose logs airflow-scheduler -f` | Logs em tempo real |

---

## Resultados gerados

### Parquet (particionado por tipo de anomalia)
```
data/processed/description_anomalies/
├── motivo_erro=AMBOS: MES DIVERGENTE + DPD NEGATIVO/
├── motivo_erro=MES DIVERGENTE/
├── motivo_erro=DPD NEGATIVO/
└── motivo_erro=CLEAN/
```

### Relatório Markdown diário
```
data/processed/reports/description_anomalies_2024-06-15.md
```

### Email de alerta
Enviado automaticamente quando `anomaly_count >= ANOMALY_ALERT_THRESHOLD` (padrão: 1).

Conteúdo do email:
- Total de registros analisados
- Quantidade e percentual de anomalias
- Tabela por tipo de anomalia
- Relatório completo em texto

---

## Estrutura dos arquivos

```
invoice-anomaly-monitor/
├── dags/
│   ├── dag_invoice_monitor_client_xyz.py          # DAG original (dados sintéticos)
│   └── dag_description_anomaly_monitor.py        # DAG novo (SQL Server real via JDBC)
├── spark_jobs/
│   ├── invoice_anomaly_detection.py              # Spark job original
│   └── invoice_description_anomaly_detection.py  # Spark job novo (replica T-SQL)
├── scripts/
│   └── generate_invoices.py                      # Gerador de dados sintéticos
├── tests/
│   └── test_invoice_anomaly_detection.py         # Testes unitários
├── data/
│   ├── raw/
│   └── processed/
├── .env.example     ← copie para .env e preencha
├── docker-compose.yml
├── SETUP.md         ← este arquivo
└── README.md
```

---

## Parar o ambiente

```bash
docker-compose down          # para os containers, mantém os dados
docker-compose down -v       # para E apaga todos os volumes (reset total)
```

---

## Troubleshooting

### Spark job falha com "No suitable driver found"
O jar JDBC não foi baixado. Verifique:
```bash
docker volume inspect invoice-anomaly-monitor_spark-jars
docker-compose logs jdbc-jar-downloader
```

### Email não enviado
Verifique as variáveis SMTP no `.env` e os logs do Airflow:
```bash
docker-compose logs airflow-scheduler | grep -i smtp
```

### DAG não aparece no Airflow
```bash
docker-compose exec airflow-scheduler airflow dags list
docker-compose exec airflow-scheduler airflow dags report
```
