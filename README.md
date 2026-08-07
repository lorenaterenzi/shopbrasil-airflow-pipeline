# ShopBrasil Analytics Pipeline - Apache Airflow

Este repositório contém o projeto de modernização da arquitetura de ingestão e processamento do catálogo de produtos do marketplace **ShopBrasil**, desenvolvido como Atividade Avaliativa da Pós-Graduação em Engenharia de IA e MLOps da PUC Minas.

## Principais Características e Requisitos Atendidos

1. **TaskFlow API & Modularização:** Construído usando decoradores `@dag` e `@task`, organizado modularmente em 3 `TaskGroups` (Ingestão, Análise e Persistência).
2. **Resiliência e Retries:** A task de ingestão possui tratamento contra instabilidades na API com **Exponential Backoff** e callbacks de ciclo de vida (`on_failure`, `on_retry`, `on_success`).
3. **Escalonamento Dinâmico (Fan-out / Fan-in):** Utilização de **Dynamic Task Mapping (`.expand()`)** para calcular as métricas individualmente por categoria em paralelo.
4. **Gerenciamento de Recursos:** Limitação de concorrência configurada para executar via `pool` no Airflow (`ecommerce_pool`).
5. **Persistência Idempotente:** Escrita no banco analítico PostgreSQL garantindo que re-execuções no mesmo dia não dupliquem registros na tabela snapshot.
6. **Requisitos Opcionais Implementados:**
   - Operador Customizado `ValidarProdutosOperator` para verificação de schema dos dados.
   - Gravação adicional em tabela histórica no formato Append.
   - Sistema de Callbacks configurado para simulador de alerta SLA.

---

## Como Executar o Projeto

### Pré-requisitos
- Docker Engine e Docker Compose instalados.

### Passos:

#### 1. Clone o repositório e navegue até a pasta do projeto:
   ```bash
   git clone <https://github.com/lorenaterenzi/shopbrasil-airflow-pipeline>
   cd shopbrasil-airflow-pipeline
   ```

#### 2. Suba o ambiente via Docker Compose:
   ```bash
   docker-compose up -d
```
#### 3. Acesse a Interface Web do Airflow:

- **URL**: http://localhost:8080

- **Usuário**: admin

- **Senha**: admin

#### 4. Configurações Prévias no Painel do Airflow (Admin):

#### Conexão PostgreSQL:

Vá em Admin -> Connections -> Clique em "+":

- **Conn Id**: postgres_shopbrasil

- **Conn Type**: Postgres

- **Host**: postgres-analytics

- **Database**: shopbrasil_db

- **Login**: shop_admin

- **Password**: shop_password

- **Port**: 5432

#### Pool de Concorrência:

Vá em Admin -> Pools -> Clique em "+":

- **Pool**: ecommerce_pool

- **Slots**: 2

- Ative a chave (toggle ON) e execute a **DAG** shopbrasil_catalog_pipeline.
