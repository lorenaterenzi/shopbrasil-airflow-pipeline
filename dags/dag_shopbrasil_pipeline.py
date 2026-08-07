import logging
from datetime import datetime
import pendulum
import requests
import pandas as pd

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from plugins.operators.custom_operators import validar_produtos_func

# Timezone fixo para America/Sao_Paulo conforme requisito
local_tz = pendulum.timezone("America/Sao_Paulo")

# Callbacks de monitoramento e alertas
def on_failure_alert(context):
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    logging.error(f"🚨 ALERT [SLA/FAILURE]: Task {task_id} falhou na execução {execution_date}.")

def on_retry_alert(context):
    task_id = context.get('task_instance').task_id
    logging.warning(f"⚠️ RETRY: Tentando re-executar a Task {task_id} devido a uma oscilação...")

def on_success_log(context):
    task_id = context.get('task_instance').task_id
    logging.info(f"✅ SUCCESS: Task {task_id} concluída com sucesso.")

default_args = {
    'owner': 'tech_lead_dados',
    'depends_on_past': False,
    'email_on_failure': False,
    'on_failure_callback': on_failure_alert,
    'on_retry_callback': on_retry_alert,
}

@dag(
    dag_id='shopbrasil_catalog_pipeline',
    default_args=default_args,
    description='Pipeline diário de ETL do catálogo de produtos ShopBrasil',
    schedule='0 6 * * *',  # Rodar todos os dias às 06:00 AM (Horário de Brasília)
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=['shopbrasil', 'ecommerce', 'analytics'],
)
def shopbrasil_pipeline():

    @task_group(group_id='ingestao_e_validacao')
    def grupo_ingestao():

        @task(
            retries=3,
            retry_delay=pendulum.duration(seconds=10),
            retry_exponential_backoff=True,
            on_success_callback=on_success_log
        )
        def buscar_produtos() -> list:
            """Busca todos os produtos da API FakeStore com resiliência a oscilações."""
            url = "https://fakestoreapi.com/products"
            try:
                response = requests.get(url, timeout=15)
                response.raise_for_status()
                produtos = response.json()
                logging.info(f"Sucesso ao extrair {len(produtos)} produtos.")
                return produtos
            except Exception as e:
                logging.error(f"Erro na requisição à API: {str(e)}")
                raise e

        # Wrapper em @task para validar o schema
        @task
        def validar_schema(produtos: list) -> list:
            return validar_produtos_func(produtos)

        @task
        def extrair_categorias(produtos: list) -> list:
            """Extrai a lista única de categorias (Topologia Fan-out)."""
            categorias = list(set([prod['category'] for prod in produtos]))
            return categorias

        # Fluxo do grupo de ingestão
        raw_products = buscar_produtos()
        valid_products = validar_schema(raw_products)
        categorias = extrair_categorias(valid_products)

        return valid_products, categorias

    @task_group(group_id='analise_e_metricas')
    def grupo_analise(produtos: list, categorias: list):

        # Requisito: Dynamic Task Mapping (.expand) com limitação via Pool
        @task(pool='ecommerce_pool')
        def calcular_metricas_por_categoria(categoria: str, produtos: list) -> dict:
            """Calcula min, max, média e quantidade por categoria (Processamento em Paralelo)."""
            prods_cat = [p for p in produtos if p['category'] == categoria]
            df = pd.DataFrame(prods_cat)

            metrics = {
                "categoria": categoria,
                "qtd_produtos": int(len(df)),
                "preco_medio": float(df['price'].mean()),
                "preco_min": float(df['price'].min()),
                "preco_max": float(df['price'].max())
            }
            return metrics

        # Fan-out: Mapeia as categorias dinamicamente
        metricas_mapeadas = calcular_metricas_por_categoria.partial(produtos=produtos).expand(categoria=categorias)
        return metricas_mapeadas

    @task_group(group_id='persistencia_dados')
    def grupo_persistencia(metricas: list):

        # Requisito: Topologia Fan-in e inserção Idempotente no Postgres
        @task
        def salvar_banco_dados(metricas_lista: list, execution_date=None):
            """Grava os dados consolidados no PostgreSQL mantendo idempotência."""
            postgres_hook = PostgresHook(postgres_conn_id='postgres_shopbrasil')
            conn = postgres_hook.get_conn()
            cursor = conn.cursor()

            # Garantir existência das tabelas com Chave Composta correta
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS metricas_categoria_snapshot (
                    data_processamento DATE,
                    categoria VARCHAR(100),
                    qtd_produtos INT,
                    preco_medio NUMERIC(10,2),
                    preco_min NUMERIC(10,2),
                    preco_max NUMERIC(10,2),
                    PRIMARY KEY (data_processamento, categoria)
                );
                CREATE TABLE IF NOT EXISTS metricas_categoria_historico (
                    data_execucao TIMESTAMP,
                    categoria VARCHAR(100),
                    qtd_produtos INT,
                    preco_medio NUMERIC(10,2),
                    preco_min NUMERIC(10,2),
                    preco_max NUMERIC(10,2)
                );
            """)

            dt_exec = execution_date if execution_date else pendulum.now("America/Sao_Paulo")
            dt_date = dt_exec.strftime('%Y-%m-%d')

            # Inserção Idempotente (Garante re-execução sem duplicar)
            cursor.execute("DELETE FROM metricas_categoria_snapshot WHERE data_processamento = %s;", (dt_date,))
            
            for m in metricas_lista:
                # Tabela Snapshot
                cursor.execute("""
                    INSERT INTO metricas_categoria_snapshot 
                    (data_processamento, categoria, qtd_produtos, preco_medio, preco_min, preco_max)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """, (dt_date, m['categoria'], m['qtd_produtos'], m['preco_medio'], m['preco_min'], m['preco_max']))

                # Tabela Histórica
                cursor.execute("""
                    INSERT INTO metricas_categoria_historico 
                    (data_execucao, categoria, qtd_produtos, preco_medio, preco_min, preco_max)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """, (dt_exec, m['categoria'], m['qtd_produtos'], m['preco_medio'], m['preco_min'], m['preco_max']))

            conn.commit()
            cursor.close()
            conn.close()
            logging.info("Dados gravados com sucesso e idempotência garantida!")

        salvar_banco_dados(metricas)

    # Fluxo principal de dependências (Linear -> Fan-out -> Fan-in)
    produtos, categorias = grupo_ingestao()
    metricas = grupo_analise(produtos, categorias)
    grupo_persistencia(metricas)

dag_instance = shopbrasil_pipeline()