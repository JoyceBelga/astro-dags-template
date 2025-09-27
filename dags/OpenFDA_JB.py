from __future__ import annotations

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import date
import pendulum
import pandas as pd
import requests
import urllib.parse

# =========================
# SUA CONFIG
# =========================
GCP_PROJECT = "bigquery-sandbox-470814"       # seu projeto no BigQuery
BQ_DATASET  = "fdadataset"                    # seu dataset
BQ_TABLE    = "openfda_sildenafil_range_test" # tabela alvo
BQ_LOCATION = "US"                            # região do dataset (igual ao dataset)
GCP_CONN_ID = "google_cloud_default"          # conexão GCP no Airflow
DRUG_QUERY  = "sildenafil"                    # patient.drug.medicinalproduct
USE_POOL    = True                            # usar pool p/ limitar concorrência
POOL_NAME   = "openfda_api"                   # nome do pool criado

# Janela fixa para o teste (ajuste as datas que quiser)
TEST_START = date(2025, 6, 1)
TEST_END   = date(2025, 7, 29)

DEFAULT_ARGS = {"email_on_failure": False, "owner": "Joyce - openFDA ETL"}

# =========================
# Helpers
# =========================
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "openfda-airflow-etl/1.0 (contato: voce@exemplo.gov.br)"})

def _yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")

def _build_openfda_url(start_d: date, end_d: date, drug_query: str) -> str:
    """
    Monta a URL do openFDA /drug/event com:
      - filtro por medicamento (medicinalproduct)
      - faixa de datas [YYYYMMDD TO YYYYMMDD]
      - agregação diária (count=receivedate), retornando um histograma por dia
    """
    q = urllib.parse.quote(drug_query)
    start_str = _yyyymmdd(start_d)
    end_str   = _yyyymmdd(end_d)
    return (
        "https://api.fda.gov/drug/event.json"
        f"?search=patient.drug.medicinalproduct:%22{q}%22"
        f"+AND+receivedate:[{start_str}+TO+{end_str}]"
        "&count=receivedate"
    )

def _openfda_get(url: str) -> dict:
    r = SESSION.get(url, timeout=30)
    if r.status_code == 404:
        return {"results": []}
    r.raise_for_status()
    return r.json()

# =========================
# Task
# =========================
_task_kwargs = dict(retries=0)
if USE_POOL:
    _task_kwargs["pool"] = POOL_NAME

@task(**_task_kwargs)
def fetch_fixed_range_and_to_bq():
    """
    Busca o histograma diário (count=receivedate) do openFDA para uma FAIXA FIXA (TEST_START..TEST_END)
    e grava no BigQuery com o schema: time (TS), events (INT), win_start (DATE), win_end (DATE), drug (STRING).
    """
    url = _build_openfda_url(TEST_START, TEST_END, DRUG_QUERY)
    data = _openfda_get(url)
    results = data.get("results", [])

    if not results:
        print("Nenhum resultado retornado para a faixa informada.")
        return

    # results vem como [{"time":"YYYYMMDD","count":N}, ...]
    df = pd.DataFrame(results).rename(columns={"count": "events"})
    # 'time' → TIMESTAMP (será meia-noite UTC do dia)
    df["time"] = pd.to_datetime(df["time"], format="%Y%m%d", utc=True)
    # metadados da faixa e do termo
    df["win_start"] = pd.to_datetime(TEST_START)
    df["win_end"]   = pd.to_datetime(TEST_END)
    df["drug"]      = DRUG_QUERY  # mantém o termo original

    # Envia pro BigQuery
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
    df.to_gbq(
        destination_table=f"{BQ_DATASET}.{BQ_TABLE}",
        project_id=GCP_PROJECT,
        if_exists="append",
        credentials=hook.get_credentials(),
        table_schema=[
            {"name": "time", "type": "TIMESTAMP"},
            {"name": "events", "type": "INTEGER"},
            {"name": "win_start", "type": "DATE"},
            {"name": "win_end", "type": "DATE"},
            {"name": "drug", "type": "STRING"},
        ],
        location=BQ_LOCATION,
        progress_bar=False,
    )
    print(f"[OK] Inseridos {len(df)} dias em {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE} para '{DRUG_QUERY}'.")

# =========================
# DAG (roda uma única vez)
# =========================
@dag(
    dag_id="openfda_jb",
    default_args=DEFAULT_ARGS,
    schedule="@once",  # executa uma vez ao habilitar/triggerar
    start_date=pendulum.datetime(2025, 9, 23, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["openfda", "bigquery", "test", "range"],
)
def openfda_pipeline_test_range():
    fetch_fixed_range_and_to_bq()

dag = openfda_pipeline_test_range()
