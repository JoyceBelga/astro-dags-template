from __future__ import annotations

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import timedelta
import pendulum
import pandas as pd
import requests

# =========================
# SUA CONFIG
# =========================
GCP_PROJECT = "bigquery-sandbox-470814"   # seu projeto no BigQuery
BQ_DATASET  = "crypto"                    # seu dataset p/ cripto
BQ_TABLE    = "btc_usd_daily_6m"          # tabela destino
BQ_LOCATION = "US"                        # localização do dataset
GCP_CONN_ID = "google_cloud_default"      # conexão GCP no Airflow

DEFAULT_ARGS = {"email_on_failure": False, "owner": "Joyce - crypto ETL"}

# =========================
# Helpers
# =========================
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "btc-daily-airflow-etl/1.0 (contato: voce@exemplo.gov.br)"})


def _build_coingecko_url_last_6m() -> str:
    """
    CoinGecko: preços históricos diários (aprox. 180 dias)
    GET /api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=180&interval=daily
    Retorna:
      {
        "prices": [[ts_ms, price], ...],
        "market_caps": [...],
        "total_volumes": [...]
      }
    """
    return (
        "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
        "?vs_currency=usd&days=180&interval=daily"
    )


def _http_get_json(url: str) -> dict:
    r = SESSION.get(url, timeout=30)
    if r.status_code == 404:
        return {}
    r.raise_for_status()
    return r.json()


# =========================
# Task
# =========================
@task(retries=0)
def fetch_last_6m_and_to_bq():
    url = _build_coingecko_url_last_6m()
    data = _http_get_json(url)
    prices = data.get("prices", [])
    if not prices:
        print("Nenhum dado retornado pela API.")
        return

    # prices: [[ts_ms, price], ...]
    df = pd.DataFrame(prices, columns=["ts_ms", "price_usd"])

    # TIPOS CORRETOS ↓
    df["price_usd"]  = pd.to_numeric(df["price_usd"], errors="coerce")
    df["quote_date"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True).dt.date  # DATE puro

    # janela (~180 dias até ontem)
    end_day   = (pendulum.now("UTC") - timedelta(days=1)).date()
    start_day = end_day - timedelta(days=179)

    # TIMESTAMP real (não string)
    etl_time = pd.Timestamp.now(tz="UTC")  # datetime64[ns, UTC]

    # monta dataframe final SÓ com as colunas do schema
    df = df.assign(
        time=etl_time,          # TIMESTAMP
        win_start=start_day,    # DATE
        win_end=end_day,        # DATE
        asset="BTC-USD",
        source="CoinGecko",
    )[["quote_date", "price_usd", "time", "win_start", "win_end", "asset", "source"]]

    # envia ao BigQuery
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
    df.to_gbq(
        destination_table=f"{BQ_DATASET}.{BQ_TABLE}",
        project_id=GCP_PROJECT,
        if_exists="append",
        credentials=hook.get_credentials(),
        table_schema=[
            {"name": "quote_date", "type": "DATE"},
            {"name": "price_usd",  "type": "FLOAT"},
            {"name": "time",       "type": "TIMESTAMP"},
            {"name": "win_start",  "type": "DATE"},
            {"name": "win_end",    "type": "DATE"},
            {"name": "asset",      "type": "STRING"},
            {"name": "source",     "type": "STRING"},
        ],
        location=BQ_LOCATION,
        progress_bar=False,
    )

    print(f"[OK] Inseridos {len(df)} dias em {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE} ({start_day} → {end_day})")

# =========================
# DAG (uma execução única, estilo exercício)
# =========================
@dag(
    dag_id="btc_daily_last_6m_to_bq",
    default_args=DEFAULT_ARGS,
    schedule="@once",  # executa uma vez ao dar Trigger
    start_date=pendulum.datetime(2025, 9, 27, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["bitcoin", "crypto", "bigquery", "etl", "6m"],
)
def btc_daily_pipeline_once():
    fetch_last_6m_and_to_bq()

dag = btc_daily_pipeline_once()
