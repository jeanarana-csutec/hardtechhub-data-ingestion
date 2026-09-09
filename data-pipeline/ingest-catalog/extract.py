"""
Ingestor: Catalog Service (PostgreSQL) -> S3

Extrae el 100% de los registros de las tablas del catalog-service
(brands, categories, products) y las sube al bucket S3 como CSV,
listas para catalogar con AWS Glue y consultar con Athena.
"""

import io
import json
import logging
import os
import time
from datetime import datetime, timezone

import boto3
import pandas as pd
import psycopg2
from botocore.exceptions import BotoCoreError, ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("ingest-catalog")

# --- Config (via variables de entorno, con defaults iguales al docker-compose del backend) ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "hardtech_catalog")
POSTGRES_USER = os.getenv("POSTGRES_USER", "hardtech")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "hardtech")

S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL")  # vacio/no seteado = AWS real
S3_BUCKET = os.getenv("S3_BUCKET", "hardtech-datalake")
S3_PREFIX = os.getenv("S3_PREFIX", "db-extracts/catalog")

# Tablas a extraer: 100% de los registros, tal cual están en la BD
TABLES = ["brands", "categories", "products"]

# Si INTERVAL_SECONDS > 0, corre en loop (snapshot periódico).
# Si es 0 (default), corre una sola vez y termina (ideal para "pull único" que pide el enunciado).
INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", "0"))


def get_pg_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def get_s3_client():
    kwargs = dict(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN"),  # requerido en AWS Academy Learner Lab
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )
    if S3_ENDPOINT:  # solo se pasa si estamos usando LocalStack
        kwargs["endpoint_url"] = S3_ENDPOINT
    return boto3.client("s3", **kwargs)


def extract_table(conn, table_name: str) -> pd.DataFrame:
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql(query, conn)

    # Las columnas JSONB (ej. 'specs') llegan como dict de Python.
    # Las serializamos a JSON valido (comillas dobles, true/false) para
    # que Glue/Athena puedan parsearlas correctamente mas adelante.
    for col in df.columns:
        if df[col].apply(lambda v: isinstance(v, (dict, list))).any():
            df[col] = df[col].apply(lambda v: json.dumps(v) if isinstance(v, (dict, list)) else v)

    log.info("Extraidas %d filas de la tabla '%s'", len(df), table_name)
    return df


def upload_csv(s3, df: pd.DataFrame, table_name: str, now: datetime) -> str:
    # specs (JSONB) y otras columnas complejas se serializan como texto JSON dentro del CSV
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)

    key = (
        f"{S3_PREFIX}/{table_name}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"{table_name}_{now.strftime('%H%M%S')}.csv"
    )
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    log.info("Subido -> s3://%s/%s", S3_BUCKET, key)
    return key


def run_once() -> None:
    now = datetime.now(timezone.utc)
    log.info("Iniciando extraccion Catalog Service (PostgreSQL) | db=%s host=%s", POSTGRES_DB, POSTGRES_HOST)

    conn = get_pg_connection()
    s3 = get_s3_client()

    try:
        for table in TABLES:
            df = extract_table(conn, table)
            upload_csv(s3, df, table, now)
        log.info("Extraccion completa. Tablas procesadas: %s", TABLES)
    except (psycopg2.Error, BotoCoreError, ClientError) as exc:
        log.error("Fallo la extraccion: %s", exc)
        raise
    finally:
        conn.close()


def main() -> None:
    if INTERVAL_SECONDS > 0:
        log.info("Modo loop activado, intervalo=%ds", INTERVAL_SECONDS)
        while True:
            run_once()
            time.sleep(INTERVAL_SECONDS)
    else:
        run_once()


if __name__ == "__main__":
    main()
