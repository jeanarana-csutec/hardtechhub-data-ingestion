

import json
import logging
import os
import time
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("ingest-identity")

# --- Config ---
DYNAMODB_ENDPOINT = os.getenv("DYNAMODB_ENDPOINT")
USERS_TABLE = os.getenv("USERS_TABLE", "users")

S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL")  # vacio/no seteado = AWS real
S3_BUCKET = os.getenv("S3_BUCKET", "hardtech-datalake")
S3_PREFIX = os.getenv("S3_PREFIX", "db-extracts/identity")

# Igual que en ingest-catalog: 0 = corre una sola vez (pull unico), >0 = loop periodico
INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", "0"))


def get_dynamodb_resource():
    kwargs = dict(
        region_name=os.getenv("DYNAMODB_REGION", "us-east-1"),
    )
    if os.getenv("AWS_ACCESS_KEY_ID"):
        kwargs["aws_access_key_id"] = os.getenv("AWS_ACCESS_KEY_ID")
        kwargs["aws_secret_access_key"] = os.getenv("AWS_SECRET_ACCESS_KEY")
        if os.getenv("AWS_SESSION_TOKEN"):
            kwargs["aws_session_token"] = os.getenv("AWS_SESSION_TOKEN")
    if DYNAMODB_ENDPOINT:
        kwargs["endpoint_url"] = DYNAMODB_ENDPOINT
    return boto3.resource("dynamodb", **kwargs)
def get_s3_client():
    kwargs = dict(
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )
    if os.getenv("AWS_ACCESS_KEY_ID"):
        kwargs["aws_access_key_id"] = os.getenv("AWS_ACCESS_KEY_ID")
        kwargs["aws_secret_access_key"] = os.getenv("AWS_SECRET_ACCESS_KEY")
        if os.getenv("AWS_SESSION_TOKEN"):
            kwargs["aws_session_token"] = os.getenv("AWS_SESSION_TOKEN")
    if S3_ENDPOINT:
        kwargs["endpoint_url"] = S3_ENDPOINT
    return boto3.client("s3", **kwargs)

def decimal_default(obj):
    """DynamoDB devuelve numeros como Decimal; json.dumps no sabe serializarlos."""
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def scan_table(table) -> list:
    """Escanea el 100% de los items de la tabla, paginando si hace falta."""
    items = []
    response = table.scan()
    items.extend(response.get("Items", []))

    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))

    return items


def upload_json(s3, items: list, now: datetime) -> str:
    # Athena/Hive necesitan formato JSON Lines (1 objeto JSON por linea, sin
    # array envolvente ni comas entre objetos) para poder leer el archivo fila por fila.
    lines = [json.dumps(item, ensure_ascii=False, default=decimal_default) for item in items]
    body = "\n".join(lines)

    key = (
        f"{S3_PREFIX}/users/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"users_{now.strftime('%H%M%S')}.json"
    )
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    log.info("Subido -> s3://%s/%s", S3_BUCKET, key)
    return key


def run_once() -> None:
    now = datetime.now(timezone.utc)
    log.info("Iniciando extraccion Identity Service (DynamoDB) | tabla=%s endpoint=%s", USERS_TABLE, DYNAMODB_ENDPOINT)

    dynamodb = get_dynamodb_resource()
    s3 = get_s3_client()
    table = dynamodb.Table(USERS_TABLE)

    try:
        items = scan_table(table)
        log.info("Extraidos %d items de la tabla '%s'", len(items), USERS_TABLE)
        upload_json(s3, items, now)
        log.info("Extraccion completa.")
    except (BotoCoreError, ClientError) as exc:
        log.error("Fallo la extraccion: %s", exc)
        raise


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
