import json
import logging
import os
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from pymongo import MongoClient
from pymongo.errors import PyMongoError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("ingest-inventory")

# --- Config ---
MONGO_URI = os.getenv("INVENTORY_MONGO_URI")
MONGO_DB = os.getenv("INVENTORY_MONGO_DB", "hardtech_inventory")

S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL")
S3_BUCKET = os.getenv("S3_BUCKET", "hardtech-datalake")
S3_PREFIX = os.getenv("S3_PREFIX", "db-extracts/inventory")

COLLECTIONS = [
    "inventory",
    "inventory_movements",
    "inventory_reservations",
]

# 0 = corre una sola vez, >0 = loop periodico
INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", "0"))


def get_mongo_client():
    return MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000
    )


def get_s3_client():
    kwargs = dict(
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )
    if S3_ENDPOINT:
        kwargs["endpoint_url"] = S3_ENDPOINT
    return boto3.client("s3", **kwargs)


def extract_collection(db, collection_name: str):
    documents = list(db[collection_name].find())
    log.info(
        "Extraidos %d documentos de la coleccion '%s'",
        len(documents),
        collection_name,
    )
    return documents


def upload_json(s3, documents, collection_name: str, now: datetime) -> str:
    lines = []

    for document in documents:
        if "_id" in document:
            document["_id"] = str(document["_id"])

        lines.append(
            json.dumps(
                document,
                default=str,
                ensure_ascii=False,
            )
        )

    body = "\n".join(lines)

    key = (
        f"{S3_PREFIX}/{collection_name}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"{collection_name}_{now.strftime('%H%M%S')}.json"
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

    log.info(
        "Iniciando extraccion Inventory Service (MongoDB) | db=%s",
        MONGO_DB,
    )

    mongo = get_mongo_client()
    s3 = get_s3_client()

    try:
        mongo.admin.command("ping")
        db = mongo[MONGO_DB]

        for collection in COLLECTIONS:
            documents = extract_collection(db, collection)
            upload_json(s3, documents, collection, now)

        log.info(
            "Extraccion completa. Colecciones procesadas: %s",
            COLLECTIONS,
        )

    except (PyMongoError, BotoCoreError, ClientError) as exc:
        log.error("Fallo la extraccion: %s", exc)
        raise

    finally:
        mongo.close()


def main() -> None:
    if INTERVAL_SECONDS > 0:
        log.info(
            "Modo loop activado, intervalo=%ds",
            INTERVAL_SECONDS,
        )

        while True:
            run_once()
            time.sleep(INTERVAL_SECONDS)
    else:
        run_once()


if __name__ == "__main__":
    main()
