import json
import logging
import os
from datetime import datetime, timezone

import boto3
from pymongo import MongoClient


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

log = logging.getLogger("ingest-inventory")


# =========================
# MongoDB
# =========================

MONGO_URI = os.getenv("INVENTORY_MONGO_URI")
MONGO_DB = os.getenv(
    "INVENTORY_MONGO_DB",
    "hardtech_inventory"
)


# =========================
# S3
# =========================

S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv(
    "S3_PREFIX",
    "db-extracts/inventory"
)

AWS_REGION = os.getenv(
    "AWS_DEFAULT_REGION",
    "us-east-1"
)


COLLECTIONS = [
    "inventory",
    "inventory_movements",
    "inventory_reservations"
]


def get_mongo_client():
    return MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000
    )


def get_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION
    )


def extract_collection(db, collection_name):
    collection = db[collection_name]

    documents = list(collection.find())

    log.info(
        "Extraidos %d documentos de %s",
        len(documents),
        collection_name
    )

    return documents


def upload_to_s3(
    s3,
    collection_name,
    documents,
    now
):
    lines = []

    for document in documents:

        # Convertimos _id a un valor serializable
        if "_id" in document:
            document["_id"] = str(document["_id"])

        lines.append(
            json.dumps(
                document,
                default=str,
                ensure_ascii=False
            )
        )

    body = "\n".join(lines)

    key = (
        f"{S3_PREFIX}/{collection_name}/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"{collection_name}_{now.strftime('%H%M%S')}.json"
    )

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json"
    )

    log.info(
        "Subido a s3://%s/%s",
        S3_BUCKET,
        key
    )


def main():

    if not MONGO_URI:
        raise RuntimeError(
            "INVENTORY_MONGO_URI no configurado"
        )

    if not S3_BUCKET:
        raise RuntimeError(
            "S3_BUCKET no configurado"
        )

    now = datetime.now(timezone.utc)

    mongo = get_mongo_client()
    s3 = get_s3_client()

    try:

        # Verifica conexión
        mongo.admin.command("ping")

        db = mongo[MONGO_DB]

        for collection_name in COLLECTIONS:

            documents = extract_collection(
                db,
                collection_name
            )

            upload_to_s3(
                s3,
                collection_name,
                documents,
                now
            )

        log.info(
            "Ingesta de Inventory completada"
        )

    finally:
        mongo.close()


if __name__ == "__main__":
    main()