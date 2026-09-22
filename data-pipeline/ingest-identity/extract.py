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

log = logging.getLogger("ingest-identity")



MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "hardtech_identity")
MONGO_USERS_COLLECTION = os.getenv("MONGO_USERS_COLLECTION", "users")

S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL")
S3_BUCKET = os.getenv("S3_BUCKET", "hardtech-datalake")
S3_PREFIX = os.getenv("S3_PREFIX", "db-extracts/identity")

INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", "0"))


def get_mongo_client():
    if not MONGO_URI:
        raise RuntimeError("MONGO_URI no esta configurado")

    return MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        socketTimeoutMS=5000,
    )


def get_s3_client():
    kwargs = {
        "region_name": os.getenv(
            "AWS_DEFAULT_REGION",
            "us-east-1"
        )
    }

    if S3_ENDPOINT:
        kwargs["endpoint_url"] = S3_ENDPOINT

    return boto3.client("s3", **kwargs)


def extract_users(collection):
    users = list(
        collection.find(
            {},
            {
                "_id": 0,
                "password_hash": 0,
            }
        )
    )

    log.info(
        "Extraidos %d usuarios de MongoDB",
        len(users)
    )

    return users


def upload_json(s3, users, now):
    lines = [
        json.dumps(
            user,
            ensure_ascii=False,
            default=str
        )
        for user in users
    ]

    body = "\n".join(lines)

    key = (
        f"{S3_PREFIX}/users/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"users_{now.strftime('%H%M%S')}.json"
    )

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )

    log.info(
        "Subido -> s3://%s/%s",
        S3_BUCKET,
        key
    )

    return key


def run_once():
    now = datetime.now(timezone.utc)

    log.info(
        "Iniciando extraccion Identity Service "
        "(MongoDB) | db=%s collection=%s",
        MONGO_DB,
        MONGO_USERS_COLLECTION,
    )

    mongo_client = get_mongo_client()
    s3 = get_s3_client()

    try:
        mongo_client.admin.command("ping")

        db = mongo_client[MONGO_DB]
        collection = db[MONGO_USERS_COLLECTION]

        users = extract_users(collection)

        upload_json(
            s3,
            users,
            now
        )

        log.info("Extraccion completa.")

    except (
        PyMongoError,
        BotoCoreError,
        ClientError
    ) as exc:

        log.error(
            "Fallo la extraccion: %s",
            exc
        )

        raise

    finally:
        mongo_client.close()


def main():
    if INTERVAL_SECONDS > 0:

        log.info(
            "Modo loop activado, intervalo=%ds",
            INTERVAL_SECONDS
        )

        while True:
            run_once()
            time.sleep(INTERVAL_SECONDS)

    else:
        run_once()


if __name__ == "__main__":
    main()
