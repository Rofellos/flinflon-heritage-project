import os
import io
import json
import logging
import pandas as pd
import boto3
import requests
from requests_aws4auth import AWS4Auth

# ------------------------------------------------------------
# Setup
# ------------------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

AWS_REGION = os.environ.get("AWS_REGION", "us-west-1")

DDB_TABLE = os.environ["DDB_TABLE"]
OPENSEARCH_ENDPOINT = os.environ.get("OPENSEARCH_ENDPOINT")
OPENSEARCH_INDEX = os.environ.get("OPENSEARCH_INDEX", "ffhp")

session = boto3.Session()
credentials = session.get_credentials()

awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    AWS_REGION,
    "es",
    session_token=credentials.token,
)

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")
table = ddb.Table(DDB_TABLE)


# ------------------------------------------------------------
# Utility functions
# ------------------------------------------------------------

def as_set(val):
    """Split semicolon-separated values into a list."""
    if pd.isna(val) or str(val).strip() == "":
        return []
    return [v.strip() for v in str(val).split(";") if v.strip()]


def _normalize_row(row):
    """Convert a Pandas row into a clean DynamoDB record."""
    item = {
        "photo_id": str(row.get("photo_id")).zfill(6),
        "snippet": (row.get("snippet") or "").strip(),
        "year": int(row["year"]) if str(row.get("year")).isdigit() else None,
        "location": (row.get("location") or "").strip(),
        "people": as_set(row.get("people")),
        "collection": (row.get("collection") or "").strip(),
        "tags": as_set(row.get("tags")),
        "created_at": (row.get("created_at") or "").strip(),
        "has_derivatives": False,
    }

    # Remove None / empty lists for DynamoDB
    return {k: v for k, v in item.items() if v not in (None, [])}


def _batch_write(items):
    """Write items to DynamoDB using batch_writer."""
    with table.batch_writer(overwrite_by_pkeys=["photo_id"]) as batch:
        for it in items:
            batch.put_item(Item=it)


def _index_opensearch(items):
    """Bulk index items into OpenSearch."""
    if not OPENSEARCH_ENDPOINT:
        return

    ndjson_lines = []
    for it in items:
        # action line
        action = {
            "index": {"_index": OPENSEARCH_INDEX, "_id": it["photo_id"]}
        }
        doc = {
            "photo_id": it.get("photo_id"),
            "snippet": it.get("snippet", ""),
            "tags": it.get("tags", []),
            "year": it.get("year"),
            "location": it.get("location", ""),
            "people": it.get("people", []),
            "collection": it.get("collection", ""),
        }

        ndjson_lines.append(json.dumps(action))
        ndjson_lines.append(json.dumps(doc))

    payload = "\n".join(ndjson_lines) + "\n"

    url = f"{OPENSEARCH_ENDPOINT}/_bulk"
    headers = {"Content-Type": "application/x-ndjson"}

    resp = requests.post(url, data=payload, headers=headers, auth=awsauth, timeout=60)
    resp.raise_for_status()


def _read_s3_as_df(bucket, key):
    """Download XLSX/CSV from S3 and return a pandas DataFrame."""
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()

    if key.lower().endswith(".xlsx"):
        return pd.read_excel(io.BytesIO(body))
    elif key.lower().endswith(".csv"):
        return pd.read_csv(io.BytesIO(body))
    else:
        raise ValueError("Unsupported file type (expected .xlsx or .csv)")


# ------------------------------------------------------------
# Lambda Handler
# ------------------------------------------------------------

def handler(event, context):
    logger.info("Event received: %s", json.dumps(event))

    records = event.get("Records", [])
    all_items = []

    for r in records:
        bucket = r["s3"]["bucket"]["name"]
        key = r["s3"]["object"]["key"]

        logger.info(f"Processing S3 object: s3://{bucket}/{key}")

        df = _read_s3_as_df(bucket, key)
        df.columns = [c.strip().lower() for c in df.columns]

        required = {"photo_id", "snippet"}
        if not required.issubset(df.columns):
            raise ValueError(f"Missing required columns: {required - set(df.columns)}")

        items = [_normalize_row(row) for _, row in df.iterrows()]
        all_items.extend(items)

    # DynamoDB in batches of 100
    for i in range(0, len(all_items), 100):
        _batch_write(all_items[i : i + 100])

    # OpenSearch in batches of 500
    for i in range(0, len(all_items), 500):
        _index_opensearch(all_items[i : i + 500])

    logger.info(f"Imported {len(all_items)} items")

    return {"status": "ok", "count": len(all_items)}
