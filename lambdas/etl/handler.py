import os
def as_set(val):
if pd.isna(val) or str(val).strip() == "":
return []
return [v.strip() for v in str(val).split(';') if v.strip()]


item = {
'photo_id': str(row.get('photo_id')).zfill(6),
'snippet': (row.get('snippet') or '').strip(),
'year': int(row['year']) if str(row.get('year')).isdigit() else None,
'location': (row.get('location') or '').strip(),
'people': as_set(row.get('people')),
'collection': (row.get('collection') or '').strip(),
'tags': as_set(row.get('tags')),
'created_at': (row.get('created_at') or '').strip(),
'has_derivatives': False
}
# Remove None values for DDB
return {k: v for k, v in item.items() if v not in (None, [])}




def _batch_write(items):
with table.batch_writer(overwrite_by_pkeys=['photo_id']) as batch:
for it in items:
batch.put_item(Item=it)




def _index_opensearch(items):
if not OPENSEARCH_ENDPOINT:
return
# Bulk index NDJSON
ndjson_lines = []
for it in items:
action = {"index": {"_index": OPENSEARCH_INDEX, "_id": it['photo_id']}}
doc = {
'photo_id': it['photo_id'],
'snippet': it.get('snippet', ''),
'tags': it.get('tags', []),
'year': it.get('year'),
'location': it.get('location', ''),
'people': it.get('people', []),
'collection': it.get('collection', ''),
}
ndjson_lines.append(json.dumps(action))
ndjson_lines.append(json.dumps(doc))
payload = "\n".join(ndjson_lines) + "\n"


url = f"{OPENSEARCH_ENDPOINT}/_bulk"
headers = {"Content-Type": "application/x-ndjson"}
resp = requests.post(url, data=payload, headers=headers, auth=awsauth, timeout=60)
resp.raise_for_status()




def _read_s3_object_as_dataframe(bucket, key) -> pd.DataFrame:
obj = s3.get_object(Bucket=bucket, Key=key)
body = obj['Body'].read()
if key.lower().endswith('.xlsx'):
return pd.read_excel(io.BytesIO(body))
elif key.lower().endswith('.csv'):
return pd.read_csv(io.BytesIO(body))
else:
raise ValueError("Unsupported file type. Use .xlsx or .csv")




def handler(event, context):
# S3 Put trigger
records = event.get('Records', [])
all_items = []
for r in records:
b = r['s3']['bucket']['name']
k = r['s3']['object']['key']
df = _read_s3_object_as_dataframe(b, k)
df.columns = [c.strip().lower() for c in df.columns]
required = {'photo_id','snippet'}
if not required.issubset(set(df.columns)):
raise ValueError(f"Missing required columns: {required - set(df.columns)}")
items = [_normalize_row(row) for _, row in df.iterrows()]
all_items.extend(items)


# Write to DDB in batches of 100
for i in range(0, len(all_items), 100):
_batch_write(all_items[i:i+100])
# Index to search
for i in range(0, len(all_items), 500):
_index_opensearch(all_items[i:i+500])


return {"status": "ok", "count": len(all_items)}