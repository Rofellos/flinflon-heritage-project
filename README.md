# Flin Flon Heritage Project (FFHP) — Web Photo Archive

A fast, searchable archive for ~50,000 photos using AWS (S3, CloudFront, DynamoDB, Lambda) and a Next.js frontend.

## What’s here
- **web/** – Next.js app (gallery + detail, mock APIs you can swap for real backends)
- **lambdas/** –
  - `etl/` Excel/CSV → DynamoDB (+ OpenSearch) importer
  - `thumbs/` Thumbnail generator (thumb.jpg, medium.jpg)
- **infra/** – optional IaC (CDK/Serverless) stubs to add later
- **docs/** – data model notes (DynamoDB table + OpenSearch mappings)

## Quick start (frontend)
```bash
cd web
npm install
# set your CloudFront URL that serves thumbnails/mediums
echo 'NEXT_PUBLIC_DERIV_BASE=https://<your-cloudfront-domain>' > .env.local
npm run dev
```
Open http://localhost:3000 and try searching (mock data included).

## Deploy Lambdas (manual, simple path)
```bash
# ETL
cd lambdas/etl
pip install -r requirements.txt -t ./package
cp handler.py package/
cd package && zip -r ../etl.zip . && cd ..

# Thumbnailer
cd ../thumbs
pip install -r requirements.txt -t ./package
cp handler.py package/
cd package && zip -r ../thumbs.zip . && cd ..
```
Upload the zip files in the AWS Lambda console, set env vars, and add S3 triggers:
- **ETL**: bucket `ffhp-etl` (`.xlsx`/`.csv` uploads)
- **Thumbnailer**: bucket `ffhp-photos-original` (new image uploads)

## S3 layout
```
ffhp-photos-original/000123.jpg
ffhp-photos-derivatives/000123/thumb.jpg
ffhp-photos-derivatives/000123/medium.jpg
ffhp-etl/metadata.xlsx
```

## Tips
- Name files to match `photo_id` (e.g., `000123.jpg`).
- Use semicolon-separated lists for `people` and `tags` in your CSV/Excel.
- GitHub/GitLab will show this README automatically when the file is named **README.md** in the repo root.
