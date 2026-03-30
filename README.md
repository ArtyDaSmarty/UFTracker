# United Front Technical Database

A Flask web app for tracking alters, locations, affiliations, relation tags, and generated IDs with level-based access.

## Included

- Login and registration with username/password
- First registered account becomes the only Level 4 `admin`
- User levels 1-3, with Level 3 acting as `mod`
- Separate creation permission for non-admin accounts
- Level-based visibility for alters, locations, affiliations, and inquiries
- Admin-only user management and JSON import
- Optional S3-backed storage for all JSON data
- Docker-ready deployment
- Existing tracker data and saved hashes preserved

## Run Locally

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

Open [http://localhost:8000](http://localhost:8000).

## Docker

```powershell
docker build -t uftd .
docker run -p 8000:8000 -e SECRET_KEY=change-me uftd
```

## S3 Storage

Set these environment variables to move all JSON-backed data into S3:

```powershell
$env:STORAGE_BACKEND="s3"
$env:S3_BUCKET="your-bucket-name"
$env:S3_PREFIX="uftd"
```

AWS credentials can be supplied through standard AWS environment variables or IAM role configuration.

## Data Files

- `relationship_data.json`: tracker records
- `saved_hashes.json`: reserved/generated IDs
- `users.json`: login accounts and roles
