# GCP Service Account Credentials

**⚠️ This directory is protected by .gitignore**

## Required File

Place your GCP service account key JSON file here:
- **Filename**: `gcp-service-account-key.json`
- **Location**: This directory

## How to obtain credentials

1. Go to [GCP Console](https://console.cloud.google.com/)
2. Navigate to **IAM & Admin** → **Service Accounts**
3. Create or select a service account with **BigQuery Data Editor** role
4. Create a new key (JSON format)
5. Download and save as `gcp-service-account-key.json` in this directory

## Security Notes

- ✅ This file is automatically ignored by Git (see `.gitignore`)
- ❌ Never commit this file to version control
- ❌ Never share this file publicly
