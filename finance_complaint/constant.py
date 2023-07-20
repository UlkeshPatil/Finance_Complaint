from datetime import datetime
import os

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

AWS_SECRET_ACCESS_KEY_ENV_KEY = "AWS_SECRET_ACCESS_KEY"

AWS_ACCESS_KEY_ID_ENV_KEY = "AWS_ACCESS_KEY_ID"

REGION_NAME = "ap-south-1"

BUCKET_NAME = "1-finance-complaint-project"

S3_MODEL_DIR_KEY = "model-registry"

MODEL_SAVED_DIR = "saved_models"

MONGO_DB_URL_ENV_KEY = "MONGO_URL"

DATABASE_NAME = "finance-complaint-db"

# Prediction pipeline constants
ROOT_DATA_DIR_NAME = "finance_data"

ARCHIVE_DIR_NAME = "archive"

INPUT_DIR_NAME = "input"

PREDICTION_DIR_NAME = "prediction"

FAILED_DIR_NAME = "failed"

PYSPARK_S3_URL_FORMAT = "s3a://"

INPUT_FILE_NAME = "finance_complaint"

PYSPARK_S3_ROOT = os.path.join(PYSPARK_S3_URL_FORMAT, BUCKET_NAME)

ROOT_DATA_DIR = os.path.join(ROOT_DATA_DIR_NAME)

ARCHIVE_DIR = os.path.join(ROOT_DATA_DIR, ARCHIVE_DIR_NAME, TIMESTAMP)

INPUT_DIR = os.path.join(ROOT_DATA_DIR, INPUT_DIR_NAME, INPUT_FILE_NAME)

PREDICTION_DIR = os.path.join(ROOT_DATA_DIR, PREDICTION_DIR_NAME, TIMESTAMP)

FAILED_DIR = os.path.join(ROOT_DATA_DIR, FAILED_DIR_NAME, TIMESTAMP)
