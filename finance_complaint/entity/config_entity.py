import os
import sys
from dataclasses import dataclass
from datetime import datetime
from .metadata_entity import DataIngestionMetadata
from finance_complaint.exception import FinanceException
from finance_complaint.constant import TIMESTAMP
from finance_complaint.constant import BUCKET_NAME
from finance_complaint.constant import S3_MODEL_DIR_KEY
from finance_complaint.constant import (
    ARCHIVE_DIR,
    INPUT_DIR,
    FAILED_DIR,
    PREDICTION_DIR,
    REGION_NAME,
)

# Dataingestion constants
DATA_INGESTION_DIR = "data_ingestion"
DATA_INGESTION_DOWNLOADED_DATA_DIR = "downloaded_files"
DATA_INGESTION_FILE_NAME = "finance_complaint"
DATA_INGESTION_FEATURE_STORE_DIR = "feature_store"
DATA_INGESTION_FAILED_DIR = "failed_downloaded_files"
DATA_INGESTION_METADATA_FILE_NAME = "meta_info.yaml"
DATA_INGESTION_MIN_START_DATE = "2021-01-01"
DATA_INGESTION_DATA_SOURCE_URL = (
    "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
    "?date_received_max=<todate>&date_received_min=<fromdate>"
    "&field=all&format=json"
)


DATA_VALIDATION_DIR = "data_validation"
DATA_VALIDATION_FILE_NAME = "finance_complaint"
DATA_VALIDATION_ACCEPTED_DATA_DIR = "accepted_data"
DATA_VALIDATION_REJECTED_DATA_DIR = "rejected_data"
DATA_VALIDATION_DRIFT_DIR = "dirft_dir"
REFERENCE_DATA_DIR = "ref_data"
REFERENCE_DATA_FILE_NAME = "ref_data.parquet"


DATA_TRANSFORMATION_DIR = "data_transformation"
DATA_TRANSFORMATION_PIPELINE_DIR = "transformed_pipeline"
DATA_TRANSFORMATION_TRAIN_DIR = "train"
DATA_TRANSFORMATION_FILE_NAME = "finance_complaint"
DATA_TRANSFORMATION_TEST_DIR = "test"
DATA_TRANSFORMATION_TEST_SIZE = 0.3

MODEL_TRAINER_BASE_ACCURACY = 0.6
MODEL_TRAINER_DIR = "model_trainer"
MODEL_TRAINER_TRAINED_MODEL_DIR = "trained_model"
MODEL_TRAINER_MODEL_NAME = "finance_estimator"
MODEL_TRAINER_LABEL_INDEXER_DIR = "label_indexer"
MODEL_TRAINER_MODEL_METRIC_NAMES = [
    "f1",
    "weightedPrecision",
    "weightedRecall",
    "weightedTruePositiveRate",
    "weightedFalsePositiveRate",
    "weightedFMeasure",
    "truePositiveRateByLabel",
    "falsePositiveRateByLabel",
    "precisionByLabel",
    "recallByLabel",
    "fMeasureByLabel",
]


MODEL_EVALUATION_DIR = "model_evaluation"
MODEL_EVALUATION_REPORT_DIR = "report"
MODEL_EVALUATION_REPORT_FILE_NAME = "evaluation_report"
MODEL_EVALUATION_THRESHOLD_VALUE = 0.002
MODEL_EVALUATION_METRIC_NAMES = [
    "f1",
]


BUCKET_NAME = BUCKET_NAME
MODEL_PUSHER_MODEL_NAME = "finance-complaint-estimator"


# training pipeline config
@dataclass
class TrainingPipelineConfig:
    pipeline_name: str = "artifact"
    artifact_dir: str = os.path.join(pipeline_name, TIMESTAMP)


# Data Ingestion Config
class DataIngestionConfig:
    def __init__(
        self,
        training_pipeline_config: TrainingPipelineConfig,
        from_date=DATA_INGESTION_MIN_START_DATE,
        to_date=None,
    ):
        try:
            self.from_date = from_date
            min_start_date = datetime.strptime(
                DATA_INGESTION_MIN_START_DATE, "%Y-%m-%d"
            )
            from_date_obj = datetime.strptime(from_date, "%Y-%m-%d")

            if from_date_obj < min_start_date:
                self.from_date = DATA_INGESTION_MIN_START_DATE

            if to_date is None:
                self.to_date = datetime.now().strftime("%Y-%m-%d")

            data_ingestion_master_dir = os.path.join(
                os.path.dirname(training_pipeline_config.artifact_dir),
                DATA_INGESTION_DIR,
            )
            self.data_ingestion_dir = os.path.join(data_ingestion_master_dir, TIMESTAMP)
            self.metadata_file_path = os.path.join(
                data_ingestion_master_dir, DATA_INGESTION_METADATA_FILE_NAME
            )

            data_ingestion_metadata = DataIngestionMetadata(
                metadata_file_path=self.metadata_file_path
            )
            if data_ingestion_metadata.is_metadata_file_present:
                metadata_info = data_ingestion_metadata.get_metadata_info()
                self.from_date = metadata_info.to_date

            self.download_dir = os.path.join(
                self.data_ingestion_dir, DATA_INGESTION_DOWNLOADED_DATA_DIR
            )
            self.failed_dir = os.path.join(
                self.data_ingestion_dir, DATA_INGESTION_FAILED_DIR
            )
            self.file_name = DATA_INGESTION_FILE_NAME
            self.feature_store_dir = os.path.join(
                data_ingestion_master_dir, DATA_INGESTION_FEATURE_STORE_DIR
            )
            self.datasource_url = DATA_INGESTION_DATA_SOURCE_URL
        except Exception as e:
            raise FinanceException(e, sys)


class DataValidationConfig:
    def __init__(self, training_pipeline_config: TrainingPipelineConfig) -> None:
        try:
            data_validation_dir = os.path.join(
                training_pipeline_config.artifact_dir, DATA_VALIDATION_DIR
            )
            self.accepted_data_dir = os.path.join(
                data_validation_dir, DATA_VALIDATION_ACCEPTED_DATA_DIR
            )
            self.rejected_data_dir = os.path.join(
                data_validation_dir, DATA_VALIDATION_REJECTED_DATA_DIR
            )
            self.file_name = DATA_VALIDATION_FILE_NAME
            self.ref_data_dir = os.path.join(
                os.path.dirname(training_pipeline_config.artifact_dir),
                REFERENCE_DATA_DIR,
            )
            self.data_drift_dir = os.path.join(
                data_validation_dir, DATA_VALIDATION_DRIFT_DIR
            )
        except Exception as e:
            raise FinanceException(e, sys)


class DataTransformationConfig:
    def __init__(self, training_pipeline_config: TrainingPipelineConfig) -> None:
        try:
            data_transformation_dir = os.path.join(
                training_pipeline_config.artifact_dir, DATA_TRANSFORMATION_DIR
            )
            self.transformed_train_dir = os.path.join(
                data_transformation_dir, DATA_TRANSFORMATION_TRAIN_DIR
            )
            self.transformed_test_dir = os.path.join(
                data_transformation_dir, DATA_TRANSFORMATION_TEST_DIR
            )
            self.export_pipeline_dir = os.path.join(
                data_transformation_dir, DATA_TRANSFORMATION_PIPELINE_DIR
            )
            self.file_name = DATA_TRANSFORMATION_FILE_NAME
            self.test_size = DATA_TRANSFORMATION_TEST_SIZE
        except Exception as e:
            raise FinanceException(e, sys)


class ModelTrainerConfig:
    def __init__(self, training_pipeline_config: TrainingPipelineConfig) -> None:
        try:
            model_trainer_dir = os.path.join(
                training_pipeline_config.artifact_dir, MODEL_TRAINER_DIR
            )
            self.trained_model_file_path = os.path.join(
                model_trainer_dir,
                MODEL_TRAINER_TRAINED_MODEL_DIR,
                MODEL_TRAINER_MODEL_NAME,
            )
            self.label_indexer_model_dir = os.path.join(
                model_trainer_dir, MODEL_TRAINER_LABEL_INDEXER_DIR
            )
            self.base_accuracy = MODEL_TRAINER_BASE_ACCURACY
            self.metric_list = MODEL_TRAINER_MODEL_METRIC_NAMES
        except Exception as e:
            raise FinanceException(e, sys)


class ModelEvaluationConfig:
    def __init__(self, training_pipeline_config: TrainingPipelineConfig) -> None:
        try:
            model_evaluation_dir = os.path.join(
                training_pipeline_config.artifact_dir, MODEL_EVALUATION_DIR
            )
            self.model_evaluation_report_file_path = os.path.join(
                model_evaluation_dir,
                MODEL_EVALUATION_REPORT_DIR,
                MODEL_EVALUATION_REPORT_FILE_NAME,
            )
            self.threshold = MODEL_EVALUATION_THRESHOLD_VALUE
            self.metric_list = MODEL_EVALUATION_METRIC_NAMES
            self.model_dir = S3_MODEL_DIR_KEY
            self.bucket_name = BUCKET_NAME
        except Exception as e:
            raise FinanceException(e, sys)


class ModelPusherConfig:
    def __init__(
        self,
    ):
        self.model_dir = S3_MODEL_DIR_KEY
        self.bucket_name = BUCKET_NAME


class PredictionPipelineConfig:
    def __init__(
        self,
        input_dir=INPUT_DIR,
        prediction_dir=PREDICTION_DIR,
        failed_dir=FAILED_DIR,
        archive_dir=ARCHIVE_DIR,
        region_name=REGION_NAME,
    ):
        self.input_dir = input_dir
        self.prediction_dir = prediction_dir
        self.failed_dir = failed_dir
        self.archive_dir = archive_dir
        self.region_name = region_name

    def to_dict(self):
        return self.__dict__
