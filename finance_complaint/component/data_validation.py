import os
import sys
from collections import namedtuple
from typing import List, Dict
from pyspark.sql import DataFrame
from finance_complaint.config.spark_manager import spark_session
from finance_complaint.entity.artifact_entity import DataIngestionArtifact
from finance_complaint.entity.config_entity import (
    DataValidationConfig,
    REFERENCE_DATA_FILE_NAME,
)
from finance_complaint.entity.schema import FinanceDataSchema
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logging as logger
from finance_complaint.constant import TIMESTAMP
from pyspark.sql.functions import lit
from finance_complaint.entity.artifact_entity import DataValidationArtifact
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset, DataQualityPreset
from finance_complaint.cloud_storage import SimpleStorageService

ERROR_MESSAGE = "error"
MissingReport = namedtuple(
    "MissingReport", ["total_row", "missing_row", "missing_percentage"]
)


class DataValidation:
    def __init__(
        self,
        data_validation_config: DataValidationConfig,
        data_ingestion_artifact: DataIngestionArtifact,
        schema=FinanceDataSchema(),
    ):
        try:
            self.data_ingestion_artifact: DataIngestionArtifact = (
                data_ingestion_artifact
            )
            self.data_validation_config = data_validation_config
            self.schema = schema
            self.s3 = SimpleStorageService()
        except Exception as e:
            raise FinanceException(e, sys) from e

    def read_data(self) -> DataFrame:
        try:
            dataframe: DataFrame = spark_session.read.parquet(
                self.data_ingestion_artifact.feature_store_file_path
            )
            logger.info(
                f"Data frame is created using file: {self.data_ingestion_artifact.feature_store_file_path}"
            )
            logger.info(
                f"Number of row: {dataframe.count()} and column: {len(dataframe.columns)}"
            )
            return dataframe
        except Exception as e:
            raise FinanceException(e, sys)

    @staticmethod
    def get_missing_report(
        dataframe: DataFrame,
    ) -> Dict[str, MissingReport]:
        try:
            missing_report: Dict[str, MissingReport] = dict()
            logger.info("Preparing missing reports for each column")
            number_of_row = dataframe.count()

            for column in dataframe.columns:
                missing_row = dataframe.filter(f"{column} is null").count()
                missing_percentage = (missing_row * 100) / number_of_row
                missing_report[column] = MissingReport(
                    total_row=number_of_row,
                    missing_row=missing_row,
                    missing_percentage=missing_percentage,
                )
            logger.info(f"Missing report prepared: {missing_report}")
            return missing_report

        except Exception as e:
            raise FinanceException(e, sys)

    def get_unwanted_and_high_missing_value_columns(
        self, dataframe: DataFrame, threshold: float = 0.2
    ) -> List[str]:
        try:
            missing_report: Dict[str, MissingReport] = self.get_missing_report(
                dataframe=dataframe
            )

            unwanted_column: List[str] = self.schema.unwanted_columns
            for column in missing_report:
                if missing_report[column].missing_percentage > (threshold * 100):
                    unwanted_column.append(column)
                    logger.info(f"Missing report {column}: [{missing_report[column]}]")
            unwanted_column = list(set(unwanted_column))
            return unwanted_column

        except Exception as e:
            raise FinanceException(e, sys)

    def drop_unwanted_columns(self, dataframe: DataFrame) -> DataFrame:
        try:
            unwanted_columns: List = self.get_unwanted_and_high_missing_value_columns(
                dataframe=dataframe,
            )
            logger.info(f"Dropping feature: {','.join(unwanted_columns)}")
            unwanted_dataframe: DataFrame = dataframe.select(unwanted_columns)

            unwanted_dataframe = unwanted_dataframe.withColumn(
                ERROR_MESSAGE, lit("Contains many missing values")
            )

            rejected_dir = os.path.join(
                self.data_validation_config.rejected_data_dir, "missing_data"
            )
            os.makedirs(rejected_dir, exist_ok=True)
            file_path = os.path.join(
                rejected_dir, self.data_validation_config.file_name
            )

            logger.info(f"Writing dropped column into file: [{file_path}]")
            unwanted_dataframe.write.mode("append").parquet(file_path)
            dataframe = dataframe.drop(*unwanted_columns)
            logger.info(f"Remaining number of columns: [{dataframe.columns}]")
            return dataframe
        except Exception as e:
            raise FinanceException(e, sys)

    def is_required_columns_exist(self, dataframe: DataFrame):
        try:
            columns = list(
                filter(lambda x: x in self.schema.required_columns, dataframe.columns)
            )

            if len(columns) != len(self.schema.required_columns):
                raise Exception(
                    f"Required column missing\n\
                 Expected columns: {self.schema.required_columns}\n\
                 Found columns: {columns}\
                 "
                )

        except Exception as e:
            raise FinanceException(e, sys)

    def detect_data_drift(self, cur_dir):
        try:
            os.makedirs(self.data_validation_config.data_drift_dir, exist_ok=True)
            current_data = spark_session.read.parquet(cur_dir)
            current_df = current_data.sample(fraction=0.1, seed=42).toPandas()
            os.makedirs(self.data_validation_config.ref_data_dir, exist_ok=True)
            ref_data_path = os.path.join(
                self.data_validation_config.ref_data_dir, REFERENCE_DATA_FILE_NAME
            )
            if os.path.exists(ref_data_path):
                logger.info(f"{ref_data_path} exists")
                ref_data = spark_session.read.parquet(ref_data_path)
                ref_df = ref_data.sample(fraction=0.1, seed=42).toPandas()
                report = Report(metrics=[DataQualityPreset(), DataDriftPreset()])
                report.run(reference_data=ref_df, current_data=current_df)
                drift_report = os.path.join(
                    self.data_validation_config.data_drift_dir, "drift_report.html"
                )
                report.save_html(drift_report)
                self.s3.upload_file(
                    s3_key=f"data_drift_report/{TIMESTAMP}_drift_report.html",
                    local_file_path=drift_report,
                    remove=True,
                )
                logger.info("Saved and uploaded drift report to s3")
            else:
                logger.info(f"{ref_data_path} does not exists")
                report = Report(metrics=[DataQualityPreset()])
                report.run(reference_data=None, current_data=current_df)
                quality_report = os.path.join(
                    self.data_validation_config.data_drift_dir, "data_quality.html"
                )
                report.save_html(quality_report)
                self.s3.upload_file(
                    s3_key=f"data_drift_report/{TIMESTAMP}_data_quality_report.html",
                    local_file_path=quality_report,
                )
                logger.info("Saved and uploaded drift report to s3")
        except Exception as e:
            raise FinanceException(e, sys)

    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            logger.info("Initiating data preprocessing.")
            dataframe: DataFrame = self.read_data()

            logger.info("Dropping unwanted columns")
            dataframe = self.drop_unwanted_columns(dataframe=dataframe)

            # validation to ensure that all require column available
            self.is_required_columns_exist(dataframe=dataframe)
            logger.info("Saving preprocessed data.")
            print(f"Row: [{dataframe.count()}] Column: [{len(dataframe.columns)}]")
            print(
                f"Expected Column: {self.schema.required_columns}\nPresent Columns: {dataframe.columns}"
            )
            os.makedirs(self.data_validation_config.accepted_data_dir, exist_ok=True)
            accepted_file_path = os.path.join(
                self.data_validation_config.accepted_data_dir,
                self.data_validation_config.file_name,
            )
            dataframe.write.parquet(accepted_file_path)
            self.detect_data_drift(cur_dir=accepted_file_path)
            artifact = DataValidationArtifact(
                accepted_file_path=accepted_file_path,
                rejected_dir=self.data_validation_config.rejected_data_dir,
                data_drift_dir=self.data_validation_config.data_drift_dir,
            )
            logger.info(f"Data validation artifact: [{artifact}]")
            return artifact
        except Exception as e:
            raise FinanceException(e, sys)
