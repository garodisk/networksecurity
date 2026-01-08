from __future__ import annotations

import os
import sys
from typing import Dict, List, Tuple

import pandas as pd
from scipy.stats import ks_2samp

from networksecurity.constant.training_pipeline import SCHEMA_FILE_PATH, TARGET_COLUMN
from networksecurity.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.utils.main_utils.utils import read_yaml_file, write_yaml_file


class DataValidation:
    def __init__(
        self,
        data_ingestion_artifact: DataIngestionArtifact,
        data_validation_config: DataValidationConfig,
        drift_threshold: float = 0.05,
        fail_on_drift: bool = False,
    ):
       
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config

            self._schema_config: Dict = read_yaml_file(SCHEMA_FILE_PATH)

            
            self.expected_columns: List[str] = [
                list(col_dict.keys())[0] for col_dict in self._schema_config.get("columns", [])
            ]

            self.expected_numerical_columns: List[str] = self._schema_config.get("numerical_columns", [])

            self.drift_threshold = drift_threshold
            self.fail_on_drift = fail_on_drift

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    @staticmethod
    def read_data(file_path: str) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

   
    def validate_schema_columns(self, dataframe: pd.DataFrame) -> Tuple[bool, List[str]]:
        """
        Validates:
        - all expected columns exist
        - (optionally) no unexpected extra columns
        """
        try:
            errors: List[str] = []
            df_cols = set(dataframe.columns)
            expected = set(self.expected_columns)

            missing = sorted(list(expected - df_cols))
            extra = sorted(list(df_cols - expected))

            if missing:
                errors.append(f"Missing columns: {missing}")
            if extra:
                # ✅ FIX: don't fail by default, but log. You can change this to be strict if you want.
                logging.warning(f"Extra columns found (not in schema): {extra}")

            is_valid = len(missing) == 0
            return is_valid, errors

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def validate_number_of_columns(self, dataframe: pd.DataFrame) -> bool:
        
        try:
            required_number_of_columns = len(self.expected_columns)
            logging.info(f"Required number of columns: {required_number_of_columns}")
            logging.info(f"Data frame has columns: {len(dataframe.columns)}")
            return len(dataframe.columns) == required_number_of_columns
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def validate_numerical_columns_exist(self, dataframe: pd.DataFrame) -> Tuple[bool, List[str]]:
        """
        This checks presence only (not dtype).
        """
        try:
            errors: List[str] = []
            df_cols = set(dataframe.columns)
            required_num = set(self.expected_numerical_columns)

            missing_num = sorted(list(required_num - df_cols))
            if missing_num:
                errors.append(f"Missing numerical columns: {missing_num}")

            return len(missing_num) == 0, errors
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def detect_dataset_drift(self, base_df: pd.DataFrame, current_df: pd.DataFrame) -> bool:
        """
        Runs KS-test feature-by-feature and writes a YAML drift report.

        
        """
        try:
            status = True
            report: Dict = {}

            compare_cols = [c for c in self.expected_columns if c in base_df.columns and c in current_df.columns]
            if TARGET_COLUMN in compare_cols:
                compare_cols.remove(TARGET_COLUMN)

            for column in compare_cols:
                d1 = pd.to_numeric(base_df[column], errors="coerce").dropna()
                d2 = pd.to_numeric(current_df[column], errors="coerce").dropna()

                # If a column becomes empty after coercion, mark as drift/invalid signal
                if len(d1) == 0 or len(d2) == 0:
                    report[column] = {"p_value": 0.0, "drift_status": True}
                    status = False
                    continue

                ks = ks_2samp(d1, d2)
                p_value = float(ks.pvalue)

                drift_found = p_value < self.drift_threshold
                if drift_found:
                    status = False

                report[column] = {"p_value": p_value, "drift_status": drift_found}

            drift_report_file_path = self.data_validation_config.drift_report_file_path
            os.makedirs(os.path.dirname(drift_report_file_path), exist_ok=True)
            write_yaml_file(file_path=drift_report_file_path, content=report, replace=True)

            return status

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    # -------------------------------------------------------------------------
    # Orchestration
    # -------------------------------------------------------------------------
    def initiate_data_validation(self) -> DataValidationArtifact:
        
        try:
            train_file_path = self.data_ingestion_artifact.trained_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            train_df = self.read_data(train_file_path)
            test_df = self.read_data(test_file_path)

            validation_status = True
            error_messages: List[str] = []

            train_schema_ok, train_schema_errors = self.validate_schema_columns(train_df)
            test_schema_ok, test_schema_errors = self.validate_schema_columns(test_df)

            validation_status = validation_status and train_schema_ok and test_schema_ok
            error_messages.extend(train_schema_errors)
            error_messages.extend(test_schema_errors)

            train_num_ok, train_num_errors = self.validate_numerical_columns_exist(train_df)
            test_num_ok, test_num_errors = self.validate_numerical_columns_exist(test_df)

            validation_status = validation_status and train_num_ok and test_num_ok
            error_messages.extend(train_num_errors)
            error_messages.extend(test_num_errors)

            if not self.validate_number_of_columns(train_df):
                validation_status = False
                error_messages.append("Train dataframe column count does not match schema.")
            if not self.validate_number_of_columns(test_df):
                validation_status = False
                error_messages.append("Test dataframe column count does not match schema.")

            if error_messages:
                logging.error("Data validation errors:\n" + "\n".join(error_messages))

            drift_ok = self.detect_dataset_drift(base_df=train_df, current_df=test_df)

            if not drift_ok:
                logging.warning(
                    f"Dataset drift detected (threshold={self.drift_threshold}). "
                    f"{'Failing validation because fail_on_drift=True.' if self.fail_on_drift else 'Proceeding (warning only).'}"
                )
                if self.fail_on_drift:
                    validation_status = False

            if validation_status:
                out_train_path = self.data_validation_config.valid_train_file_path
                out_test_path = self.data_validation_config.valid_test_file_path
                invalid_train_path = None
                invalid_test_path = None
            else:
                out_train_path = self.data_validation_config.invalid_train_file_path
                out_test_path = self.data_validation_config.invalid_test_file_path
                invalid_train_path = out_train_path
                invalid_test_path = out_test_path

            os.makedirs(os.path.dirname(out_train_path), exist_ok=True)
            train_df.to_csv(out_train_path, index=False, header=True)

            os.makedirs(os.path.dirname(out_test_path), exist_ok=True)
            test_df.to_csv(out_test_path, index=False, header=True)

            data_validation_artifact = DataValidationArtifact(
                validation_status=validation_status,
                valid_train_file_path=self.data_validation_config.valid_train_file_path if validation_status else None,
                valid_test_file_path=self.data_validation_config.valid_test_file_path if validation_status else None,
                invalid_train_file_path=invalid_train_path,
                invalid_test_file_path=invalid_test_path,
                drift_report_file_path=self.data_validation_config.drift_report_file_path,
            )

            logging.info(f"DataValidationArtifact: {data_validation_artifact}")
            return data_validation_artifact

        except Exception as e:
            raise NetworkSecurityException(e, sys)
