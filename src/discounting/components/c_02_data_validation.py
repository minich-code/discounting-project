
import sys
from pathlib import Path
import pandas as pd
import sys
import json
from src.discounting.exception import CustomException  
from src.discounting.logger import logger 
from src.discounting.config_entity.config_params import DataValidationConfig 


class DataValidation:
    def __init__(self, config: DataValidationConfig):
        self.config = config

    def validate_all_columns(self) -> bool:
        try:
            overall_status = True  # Assume valid until proven otherwise
            validation_results = {} #Collects all validation details

            try:
                data = pd.read_parquet(self.config.data_dir)  # Read the Parquet file
            except Exception as e:
                logger.error(f"Error reading Parquet file: {e}")
                raise CustomException(f"Error reading Parquet file: {e}", sys)

            all_cols = list(data.columns)
            all_schema = self.config.all_schema

            for col in all_cols:
                if col not in all_schema:
                    logger.error(f"Column {col} not found in schema")
                    validation_results[col] = "Column missing in schema"
                    overall_status = False #No need to continue, validation failed
                else:
                    validation_results[col] = "Column present in schema"

            #Additional check for column datatypes
            if overall_status:
                for col in all_cols:
                    expected_dtype = str(all_schema[col])
                    actual_dtype = str(data[col].dtype)
                    if expected_dtype != actual_dtype:
                        logger.error(f"Column {col} has incorrect data type: expected {expected_dtype}, got {actual_dtype}")
                        validation_results[col] = f"Incorrect data type: expected {expected_dtype}, got {actual_dtype}"
                        overall_status = False
                    else:
                        validation_results[col] = "Data type valid"

            # Save results to a file
            val_status_path = self.config.val_status
            try:
                with open(val_status_path, 'w') as f:
                    json.dump(validation_results, f, indent=4)
                logger.info(f"Validation results saved to {val_status_path}")
            except Exception as e:
                logger.error(f"Failed to save validation results: {e}")
                raise CustomException(f"Failed to save validation results: {e}", sys)


            root_dir_path = Path(self.config.root_dir)

            # Save the data to a parquet file only if the validation passed
            if overall_status:
                try:
                    output_path = self.config.validated_data
                    data.to_parquet(output_path, index=False) #use to_parquet
                    logger.info(f"Validated data saved to {output_path}")
                except Exception as e:
                    logger.error(f"Failed to save validated data: {e}")
                    raise CustomException(f"Failed to save validated data: {e}", sys)
            else:
                logger.warning(f"Data validation failed. Check {val_status_path} for more details")
                

            return overall_status

        except Exception as e:
            logger.exception(f"Error during validation: {e}")
            raise CustomException(e, sys)

