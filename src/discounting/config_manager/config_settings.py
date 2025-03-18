
import sys
import os
from dotenv import load_dotenv

# Custom modules
from src.discounting.exception import CustomException
from src.discounting.logger import logger
from src.discounting.constants import * #(DATA_INGESTION_CONFIG_FILEPATH, DATA_VALIDATION_CONFIG_FILEPATH)
from src.discounting.utils.commons import read_yaml, create_directories

from src.discounting.config_entity.config_params import *# DataIngestionConfig, DataValidationConfig
load_dotenv()



class ConfigurationManager:
    def __init__(
            self, 
            data_ingestion_config: str = DATA_INGESTION_CONFIG_FILEPATH,
            config_filepath: str = DATA_VALIDATION_CONFIG_FILEPATH,
            ):
        
        
        
        
        
        try:
            logger.info(f"Initializing ConfigurationManager with config file")
            
            self.ingestion_config = read_yaml(data_ingestion_config)
            self.config = read_yaml(config_filepath)
            
            
            
            create_directories([self.ingestion_config['artifacts_root']])
            create_directories([self.config['artifact_root']])
            
            
            
            
            
            
            logger.info("Configuration directories created successfully.")
        except Exception as e:
            logger.error(f"Error initializing ConfigurationManager: {e}")
            raise CustomException(e, sys)
    
    def get_data_ingestion_config(self) -> DataIngestionConfig:
        try:
            data_config = self.ingestion_config['data_ingestion']
            create_directories([data_config['root_dir']])
            logger.info(f"Data ingestion configuration loaded from: {DATA_INGESTION_CONFIG_FILEPATH}")
            mongo_uri = os.environ.get('MONGO_URI')
            
            return DataIngestionConfig(
                root_dir=data_config['root_dir'],
                database_name=data_config['database_name'],
                collection_name=data_config['collection_name'],
                batch_size=data_config['batch_size'],
                mongo_uri=mongo_uri
            )
        except Exception as e:
            logger.error(f"Error loading data ingestion configuration: {e}")
            raise CustomException(e, sys)

## Data Validation object 
    def get_data_validation_config(self) -> DataValidationConfig:
        try:
            config = self.config['data_validation']
            create_directories([config['root_dir']])

            data_validation_config = DataValidationConfig(
                root_dir=config['root_dir'],
                data_dir=config['data_dir'],
                val_status=config['val_status'],
                all_schema=config['all_schema'],
                validated_data=config['validated_data']
            )
            return data_validation_config
        except Exception as e:
            logger.exception(f"Error getting Data Validation config: {e}")
            raise CustomException(e, sys)
