


from dataclasses import dataclass
from typing import Any
import pandas as pd
import sys 
import time

from src.discounting.exception import CustomException
from src.discounting.logger import logger
from src.discounting.config_manager.config_settings import ConfigurationManager
from src.discounting.components.c_01_data_ingestion import DataIngestion
from src.discounting.data_source.mongo import MongoDBConnection

PIPELINE_NAME= "DATA INGESTION PIPELINE"


@dataclass 
class PipelineData():
    "Represent data passed between pipeline "
    data_ingestion_config: Any
    ingested_data: pd.DataFrame = None 



class DataIngestionPipeline:
    " Will orchestrate the data ingestion pipeline"
    def __init__(self):
        self.config_manager = ConfigurationManager

    def run(self):
        " Execute the data ingestion pipeline"
        try:
            logger.info(f"======== Starting {PIPELINE_NAME} =================")

            # Fetches the config details 
            data_ingestion_config = self.config_manager.get_data_ingestion_config()

            # Creates a DataIngestion object with the fetched config details
            ingested_data  = self.ingested_data(data_ingestion_config)

            logger.info(f"======== {PIPELINE_NAME} completed successfully =================")


        except Exception as e:
            logger.error(f"Error during {PIPELINE_NAME}: {e}")
            raise CustomException(f"Error during {PIPELINE_NAME}: {e}", sys)
        

    def ingested_data(self, config):
        " Method to perform data ingestion and return the ingested data"

        max_retries = 3
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempt {attempt + 1}/ {max_retries}")
                
                # Creates a DataIngestion object with the fetched config details
                data_ingestion = DataIngestion(config=config)
                data_ingestion.import_data_from_mongodb()

                logger.info(f"DataIngestion completed successfully")
                
                # Returns the ingested data
                return data_ingestion

            except Exception as e:
                logger.error(f"Error during data ingestion attempt {attempt+1}/{max_retries}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    logger.info(f"Retrying data ingestion in {10**(attempt+1)} seconds...")
                    time.sleep(10**(attempt+1))

                else:
                    raise CustomException(f"Unable to complete data ingestion after {max_retries} attempts", sys)

        return None
    
if __name__ == "__main__":
    try:
        data_ingestion_pipeline = DataIngestionPipeline()
        data_ingestion_pipeline.run()

    except CustomException as e:
        logger.error(f"Error during data ingestion pipeline: {e}")
        sys.exit(1)
        

    
