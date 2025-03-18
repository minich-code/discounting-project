
from pathlib import Path
from dataclasses import dataclass

# -------Data Ingestion ------------
@dataclass
class DataIngestionConfig:
    root_dir: Path
    database_name: str
    collection_name: str
    batch_size: int
    mongo_uri: str


# -------Data Validation -----
@dataclass
class DataValidationConfig:
    root_dir: str
    data_dir: str
    val_status: str
    all_schema: dict
    validated_data: str  
