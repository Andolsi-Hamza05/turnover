""" 
this module is for ingesting data from the data persisted 
in csv format and for splitting it to train validation and test sets 
"""

import os
import sys
import logging
from dataclasses import dataclass
import pandas as pd
from sklearn.model_selection import train_test_split
from src.exception import CustomException
from src.logger import logging

from src.ml.transformation import DataTransformation, DataTransformationConfig
from src.ml.model_trainer import ModelTrainer, ModelTrainerConfig

@dataclass
class DataIngestionConfig:
    """ preparing the paths for artifacts folder """
    train_path: str = os.path.join('artifacts', "train.csv")
    validation_path: str = os.path.join('artifacts', "validation.csv")
    test_path: str = os.path.join('artifacts', "test.csv")
    raw_path: str = os.path.join('artifacts', "data.csv")


class DataIngestion:
    """ this class is responsible for data ingestion task """
    def __init__(self):
        self.ingestion_config = DataIngestionConfig()

    def initiate_data_ingestion(self):
        """ 
        ingest data from the csv file persisted 
        split the data to train,validation and test sets
        """
        logging.info("Ingesting data started")
        try:
            df = pd.read_csv('notebook/data/ML_data/turnover.csv')
            logging.info('The dataset is read successfully')
            os.makedirs(os.path.dirname(
                self.ingestion_config.train_path), exist_ok=True)
            df.to_csv(self.ingestion_config.raw_path, index=False, header=True)
            logging.info("Train and Test split are ready")
            train_set, temp_set = train_test_split(
                df, test_size=0.2, random_state=42)
            validation_set, test_set = train_test_split(
                temp_set, test_size=0.5, random_state=42)
            train_set.to_csv(self.ingestion_config.train_path,
                             index=False, header=True)
            validation_set.to_csv(
                self.ingestion_config.validation_path, index=False, header=True)
            test_set.to_csv(self.ingestion_config.test_path,
                            index=False, header=True)
            logging.info("Training set shape: %s", train_set.shape)
            logging.info("Validation set shape: %s", validation_set.shape)
            logging.info("Test set shape: %s", test_set.shape)
            logging.info("Ingestion is completed")
            return (
                self.ingestion_config.train_path,
                self.ingestion_config.test_path
            )
        except Exception as e:
            raise CustomException(e, sys) from e


if __name__ == "__main__":
    obj = DataIngestion()
    train_data,test_data=obj.initiate_data_ingestion()

    data_transformation=DataTransformation()
    train_arr,test_arr,_=data_transformation.initiate_data_transformation(train_data,test_data)

    modeltrainer=ModelTrainer()
    print(modeltrainer.initiate_model_trainer(train_arr,test_arr))