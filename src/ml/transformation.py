""" this module is for defining the main transformations in the preprocessing workflow """

import sys
from dataclasses import dataclass
import os

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from src.exception import CustomException
from src.logger import logging
from src.utils import save_object


@dataclass
class DataTransformationConfig:
    """ save preprocessor pkl file in artifacts folder """
    preprocessor_obj_file_path = os.path.join('artifacts', "proprocessor.pkl")


class Transform:
    """ this class is for formatting the data train/test/valid sets """

    def __init__(self, df):
        self.df = df

    def transform_columns(self, df):
        """ format the type of columns """
        df[['POSITION']] = df[['POSITION']].astype('category')
        df['TURNOVER'] = df['TURNOVER'].replace({'no': 0, 'yes': 1})
        df[['TURNOVER']] = df[['TURNOVER']].astype('int')

    def map_salary_column(self, df):
        """ map the ordinal categorical column salary 0 for low, 1 for medium and 2 for high """
        df['SALARY'] = df['SALARY'].map(
            {'high': 2, 'medium': 1, 'low': 0}).astype(int)


class DataTransformation:
    """ this class is for creating a standard data pipeline for data transformation """

    def __init__(self):
        self.data_transformation_config = DataTransformationConfig()

    def get_data_transformer_object(self):
        '''
        This function is responsible for data transformation
        '''
        try:
            numerical_columns = ["SENIORITY_YEARS", "ASSIGNMENTs", "ABSENCES"]
            categorical_columns = [
                "POSITION",
                "GENDER",
                "SALARY",
                "PROMOTION"
            ]

            num_pipeline = Pipeline(
                steps=[
                    ("imputer", SimpleImputer(strategy="median")),
                    ("scaler", StandardScaler())

                ]
            )

            cat_pipeline = Pipeline(
                steps=[
                    ("imputer", SimpleImputer(strategy="most_frequent")),
                    ("one_hot_encoder", OneHotEncoder()),
                    ("scaler", StandardScaler(with_mean=False))
                ]

            )

            logging.info("Categorical columns: %s",categorical_columns)
            logging.info("Numerical columns: %s",numerical_columns)

            preprocessor = ColumnTransformer(
                [
                    ("num_pipeline", num_pipeline, numerical_columns),
                    ("cat_pipelines", cat_pipeline, categorical_columns)
                ]
            )
            return preprocessor
        except Exception as e:
            raise CustomException(e, sys) from e

    def initiate_data_transformation(self, train_path, validation_path):
        """ do the transformations on the train and test data """
        try:
            logging.info("Read train and test data completed")
            train_df = pd.read_csv(train_path)
            valid_df = pd.read_csv(validation_path)
            transformer1 = Transform(train_df)
            transformer2 = Transform(valid_df)

            logging.info("Preparing columns")
            transformer1.transform_columns(train_df)
            transformer2.transform_columns(valid_df)

            transformer1.map_salary_column(train_df)
            transformer2.map_salary_column(valid_df)

            logging.info("Obtaining preprocessing object")
            preprocessing_obj = self.get_data_transformer_object()

            target_column_name = "TURNOVER"

            input_feature_train_df = train_df.drop(
                columns=[target_column_name], axis=1)
            target_feature_train_df = train_df[target_column_name]

            input_feature_valid_df = valid_df.drop(
                columns=[target_column_name], axis=1)
            target_feature_valid_df = valid_df[target_column_name]

            logging.info(
                "Applying preprocessing object on training dataframe and testing dataframe."
            )

            input_feature_train_arr = preprocessing_obj.fit_transform(
                input_feature_train_df)
            input_feature_valid_arr = preprocessing_obj.transform(
                input_feature_valid_df)

            train_arr = np.c_[
                input_feature_train_arr, np.array(target_feature_train_df)
            ]
            test_arr = np.c_[input_feature_valid_arr,
                             np.array(target_feature_valid_df)]

            logging.info("Saved preprocessing object.")

            save_object(
                file_path=self.data_transformation_config.preprocessor_obj_file_path,
                obj=preprocessing_obj
            )

            return (
                train_arr,
                test_arr,
                self.data_transformation_config.preprocessor_obj_file_path,
            )
        except Exception as e:
            raise CustomException(e, sys) from e
