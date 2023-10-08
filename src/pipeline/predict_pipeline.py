
import os

import sys
import pandas as pd

from src.exception import CustomException

from src.utils import load_object
from src.logger import logging

from src.ml.transformation import Transform



class PredictPipeline:

    def __init__(self):
        pass


    def predict(self,features):

        try:

            model_path=os.path.join("artifacts",'model.pkl')

            preprocessor_path=os.path.join('artifacts','preprocessor.pkl')

            logging.info("Before Loading")

            model=load_object(file_path=model_path)

            preprocessor=load_object(file_path=preprocessor_path)

            logging.info("After Loading")

            transformer1 = Transform(features)


            logging.info("Preparing columns")


            transformer1.transform_columns()
            
            logging.info("Transformer1 transformations applied and this is the shape %s and the columns %s",features.shape,features.columns)
            preprocessor.fit(
                features.reindex(columns=['POSITION', 'PROMOTION', 'ASSIGNMENTs', 'SALARY', 'SENIORITY_YEARS', 'GENDER', 'ABSENCES'])
                )
            logging.info('fit method of preprocessor object is done ')
            
            data=preprocessor.transform(
                features.reindex(columns=['POSITION', 'PROMOTION', 'ASSIGNMENTs', 'SALARY', 'SENIORITY_YEARS', 'GENDER', 'ABSENCES'])
            )
            logging.info('transform method of preprocessor object is done')
            preds=model.predict(data)

            logging.info('predict method of the model object is done')
            return preds

        except Exception as e:
            logging.info('something occured in predict function in the pipeline')
            raise CustomException(e,sys) from e


class CustomData:
    def __init__(  self,
        gender,
        promotion,
        assignments,
        salary,
        seniority,
        position,
        absences):

        self.gender = gender
        self.promotion = promotion
        self.assignments = assignments
        self.salary = salary
        self.seniority = seniority
        self.position = position
        self.absences = absences


    def get_data_as_data_frame(self):

        try:

            custom_data_input_dict = {

                "GENDER": [self.gender],

                "PROMOTION": [self.promotion],

                "ASSIGNMENTs": [self.assignments],

                "SALARY": [self.salary],

                "SENIORITY_YEARS": [self.seniority],

                "POSITION": [self.position],

                "ABSENCES": [self.absences],

            }

            return pd.DataFrame(custom_data_input_dict)

        except Exception as e:
            logging.info(" A problem occured when using the get data function ")
            raise CustomException(e, sys) from e

