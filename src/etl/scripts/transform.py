import pandas as pd
import numpy as np
import requests 
from bs4 import BeautifulSoup
import re
from datetime import datetime
from src.logger import logging

#logging.basicConfig(level=logging.INFO)

class Transform:
    def __init__(self, df):
        self.df = df
    
    def remove_column(self, column_names):
        self.df.drop(column_names, axis=1, inplace=True)
    
    def format_column_as_date(self, column_list, date_format='%Y-%m-%d'):
        for column in column_list:
            self.df[column] = pd.to_datetime(
                self.df[column],
                format=date_format,
                infer_datetime_format=True,
                errors='coerce'
            )
    
    def transform_columns_to_type(self, column_list: list, data_type):
        for column in column_list:
            self.df[column] = self.df[column].astype(data_type)
    
    def treat_text_column(self,column_name):
        self.df[column_name] = self.df[column_name].apply(lambda x: re.sub(r'\s+', ' ', x).strip().lower())

    def apply_mapping(self, column_name, mapping_dict):
        def convert_lib(lib):
            for category, word_list in mapping_dict.items():
                for word in word_list:
                    pattern = rf'\b\w*{re.escape(word)}\w*\b'
                    if re.search(pattern, lib, re.IGNORECASE):
                        return category
            return 'Autres'
        
        self.df[column_name] = self.df[column_name].apply(convert_lib)
    
    def remove_out_of_range_date(self,column_name):
        # Get today's date
        today = datetime.now()
        self.df[column_name]=pd.to_datetime(self.df[column_name])
        self.df.loc[self.df[column_name] > pd.Timestamp(today), column_name] = pd.NaT
    
    def delete_rows_by_date_comparison(self, start_date_column, end_date_column):
        mask = self.df[end_date_column] < self.df[start_date_column]
        self.df = self.df[~mask]

    def webscrape_country_code(self):
        # URL of the webpage containing the table
        url = 'https://www.iban.com/country-codes'

        # Send an HTTP request and get the HTML content
        response = requests.get(url,timeout=10)
        html_content = response.content

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')

        # Find the table you want to scrape based on its HTML structure
        table = soup.find('table')

        # Extract data from the table
        table_data = []
        for row in table.find_all('tr'):
            row_data = [cell.get_text(strip=True) for cell in row.find_all(['th', 'td'])]
            table_data.append(row_data)

        # Convert the data into a DataFrame
        columns = table_data[0]  # first row contains column headers
        data = table_data[1:]     # Rest of the rows are data
        data = pd.DataFrame(data, columns=columns)
        return data
    
    def match_country_code_with_lib(self,data,match_column,key_column):
        self.df.rename(columns={match_column: "Alpha-3 code"}, inplace=True)
        merged = data[['Country','Alpha-3 code']].merge(self.df, on="Alpha-3 code", how="left")
        merged.dropna(subset=[key_column], inplace=True)
        self.df=merged
    
    def remove_possible_inaccurent_salaries(self, column_name, lower_percentile=1, upper_percentile=9):
        column_data = self.df[column_name]
        lower_threshold = np.percentile(column_data, lower_percentile)
        upper_threshold = np.percentile(column_data, upper_percentile)
        filtered_df = self.df[(self.df[column_name] >= lower_threshold) & (self.df[column_name] <= upper_threshold)]
        self.df = filtered_df


absence_dict = {
    'Familiale': ['parent', 'enfant', 'maternit', 'paternit', 'parent', 'familial', 'mariage', 'proche','grossesse','parental','naissance'],
    'formation': ['formation', 'apprentissage'],
    'congé non payé': ['non pay', 'sans solde', 'absence', 'non remun', 'longue dur','sabbatique'],
    'autre congé': ['vacances', 'remun', 'annuel', 'payant', 'compens','pay'],
    'maladie': ['maladie', 'sant', 'covid','quarantaine'],
    'accident': ['accident', 'incident']
}

def transform_absence(df):
    logger = logging.getLogger(__name__)
    transformer = Transform(df)
    logger.info("Starting the transform_absence function.")
    try :
        transformer.remove_column('ABSENCE_CODE')
    except Exception as e:
        logger.error("something went wrong when removing columns: %s", e) 
    try : 
        transformer.format_column_as_date(['ABSENCE_END_DATE','ABSENCE_START_DATE'])
        transformer.transform_columns_to_type(['ABSENCE_LIB'],'category')
    except Exception as e:
        logger.error("something went wrong when formatting columns type: %s",e)
    try :
        transformer.treat_text_column('ABSENCE_LIB')
        transformer.apply_mapping('ABSENCE_LIB',absence_dict)
    except Exception as e:
        logger.error("something went wrong when working matching libelles : %s",e)
    try :
        transformer.remove_out_of_range_date('ABSENCE_END_DATE')
        transformer.remove_out_of_range_date('ABSENCE_START_DATE')
        transformer.delete_rows_by_date_comparison('ABSENCE_START_DATE','ABSENCE_END_DATE')
    except Exception as e:
        logger.error("An error occurred during removing inconsistent data: %s",e)

    logger.info("Ending the transform_absence function.")
    return df


# create a mapping dictionnary to collapse categories : 
assignment_dict = {
    'Directeurs': ['directeur','ponsable','chef','direction','general','général'],
    'Secrétariat général': ['assistant','secrétaire','secretaire'],
    'Production': ['logistique','machin'],
    'Finance': ['paie','comptabilit','financier','de gestion','finance'],
    'Ressources Humaines': ['rh','ressource','humaine','recrutement','entretien'],
    'Commercial': ['commercial','vente'],
    'Administration': ['admini'],
    'Analyste': ['analyste'],
    'Ingénieur': ['ing','info','test'],
    'technicien': ['techni']
}

def transform_assignment(df):
    logger = logging.getLogger(__name__)
    transformer = Transform(df)
    logger.info("Starting the transform_assignment function.")
    try :
        transformer.remove_column(['ASSIGNMENT_ORGUNIT_CODE','ASSIGNMENT_OFFICE_CODE','ASSIGNMENT_JOB_CODE'])
    except Exception as e:
        logger.error("something went wrong when removing columns: %s",e)
    try :
        transformer.format_column_as_date(['ASSIGNMENT_START_DATE','ASSIGNMENT_END_DATE'])
        transformer.transform_columns_to_type(['ASSIGNMENT_JOB_LIB'],'category')
    except Exception as e:
        logger.error("something went wrong when formatting columns type: %s",e)
    try :
        transformer.treat_text_column('ASSIGNMENT_JOB_LIB')
        transformer.apply_mapping('ASSIGNMENT_JOB_LIB',assignment_dict)
    except Exception as e:
        logger.error("something went wrong when working matching libelles : %s",e)
    try :
        transformer.remove_out_of_range_date('ASSIGNMENT_START_DATE')
        transformer.remove_out_of_range_date('ASSIGNMENT_END_DATE')
        transformer.delete_rows_by_date_comparison('ASSIGNMENT_START_DATE','ASSIGNMENT_END_DATE')
    except Exception as e:
        logger.error("An error occurred during removing inconsistent data: %s",e)
    logger.info("Ending the transform_assignment function.")
    return df

def transform_employee(df):
    logger = logging.getLogger(__name__)
    transformer = Transform(df)
    logger.info("Starting the transform_employee function.")
    try :
        transformer.remove_column(['EMPLOYEE','EMP_MATRICULE','EMP_TYPE','EMP_CODE1','EMP_CODE2'])
    except Exception as e:
        logger.error("something went wrong when removing columns: %s",e)
    try : 
        transformer.format_column_as_date(['EMP_SENIORITY_DATE','EMP_BIRTH_DATE'])
        transformer.transform_columns_to_type(['EMP_GENDER_CODE'],'category')
        transformer.transform_columns_to_type(['EMP_LASTNAME','EMP_FIRSTNAME'],'string')
    except Exception as e:
        logger.error("something went wrong when formatting columns type: %s",e)
    try :    
        data=transformer.webscrape_country_code()
        transformer.match_country_code_with_lib(data,'EMP_COUNTRYOFBIRTH_CODE','EMPLOYEE_KEY')
    except Exception as e:
        logger.error("something went wrong when working matching country libelles : %s",e)
    transformer.treat_text_column('EMP_FIRSTNAME')
    transformer.treat_text_column('EMP_LASTNAME')
    try :
        transformer.remove_out_of_range_date('EMP_SENIORITY_DATE')
        transformer.remove_out_of_range_date('EMP_BIRTH_DATE')
        transformer.delete_rows_by_date_comparison('EMP_BIRTH_DATE','EMP_SENIORITY_DATE')
        transformer.remove_possible_inaccurent_salaries('EMP_SALARY')
    except Exception as e:
        logger.error("An error occurred during removing inconsistent data: %s",e)
    logger.info("Ending the transform_employee function.")
    return df

def transform_contract(df):
    logger = logging.getLogger(__name__)
    transformer = Transform(df)
    logger.info("Starting the transform_contract function.")
    try :
        transformer.remove_column(['CONTRACT_CODE','CONTRACT_QUALIF_CODE','CONTRACT_TYPE_CODE','NUDOSS','DATE_DEBUT_CONTRAT'])
    except Exception as e:
        logger.error("something went wrong when removing columns: %s",e)
    try : 
        transformer.format_column_as_date(['CONTRACT_START_DATE','CONTRACT_END_DATE'])
        transformer.transform_columns_to_type(['QUALIFDESCRIPTION'],'category')
    except Exception as e:
        logger.error("something went wrong when formatting columns type: %s", e)   
    try :
        transformer.treat_text_column('QUALIFDESCRIPTION')
        transformer.apply_mapping('ASSIGNMENT_JOB_LIB',assignment_dict)
    except Exception as e:
        logger.error("something went wrong when working matching libelles : %s",e)
    try :
        transformer.remove_out_of_range_date('CONTRACT_START_DATE')
        transformer.remove_out_of_range_date('CONTRACT_END_DATE')
        transformer.delete_rows_by_date_comparison('CONTRACT_START_DATE','CONTRACT_END_DATE')
    except Exception as e:
        logger.error("An error occurred during removing inconsistent data: %s",e)
    logger.info("Ending the transform_contract function.")
    return df

def transform_entryexit(df):
    logger = logging.getLogger(__name__)
    transformer = Transform(df)
    logger.info("Starting the transform_entryexit function.")
    try : 
        transformer.remove_column(['ENTRY_PATTERN_CODE','EXIT_PATTERN_CODE','REASON_EXIT_CODE'])
    except Exception as e:
        logger.error("something went wrong when working matching country libelles : %s",e)
    try : 
        transformer.format_column_as_date(['ENTRY_DATE','EXIT_DATE'])
        transformer.transform_columns_to_type(['LIB_ENTRY_PATTERN','LIB_EXIT_PATTERN','LIB_REASON_EXIT'],'category')
    except Exception as e:
        logger.error("something went wrong when formatting columns type: %s",e) 
    try : 
        transformer.remove_out_of_range_date('ENTRY_DATE')
        transformer.remove_out_of_range_date('EXIT_DATE')
        transformer.delete_rows_by_date_comparison('ENTRY_DATE','EXIT_DATE')
    except Exception as e:
        logger.error("An error occurred during removing inconsistent data: %s",e)
    logger.info("Ending the transform_entryexit function.")
    return df

def transform_status(df):
    logger = logging.getLogger(__name__)
    transformer = Transform(df)
    logger.info("Starting the transform_status function.")
    try :
        transformer.remove_column(['STATUS_ID'])
    except Exception as e:
        logger.error(f"something went wrong when working matching country libelles : {e}")
    try :
        transformer.format_column_as_date(['STATUS_START_DATE','STATUS_END_DATE'])
        transformer.transform_columns_to_type(['TURNOVER'],'category')
    except Exception as e:
        logger.error(f"something went wrong when formatting columns type: {e}")
    try :
        transformer.remove_out_of_range_date('STATUS_START_DATE')
        transformer.remove_out_of_range_date('STATUS_END_DATE')
        transformer.delete_rows_by_date_comparison('STATUS_START_DATE','STATUS_END_DATE')
    except Exception as e:
        logger.error(f"An error occurred during removing inconsistent data: {e}") 
    logger.info("Ending the transform_status function.")  
    return df