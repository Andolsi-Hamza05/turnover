#from configs.tables import dataframes
from ETL.scripts.extract import extract_table
from ETL.scripts.transform import transform_absence, transform_assignment, transform_contract, transform_employee, transform_entryexit, transform_status
from ETL.scripts.utils import persist_data
from ETL.scripts.load import load_dataframe_to_postgresql

def apply_function_on_dataframe(func, df):
    return func(df)


dataframes = {
    "dimemployee": transform_employee,
    "dimabsence": transform_absence,
    "dimcontract": transform_contract,
    "dimstatus": transform_status,
    "dimentryexit": transform_entryexit,
    "dimassignment": transform_assignment
}


if __name__ == '__main__':
    for dataframe_name, transformation_function in dataframes.items():
        extracted_data = extract_table(dataframe_name)
        print(extracted_data)
        transformed_data = apply_function_on_dataframe(transformation_function, extracted_data)
        persist_data(transformed_data, dataframe_name, 'clean_data')
        load_dataframe_to_postgresql(transformed_data,str(dataframe_name))

