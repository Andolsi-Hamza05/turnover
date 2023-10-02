import os 

def persist_data(dataframe, file_name,folder_name):
    """
    Save a DataFrame to a CSV file in the specified folder location.
    
    Parameters:
    - dataframe: The DataFrame to save.
    - folder_name: The name of the folder where the CSV file will be saved.
    - file_name: The name of the CSV file (including the ".csv" extension).
    """
    folder_path = 'C:/Users/hlandolsi/Desktop/turnover/data'
    full_file_path = f"{folder_path}/{folder_name}/{file_name}.csv"
    if os.path.exists(full_file_path):
        print("File already exists")
    else:
        dataframe.to_csv(full_file_path, index=False)
        print("File loaded to data folder")