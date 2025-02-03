import os
import pandas as pd
import re
import logging
from config import RAW_DATA_DIR, PROCESSED_DATA_DIR, PROGRAMS_PROCESS, COLUMNS_DICT
from config import LOG_LEVEL, LOG_FORMAT, LOG_FILE, PROCESSED_FILE_TEMPLATE
from tqdm import tqdm

# Set up logging
logging.basicConfig(filename=LOG_FILE, level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

def clean_column_names(df):
    """
    Clean the column names of the dataframe by stripping whitespace, converting to uppercase,
    replacing spaces with underscores, and removing specific prefixes and suffixes.

    Args:
        df (pd.DataFrame): The input dataframe with raw column names.

    Returns:
        pd.DataFrame: The dataframe with cleaned column names.
    """
    df.columns = df.columns.str.strip().str.upper().str.replace(' ', '_', regex=False)
    df.columns = df.columns.str.replace(r'^LCA_CASE_|_9089$', '', regex=True)
    return df

def rename_columns(df):
    """
    Renames columns in the given DataFrame according to a predefined mapping. 

    Parameters:
    df (pd.DataFrame): The DataFrame whose columns need to be renamed.

    Returns:
    pd.DataFrame: The DataFrame with renamed columns.
    """
    column_mapping = {
        # Case information columns
        'NUMBER'                    : 'CASE_NUMBER',                #
        'STATUS'                    : 'CASE_STATUS',                #
        'EMP'                       : 'EMPLOYER',                   #
        'CASE_NO'                   : 'CASE_NUMBER',                #
        # Industry information of employer columns
        'NAIC_CODE'                 : 'NAICS_CODE',                 # 
        '2007_NAICS_US_CODE'        : 'NAICS_CODE',                 #
        'NAICS_US_CODE'             : 'NAICS_CODE',                 #
        # Occupation information columns
        'FULL_TIME_POS'             : 'FULL_TIME_POSITION',         #
        'PW_SOC_CODE'               : 'SOC_CODE',                   #
        'PW_JOB_TITLE'              : 'JOB_TITLE',                  #
        # Wage information columns
        'WAGE_OFFER_FROM'           : 'WAGE_RATE_FROM',             #
        'WAGE_OFFER_TO'             : 'WAGE_RATE_TO',               #
        'WAGE_OFFERED_FROM'         : 'WAGE_RATE_FROM',             #
        'WAGE_OFFERED_TO'           : 'WAGE_RATE_TO',               #
        'WAGE_RATE_OF_PAY_FROM'     : 'WAGE_RATE_FROM',             #
        'WAGE_RATE_OF_PAY_TO'       : 'WAGE_RATE_TO',               #
        'WAGE_UNIT_OF_PAY'          : 'WAGE_RATE_UNIT',             #
        'WAGE_RATE_OF_PAY'          : 'WAGE_RATE_FROM',             #
        'WAGE_RATE_OF_PAY_FROM_1'   : 'WAGE_RATE_FROM',             #
        'WAGE_RATE_OF_PAY_TO_1'     : 'WAGE_RATE_TO',               #
        'WAGE_UNIT_OF_PAY_1'        : 'WAGE_RATE_UNIT',             #
        'PW_UNIT_1'                 : 'UNIT_OF_PAY',                #
        'PW_UNIT'                   : 'UNIT_OF_PAY',                #
        'PW_UNIT_OF_PAY'            : 'UNIT_OF_PAY',                #
        'PW_UNIT_OF_PAY_1'          : 'UNIT_OF_PAY',                #
        # Employer information columns
        'EMPLOYER_ADDRESS1'         : 'EMPLOYER_ADDRESS',           #
        'EMPLOYER_ADDRESS_1'        : 'EMPLOYER_ADDRESS',           #
        'EMPLOYER_STATE_PROVINCE'   : 'EMPLOYER_STATE',             #
        'TOTAL_WORKER_POSITIONS'    : 'TOTAL_WORKERS',              #
        # Worksite information columns
        'WORK_LOCATION_CITY1'       : 'WORKSITE_CITY',              #
        'WORK_LOCATION_STATE1'      : 'WORKSITE_STATE',             #
        'WORKLOC1_CITY'             : 'WORKSITE_CITY',              #
        'WORKLOC1_STATE'            : 'WORKSITE_STATE',             #
        'WORKSITE_CITY_1'           : 'WORKSITE_CITY',              #
        'WORKSITE_STATE_1'          : 'WORKSITE_STATE',             #
        'WORKSITE_POSTAL_CODE_1'    : 'WORKSITE_POSTAL_CODE',       #
        'WORKSITE_ADDRESS1_1'       : 'WORKSITE_ADDRESS1',          #
        'WORKSITE_ADDRESS_1'        : 'WORKSITE_ADDRESS1',          #
        'JOB_INFO_WORK_CITY'        : 'WORKSITE_CITY',              #
        'JOB_INFO_WORK_STATE'       : 'WORKSITE_STATE',             #
        'JOB_INFO_WORK_POSTAL_CODE' : 'WORKSITE_POSTAL_CODE',       # 
    }
    df.rename(columns=column_mapping, inplace=True)
    return df

def handle_special_cases(df, year, program):
    """
    Handle special cases for the given DataFrame based on the year and program.

    Parameters:
    df (pd.DataFrame): The input DataFrame to be processed.
    year (int): The year associated with the data.
    program (str): The program type, which can be "PERM" or "LCA".

    Returns:
    pd.DataFrame: The processed DataFrame with special cases handled.

    Special Cases:
    - If the program is "PERM", sets 'TOTAL_WORKERS' to 1 since Permanent Resident applications are for a single worker.
    - If 'WORKSITE_POSTAL_CODE' is not in the DataFrame columns, adds it with NA values.
    - If 'WORKSITE_ADDRESS1' is not in the DataFrame columns, adds it with NA values.
    - If the program is "LCA" and the year is 2015:
        - We dont get 'WAGE_RATE_TO' in 2015, instead we get 'WAGE_RATE_FROM' as a range X - Y.
        - Attempts to split 'WAGE_RATE_FROM' into 'WAGE_RATE_FROM' and 'WAGE_RATE_TO'.
        - Converts the split columns to numeric values, coercing errors to NA.
    """
    if program == "PERM":
        df['TOTAL_WORKERS'] = 1
    if 'WORKSITE_POSTAL_CODE' not in df.columns:
        df['WORKSITE_POSTAL_CODE'] = pd.NA
    if 'WORKSITE_ADDRESS1' not in df.columns:
        df['WORKSITE_ADDRESS1'] = pd.NA
    if (program == "LCA") & (year == 2015):
        df['WAGE_RATE_TO'] = pd.NA
        try:
            df[['WAGE_RATE_FROM', 'WAGE_RATE_TO']] = df['WAGE_RATE_FROM'].str.split(' -', expand=True)
            df[['WAGE_RATE_FROM', 'WAGE_RATE_TO']] = df[['WAGE_RATE_FROM', 'WAGE_RATE_TO']].apply(pd.to_numeric, errors='coerce')
        except Exception as e:
            logging.error(f"Error splitting WAGE_RATE_FROM: {e}")
    return df

def process_data(df, year, program, columns_dict):
    """
    Process the given DataFrame by cleaning column names, renaming columns, handling special cases,
    and ensuring all required columns are present.

    Parameters:
    df (pd.DataFrame): The input DataFrame to be processed.
    year (int): The year associated with the data.
    program (str): The program type, which can be "PERM" or "LCA".
    columns_dict (Dict[str, List[str]]): A dictionary mapping column categories to lists of required columns.

    Returns:
    pd.DataFrame: The processed DataFrame with necessary columns and pre-processing applied.
    """
    df = df.copy()
    df = clean_column_names(df)
    df = rename_columns(df)
    df = handle_special_cases(df, year, program)

    # Check for missing columns
    all_columns_present = True
    for col_cat, columns in columns_dict.items():
        missing_columns = [column for column in columns if column not in df.columns]
        if missing_columns:
            logging.warning(f"Missing {', '.join(missing_columns)} in Program = {program}, year = {year}.")
            all_columns_present = False

    if not all_columns_present:
        logging.info(f"Program = {program}, year = {year}, all columns present.")

    # Pre-process the data to drop unnecessary columns and rows
    if (int(year) in range(2019, 2025)) & (program == 'LCA'):
        df.columns = df.columns.str.replace(r'(?<!\d)_1$', '', regex=True)
        size_before = df.shape[0]
        df = df[(df['TOTAL_WORKERS'].isnull()) | (df['TOTAL_WORKERS'] == df['TOTAL_WORKERS'])]
        logging.info(f"Year = {year}, number of rows = {df.shape[0]}, percentage of rows = {df.shape[0] / size_before * 100:.2f}%")
        df.drop(columns=df.filter(regex='^PW_').columns, inplace=True)
        df.drop(columns=df.filter(regex=r'_\d+$').columns, inplace=True)

    columns_to_keep = list(set(sum(columns_dict.values(), []))) + ['PROGRAM']
    return df[columns_to_keep]

def process_and_save_program_data():
    """
    Process and save data for each program in the PROGRAMS_PROCESS list.

    This function iterates over each program, reads the raw data files, processes the data,
    and saves the processed data to the specified directory. It also logs statistics for each program dataset.
    """
    for program in tqdm(PROGRAMS_PROCESS, desc="Programs"):
        program_data = pd.DataFrame()
        list_files = [f for f in os.listdir(os.path.join(RAW_DATA_DIR, program)) if f.endswith('.csv')]
        list_files.sort()

        for f in tqdm(list_files, desc=f"Processing {program}", leave=False):
            year = re.findall(r'\d{4}', f)
            if year:
                year = int(year[0])
                logger.info(f"Processing program {program} file year {year}")

                data_year = pd.read_csv(os.path.join(RAW_DATA_DIR, program, f), low_memory=False).dropna(how='all')
                # Add a column for the program name
                data_year['PROGRAM'] = program
                processed_data = process_data(data_year, year, program, COLUMNS_DICT)
                program_data = pd.concat([program_data, processed_data], ignore_index=True)

        # Save the processed data for the program
        output_file = os.path.join(PROCESSED_DATA_DIR, PROCESSED_FILE_TEMPLATE.format(program=program))
        program_data.to_csv(output_file, index=False)
        logger.info(f"Saved processed data for {program} to {output_file}")

        # Print statistics for the program dataset
        logger.info(f"\nStatistics for {program} dataset:")
        logger.info(f"Number of rows: {program_data.shape[0]}")
        logger.info(f"Number of columns: {program_data.shape[1]}")
        logger.info(f"Columns: {list(program_data.columns)}")
        logger.info(f"Data types:\n{program_data.dtypes}")
        logger.info(f"Memory usage: {program_data.memory_usage().sum() / 1e6:.2f} MB")
        logger.info(f"Sample data:\n{program_data.head()}\n")
        logger.info("-" * 50)

if __name__ == "__main__":
    logger.info("Starting data processing")
    process_and_save_program_data()
    logger.info("Data processing completed")
