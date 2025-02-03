import os
import pandas as pd
import numpy as np
import logging
import argparse
from typing import Dict, List

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants

# Base URL for O*NET data
BASE_URL = 'https://www.onetcenter.org/dl_files/database/db_29_1_text/'

# Base paths
BASE_DIR = '.'
SHARED_DATA_DIR = os.path.join(BASE_DIR, 'shared_data', 'onet_data')
PROCESSED_DATA_DIR = os.path.join(SHARED_DATA_DIR, 'processed')

# Ensure directories exist
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

# Which data sets to download
DATA_SETS = {
    # Measurement data (e.g., abilities, skills, knowledge)
    "1_measure": [
        "Education, Training, and Experience",
        "Job Zones",
        "Interests",
        "Work Values",
        "Knowledge",
        "Skills",
        "Abilities",
        "Work Styles",
        "Work Activities",
        "Task Statements",
        "Task Ratings",
        "Technology Skills",
        "Tools Used"
    ],
    # Reference data (e.g., Content Model Reference, Crosswalks, Occupation Data (definitions))
    "0_reference": [
        "Occupation Data",
        "Scales Reference",
        "Content Model Reference",
        "Education, Training, and Experience Categories",
        "Job Zone Reference",
        "Task Categories",
        "UNSPSC Reference",
        "IWA Reference",
        "DWA Reference",
        "Tasks to DWAs",
        "Related Occupations",
        "Abilities to Work Activities",
        "Abilities to Work Context",
        "Skills to Work Activities",
        "Skills to Work Context"
    ]
}

print(DATA_SETS.keys())

# Make sure that the dictionary keys are alphabetically sorted (we need reference data saved first)
DATA_SETS = dict(sorted(DATA_SETS.items()))
print(DATA_SETS.keys())

def save_data(df: pd.DataFrame, file_name: str, data_set_type: str):

    # Rename the file
    file_name = file_name.replace(' ', '_').replace(',', '').upper() + '.csv'

    folder_save = os.path.join( PROCESSED_DATA_DIR, data_set_type[2:] )
    # Create the directory if it doesn't exist
    os.makedirs(folder_save, exist_ok=True)
    file_path = os.path.join( folder_save, file_name )
    
    df.to_csv(file_path, index=False)
    logging.info(f"Data saved to {file_path}")


COLUMNS_KEEP_MEASUREMENTS = [
    "ONET_SOC_CODE", "ELEMENT_ID", "SCALE_ID", "JOB_ZONE",
    "TASK_TYPE", "TASK_ID", "CATEGORY", "DATA_VALUE", 
    "RECOMMEND_SUPPRESS", "NOT_RELEVANT", "COMMODITY_CODE",
    "HOT_TECHNOLOGY", "IN_DEMAND"
]

def get_data(data_set_name: str) -> pd.DataFrame:
    """Retrieve data from the O*NET database."""
    try:
        data_set_name = data_set_name.replace(' ', '%20').replace(',', '%2C')
        data_url = f"{BASE_URL}{data_set_name}.txt"
        logging.info(f"Fetching data from {data_url}")
        return pd.read_csv(data_url, sep='\t')
    except Exception as e:
        logging.error(f"Failed to fetch data: {e}")
        raise

def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare the data by renaming columns and removing special characters."""
    return df.rename(columns=lambda col: col.upper().replace(' ', '_').replace('-', '_').replace('*', ''))

def process_measurements(df: pd.DataFrame) -> pd.DataFrame:
    """Process measurement data and convert to wide format."""
    df = df[list(set(COLUMNS_KEEP_MEASUREMENTS) & set(df.columns))]
    df_wide = df.pivot(
        index=['ONET_SOC_CODE', 'ELEMENT_ID', 'RECOMMEND_SUPPRESS'],
        columns='SCALE_ID',
        values='DATA_VALUE'
    ).reset_index()

    if ('NOT_RELEVANT' not in df_wide.columns) and ('NOT_RELEVANT' in df.columns):
        mapping = df[df['SCALE_ID'] == 'LV'].set_index(['ONET_SOC_CODE', 'ELEMENT_ID'])['NOT_RELEVANT'].to_dict()
        df_wide['NOT_RELEVANT'] = df_wide.apply(lambda x: mapping.get((x['ONET_SOC_CODE'], x['ELEMENT_ID']), None), axis=1)

    return df_wide

def create_parent_levels(df: pd.DataFrame) -> pd.DataFrame:
    """Create a DataFrame with repeated parent levels.
        This function should only be used to create a reference DataFrame for Model Reference Elements.
    """
    all_levels = [element_id.split('.') for element_id in df['ELEMENT_ID']]
    max_depth = max(len(x) for x in all_levels)
    column_names = [f'LEVEL_{i+1}' for i in range(max_depth)]
    
    level_df = pd.DataFrame([
        ['.'.join(parts[:i+1]) for i in range(len(parts))] + [np.nan] * (max_depth - len(parts))
        for parts in all_levels
    ], columns=column_names)
    
    return level_df



def main():
    
    for data_set_type, list_of_data_sets in DATA_SETS.items():
        if data_set_type == "measure":
            continue

        for data_set_name in list_of_data_sets:
            try:
    
                df = get_data(data_set_name)
                df = prepare_data(df)
                
                # Special cases:
                # Content Model Reference
                if data_set_name == "Content Model Reference":
                    # Add Levels
                    df['LEVEL'] = df["ELEMENT_ID"].str.split('.').apply(lambda x: len(x))	
                    # Create parent levels
                    df_parent_levels = create_parent_levels(df)
                    # Save parent levels
                    save_data(df_parent_levels, f"{data_set_name}_PARENT_LEVELS.csv")

                # Related Occupations
                if data_set_name == "Related Occupations":
                    # Simplify RELATEDNESS_TIER 
                    df["RELATEDNESS_TIER"] = df["RELATEDNESS_TIER"].apply(lambda x: "".join([word[0] for word in x.split("-")]))
                
                if data_set_type == "measure":
                    df = process_measurements(df) 

                save_data(df, data_set_name, data_set_type)
                
                logging.info(f"Successfully processed {data_set_name}")

                

            except Exception as e:
                logging.error(f"Error processing {data_set_name}: {e}")
        

if __name__ == "__main__":
    main()
