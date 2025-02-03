import os
import pandas as pd
import requests
from config import RAW_DATA_DIR, INDEX_FILE_PATH, DOWNLOAD_URL_BASE
from xlsx2csv import Xlsx2csv
import logging
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Function to convert Excel file to CSV
def excel_to_csv(excel_file, csv_file):
    try:
        Xlsx2csv(excel_file, outputencoding="utf-8").convert(csv_file)
        logger.info(f"Converted {excel_file} to {csv_file}")
    except Exception as e:
        logger.error(f"Error converting {excel_file} to CSV: {e}")
        raise

def download_and_convert(row, program_dir, download_url_base=DOWNLOAD_URL_BASE):
    """
    Downloads an Excel file from a given URL, converts it to CSV format, and deletes the original Excel file.

    Args:
        row (tuple): A tuple containing the year, program name, and the link to the Excel file.
        program_dir (str): The directory where the downloaded file will be saved.
        download_url_base (str): The base URL for downloading the file. Defaults to DOWNLOAD_URL_BASE.

    Returns:
        tuple: A tuple containing the status ("Success" or "Failed") and the path to the CSV file or the original filename in case of failure.

    Raises:
        Exception: If there is an error during the download or conversion process, it will be logged and the function will return "Failed".
    """
    year, program, link = row
    link = download_url_base + link
    filename = os.path.join(program_dir, f"{year}_{program}.xlsx")
    logger.info(f"Downloading {filename}...")
    try:
        response = requests.get(link, timeout=30)
        response.raise_for_status()
        with open(filename, 'wb') as f:
            f.write(response.content)
        logger.info(f"Downloaded {filename}")

        csv_filename = filename.replace(".xlsx", ".csv")
        excel_to_csv(filename, csv_filename)

        os.remove(filename)
        logger.info(f"Deleted original file {filename}")
        return "Success", csv_filename
    except Exception as e:
        logger.error(f"Error processing {filename}: {e}")
        return "Failed", filename

def process_error_files(error_files):
    """
    Processes a list of error files by attempting to read each file with pandas,
    converting it to a CSV file, and then deleting the original file.

    Args:
        error_files (list of str): List of file paths to the error files to be processed.

    Logs:
        - Info: When attempting to read a file and save it as CSV.
        - Info: When a file is successfully processed.
        - Error: When there is an error reading a file with pandas.
    """
    for file in error_files:
        logger.info(f"Attempting to read {file} with pandas and save as CSV...")
        try:
            df = pd.read_excel(file, engine='openpyxl')
            df.to_csv(file.replace(".xlsx", ".csv"), index=False)
            os.remove(file)
            logger.info(f"Successfully processed {file} with pandas")
        except Exception as e:
            logger.error(f"Error reading {file} with pandas: {e}")

def main():
    """
    Main function to download and process raw data files.

    This function reads an index file containing links to raw data files,
    processes each file by downloading and converting it, and logs the progress.
    If any errors occur during the process, they are logged and handled separately.

    The function performs the following steps:
    1. Reads the index file containing links to raw data files.
    2. Initializes a progress bar to track the processing of files.
    3. Iterates over the grouped data by 'program' and processes each file.
    4. Downloads and converts each file, logging any errors encountered.
    5. Updates the progress bar with the status of each processed file.
    6. Handles any files that encountered errors during processing.

    Returns:
        None
    """
    try:
        links = pd.read_csv(INDEX_FILE_PATH)
        logger.info(f"Loaded index file from {INDEX_FILE_PATH}")
    except Exception as e:
        logger.error(f"Error loading index file: {e}")
        return

    error_files = []
    total_files = len(links)

    with tqdm(total=total_files, desc="Processing files", unit="file", colour='green') as pbar:
        for group, g in links.groupby('program'):
            pbar.set_description(f"Processing {group}")
            program_dir = os.path.join(RAW_DATA_DIR, group)
            os.makedirs(program_dir, exist_ok=True)

            for _, row in g.iterrows():
                message, file = download_and_convert((row['year'], row['program'], row['link']), program_dir)
                if message == "Failed":
                    error_files.append(file)
                pbar.update(1)
                pbar.set_postfix_str(f"Last processed: {row['year']}_{row['program']}")

    process_error_files(error_files)

if __name__ == "__main__":
    main()