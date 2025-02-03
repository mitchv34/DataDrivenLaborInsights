import os

"""
Configuration file for the OFLC performance data pipeline.

This module sets up the necessary paths, URLs, and parameters for downloading,
processing, and logging data related to the OFLC performance metrics.

Attributes:
    BASE_DIR (str): The base directory for the project.
    SHARED_DATA_DIR (str): Directory for shared data.
    RAW_DATA_DIR (str): Directory for raw data.
    PROCESSED_DATA_DIR (str): Directory for processed data.
    INDEX_FILE_PATH (str): Path to the index CSV file.
    SCRAPE_URL (str): URL for scraping OFLC performance data.I 
    RAW_FILE_TEMPLATE (str): Template for naming raw data files.
    PROCESSED_FILE_TEMPLATE (str): Template for naming processed data files.
    PROGRAMS (list): List of program names to be processed.
    CHUNK_SIZE (int): Size of chunks for downloading large files.
    TIMEOUT (int): Timeout for download requests in seconds.
    LOG_LEVEL (int): Logging level.
    LOG_FORMAT (str): Format for logging messages.
    LOG_FILE (str): Path to the log file.
    DATE_COLUMNS (list): List of columns containing date values.
    NUMERIC_COLUMNS (list): List of columns containing numeric values.
"""

# Base paths
BASE_DIR = '.'
SHARED_DATA_DIR = os.path.join(BASE_DIR, 'shared_data', 'oflc_performance_data')
RAW_DATA_DIR = os.path.join(SHARED_DATA_DIR, 'raw')
PROCESSED_DATA_DIR = os.path.join(SHARED_DATA_DIR, 'processed')
INDEX_FILE_PATH = os.path.join(RAW_DATA_DIR, 'index.csv')

# Ensure directories exist
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

# URL for scraping
SCRAPE_URL = 'https://www.dol.gov/agencies/eta/foreign-labor/performance#dis'

# URL for downloading
DOWNLOAD_URL_BASE = 'https://www.dol.gov/'

# File naming conventions
RAW_FILE_TEMPLATE = "{program}_{year}.xlsx"
PROCESSED_FILE_TEMPLATE = "{program}_long.csv"

# Program names (download)
PROGRAMS = ['LCA', 'PERM', 'H-2A', 'H-2B']

# Program names (processing)
PROGRAMS_PROCESS = ['LCA', 'PERM']

# Column definitions
COLUMNS_DICT = {
    'case_columns': ['CASE_NUMBER', 'CASE_STATUS', 'DECISION_DATE'],
    'industry_columns': ['NAICS_CODE'],
    'occ_columns': ['SOC_CODE', 'JOB_TITLE'],
    'wage_columns': ['WAGE_RATE_FROM', 'WAGE_RATE_TO', 'UNIT_OF_PAY'],
    'emp_cols': ['EMPLOYER_ADDRESS', 'EMPLOYER_CITY', 'EMPLOYER_NAME', 'EMPLOYER_POSTAL_CODE', 'EMPLOYER_STATE', 'TOTAL_WORKERS'],
    'worksite_columns': ['WORKSITE_STATE', 'WORKSITE_CITY', 'WORKSITE_POSTAL_CODE', 'WORKSITE_ADDRESS1']
}

# # Download parameters
# CHUNK_SIZE = 8192  # for downloading large files
# TIMEOUT = 60  # timeout for download requests in seconds

# Logging configuration
import logging
LOG_LEVEL = logging.INFO
LOG_FORMAT = '%(asctime)s - %(filename)s - %(name)s - %(levelname)s - %(message)s'
LOG_FILE = os.path.join(BASE_DIR, 'data_pipeline', 'oflc_performance_data', 'pipeline.log')