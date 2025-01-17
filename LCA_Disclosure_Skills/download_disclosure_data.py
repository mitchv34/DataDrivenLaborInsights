import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# Download directory
DOWNLOAD_DIR = "/Users/mitchv34/Work/DataDrivenLaborInsights/LCA_Disclosure_Skills/data/raw/"

# Programs
programs = ["PERM", "H2-A", "H2-B", "LCA"]
# Years
years = list(range(2008, 2025))
# Quarters
quarters = ["Q1", "Q2", "Q3", "Q4"]

def download_file(url, save_dir, file_name):
    """
    Downloads a file from the given URL and saves it to the specified directory with the given file name.
    """
    os.makedirs(save_dir, exist_ok=True)
    response = requests.get(url)
    # if response.status_code == 200:
    file_path = os.path.join(save_dir, file_name)
    try:
        with open(file_path, 'wb') as f:
            f.write(response.content)
        return True
    except Exception as e:
    # else:
        return False

def download_task(p, y, q=None):
    """
    Constructs the URL for the file to be downloaded based on the program, year, and quarter (if applicable),
    and then downloads the file.
    """
    if q:
        url = f"https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/{p}_Disclosure_Data_FY{y}_{q}.xlsx"
        file_name = f"{p}_Disclosure_Data_FY{y}_{q}.xlsx"
    else:
        url = f"https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/{p}_FY{y}.xlsx"
        file_name = f"{p}_Disclosure_Data_FY{y}.xlsx"
    
    if download_file(url, DOWNLOAD_DIR, file_name):
        return "" #f"Downloaded {url}"
    else:
        return f"Failed to download {url}"


# Create a ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = []
    # Submit download tasks for each combination of program, year, and quarter
    for p in programs:
        for y in years:
            for q in quarters:
                futures.append(executor.submit(download_task, p, y, q))
            futures.append(executor.submit(download_task, p, y))

    # Print the result of each completed future
    for future in as_completed(futures):
        print(future.result())
