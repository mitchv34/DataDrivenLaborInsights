import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from tqdm import tqdm
import xlsx2csv
from rich import print



# Download directory
DOWNLOAD_DIR = "/Users/mitchv34/Work/DataDrivenLaborInsights/LCA_Disclosure_Skills/data/raw/"

# Programs
# programs = ["PERM", "H-2A", "H-2B", "LCA"]
# programs = ["PERM"]
programs = ["LCA", "H-1B"]
# Years
years = list(range(2008, 2025))
# Quarters
quarters = ["Q1", "Q2", "Q3", "Q4"]

def download_file(url, save_dir, file_name):
    """
    Downloads a file from the given URL and saves it to the specified directory with the given file name.
    Only downloads if the content type is an Excel file.
    """
    os.makedirs(save_dir, exist_ok=True)
    response = requests.get(url)
    
    # Check if the content type is an Excel file
    if response.headers.get('Content-Type') in ['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'application/vnd.ms-excel']:
        file_path = os.path.join(save_dir, file_name)
        try:
            with open(file_path, 'wb') as f:
                f.write(response.content)
            return True
        except Exception as e:
            print(f"[bold red]Error downloading {url}: {e}[/bold red]")
            return False
    else:
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
        print(f"[italic green]Downloaded {url}[/italic green]")
        return f"[italic green]Downloaded {url}[/italic green]"
    else:
        print(f"[bold red]Failed to download {url}[/bold red]")
        return f"[bold red]Failed to download {url}[/bold red]"

def download_year(p, y):
    """
    Tries to download all quarters both with "_Disclosure_Data" in the link and without,
    and then tries to download just the year both with "_Disclosure_Data" in the link and without.
    Also tries variations of the year format (e.g., FY2016 and FY16).
    """
    quarters = ["Q1", "Q2", "Q3", "Q4"]
    year_formats = [f"FY{y}", f"FY{str(y)[-2:]}"]
    
    for q in quarters:
        for year_format in year_formats:
            # Try with "_Disclosure_Data"
            url = f"https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/{p}_Disclosure_Data_{year_format}_{q}.xlsx"
            file_name = f"{p}_{y}_{q}.xlsx"
            if download_file(url, DOWNLOAD_DIR + p + '/', file_name):
                print(f"[italic green]Downloaded {url}[/italic green]")
            else:
                print(f"[bold red]Failed to download {url}[/bold red]")

            # Try without "_Disclosure_Data"
            url = f"https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/{p}_{year_format}_{q}.xlsx"
            if download_file(url, DOWNLOAD_DIR + p + '/', file_name):
                print(f"[italic green]Downloaded {url}[/italic green]")
            else:
                print(f"[bold red]Failed to download {url}[/bold red]")

    for year_format in year_formats:
        file_name = f"{p}_{y}.xlsx"
        # Try with "_Disclosure_Data" for the whole year
        url = f"https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/{p}_Disclosure_Data_{year_format}.xlsx"
        if download_file(url, DOWNLOAD_DIR + p + '/', file_name):
            print(f"[italic green]Downloaded {url}[/italic green]")
        else:
            print(f"[bold red]Failed to download {url}[/bold red]")

        # Try without "_Disclosure_Data" for the whole year
        url = f"https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/{p}_{year_format}.xlsx"
        if download_file(url, DOWNLOAD_DIR + p + '/', file_name):
            print(f"[italic green]Downloaded {url}[/italic green]")
        else:
            print(f"[bold red]Failed to download {url}[/bold red]")

# Create a ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = []
    # Submit download tasks for each combination of program, year
    for p in programs:
        program_dir = os.path.join(DOWNLOAD_DIR, p)
        os.makedirs(program_dir, exist_ok=True)
        for y in years:
            time.sleep(1)
            futures.append(executor.submit(download_year, p, y))
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = []

    for  p in programs:
        program_dir = os.path.join(DOWNLOAD_DIR, p)
        xlsx_files = [file for file in os.listdir(program_dir) if file.endswith(".xlsx")]
        progress_bar = tqdm(total=len(xlsx_files), desc="Converting files to CSV")

        for file in xlsx_files:
            try:
                xlsx2csv.Xlsx2csv(os.path.join(program_dir, file)).convert(os.path.join(program_dir, file.replace(".xlsx", ".csv")))
                os.remove(os.path.join(program_dir, file))
            except Exception as e:
                print(f"[bold red]Error converting {file} to CSV: {e}[/bold red]")
            progress_bar.update(1)

        progress_bar.close()

#     for future in as_completed(futures):
#         print(future.result())

#         # Create a ThreadPoolExecutor
#         with ThreadPoolExecutor(max_workers=10) as executor:
#             futures = []
#             # Submit download tasks for each combination of program and year
#             for p in programs:
#                 for y in years:
#                     futures.append(executor.submit(download_year, p, y))

#             for future in as_completed(futures):
#                 print(future.result())

# for p in programs:
#     for y in years:
#         time.sleep(1)
#         download_year(p, y)

