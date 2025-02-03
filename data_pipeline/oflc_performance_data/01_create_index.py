import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import logging
from config import SCRAPE_URL, LOG_LEVEL, LOG_FORMAT, LOG_FILE, INDEX_FILE_PATH

# Set up logging
logging.basicConfig(filename=LOG_FILE, level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

def clean_link_text(text):
    """
    Cleans the link text by replacing specific substrings and removing unwanted parts.
    
    Args:
        text (str): The original link text.
    
    Returns:
        str: The cleaned link text.
    """
    replacements = {
        "PWD": "PW",
        "Case_Data": "Disclosure_Data",
        "_revised_form": "",
        "iCert_LCA": "",
        "Data": "",
        "_EOY": "",
        "_updated": "",
        ".xlsx": "",
        ".xls": "",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    text = re.sub(r'H(\d)([A-Z])', r'H-\1\2', text)
    return "_".join(text.split())

def extract_year(href):
    """
    Extracts the year from the given href string.
    
    Args:
        href (str): The href string containing the year information.
    
    Returns:
        int: The extracted year if found and valid, otherwise None.
    """
    year = re.findall(r'FY(\d{4}|\d{2})', href)
    if year:
        year = int(year[0])
        return year if year > 2000 else 2000 + year
    return None

def process_link(link):
    """
    Processes a given link to extract relevant information if it meets certain criteria.
    Args:
        link (dict): A dictionary representing the link, expected to have an 'href' key.
    Returns:
        tuple or None: Returns a tuple (year, program, href) if the link meets the criteria,
                    otherwise returns None.
    Criteria:
        - The link must end with '.xlsx' or '.xls'.
        - The year extracted from the link must be 2010 or later.
        - The link text must be 'Disclosure' or an empty string after processing, 
            this guarantees that the link is a disclosure file.
        - If the program is 'CW-1' we will skip it and return None. This program is not relevant for our analysis.
        - LCA program at some point was referred to as H-1B, so we will replace it with 'LCA'.
            - LCA is more general and includes H-1B.
    """
    href = link['href']
    if not (href.endswith('.xlsx') or href.endswith('.xls')):
        return None

    link_text = clean_link_text(href.split("/")[-1])
    link_parts = link_text.split('_')
    program = link_parts[0]

    if program == 'CW-1' or program == 'H-1B':
        program = 'LCA' if program == 'H-1B' else None
        if not program:
            return None
    
    year = extract_year(href)
    if not year or year < 2010:
        return None

    link_text = "_".join(link_parts[1:])
    link_text = re.sub(r'FY\d{4}|FY\d{2}', '', link_text)
    
    quarter = re.findall(r'_Q(\d)', href)
    if quarter:
        link_text = link_text.replace(f"Q{quarter[0]}", "").replace("_", "")
        if quarter[0] != '4':
            return None
    else:
        link_text = link_text.replace("_", "")

    if link_text not in ('Disclosure', ''):
        return None

    return year, program, href

def scrape_links(soup):
    """
    Extracts and processes links from a BeautifulSoup object and returns a DataFrame.

    Args:
        soup (BeautifulSoup): A BeautifulSoup object containing the HTML content to scrape.

    Returns:
        pd.DataFrame: A DataFrame with columns ['year', 'program', 'link'] containing the processed link data.
    """
    data = []
    for link in soup.find_all('a', href=True):
        result = process_link(link)
        if result:
            data.append(result)
    return pd.DataFrame(data, columns=['year', 'program', 'link'])

def create_index():
    """
    Creates an index by scraping links from a specified URL and prints a summary of the indexed data.
    The function performs the following steps:
    1. Logs the start of the index creation process.
    2. Sends a GET request to the specified URL to fetch the content.
    3. Parses the HTML content using BeautifulSoup.
    4. Scrapes links from the parsed HTML content.
    5. Logs the number of valid links found.
    6. Prints a summary of the indexed data, including the programs, available years, total years, most recent year, oldest year, and the number of files for each program.
    7. Prints the total number of files indexed.
    8. Returns the scraped links.
    Returns:
        DataFrame: A DataFrame containing the scraped links if the request is successful.
        None: If there is an error fetching the URL.
    Raises:
        requests.RequestException: If there is an error with the GET request.
    """
    logger.info("Starting index creation process")
    try:
        response = requests.get(SCRAPE_URL)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        links = scrape_links(soup)
        
        logger.info(f"Found {len(links)} valid links")
        
        # Print summary
        print("\nIndex Creation Summary:")
        for program in links['program'].unique():
            program_data = links[links['program'] == program]
            years = program_data['year'].unique()
            print(f"\n{program}:")
            print(f"Years available: {sorted(years)}")
            print(f"Total years: {len(years)}")
            print(f"Most recent year: {max(years)}")
            print(f"Oldest year: {min(years)}")
            print(f"Number of files: {len(program_data)}")
        
        print(f"\nTotal files indexed: {len(links)}")
        
        return links
    except requests.RequestException as e:
        logger.error(f"Error fetching URL: {str(e)}")
        return None

if __name__ == "__main__":
    index = create_index()
    if index is not None:
        # Save the index to a CSV file
        index.to_csv(INDEX_FILE_PATH, index=False)
        logger.info(f"Index saved to {INDEX_FILE_PATH}")
    else:
        logger.error("Failed to create index")
