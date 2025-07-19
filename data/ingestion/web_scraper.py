import requests
import time
import json
import os
from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from urllib.parse import urljoin, urlparse
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import re

"""
Responsibility In The ETL Pipeline:

Automate the extraction of company financial documents.
"""

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class Company:
    """Data class for company information"""

    name: str
    ticker: str
    sedar_id: str
    market_cap: Optional[float] = None
    sector: Optional[str] = None


@dataclass
class Document:
    """Data class for company documents"""

    company_name: str
    company_ticker: str
    document_type: str
    document_publish_date: datetime
    document_url: str
    file_name: str


class SEDARScraper:
    """Web scraper to retrieve financial documents SEDAR+."""

    def __init__(self, headless: bool = True, download_dir: str = "downloads"):
        self.base_url = "https://sedar.com"
        self.search_url = "https://sedar.com/search/"
        self.download_dir = download_dir
        self.session = requests.Session()

        # Chrome Options
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")

        prefs = {
            "download.default_directory": os.path.abspath(download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
        }

        chrome_options.add_experimental_option("prefs", prefs)

        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10)

        os.makedirs(download_dir, exist_ok=True)

        self.top_companies = [
            {
                "name": "Royal Bank of Canada",
                "ticker": "RY.TO",
                "sedar_id": "0000038019",
            },
            {
                "name": "Toronto-Dominion Bank",
                "ticker": "TD.TO",
                "sedar_id": "0000038018",
            },
            {
                "name": "Bank of Nova Scotia",
                "ticker": "BNS.TO",
                "sedar_id": "0000038020",
            },
            {"name": "Bank of Montreal", "ticker": "BMO.TO", "sedar_id": "0000038021"},
            {
                "name": "Canadian Imperial Bank of Commerce",
                "ticker": "CM.TO",
                "sedar_id": "0000038022",
            },
            {"name": "Enbridge Inc.", "ticker": "ENB.TO", "sedar_id": "0000038023"},
            {
                "name": "Canadian National Railway",
                "ticker": "CNR.TO",
                "sedar_id": "0000038024",
            },
            {
                "name": "Canadian Pacific Railway",
                "ticker": "CP.TO",
                "sedar_id": "0000038025",
            },
            {"name": "Suncor Energy Inc.", "ticker": "SU.TO", "sedar_id": "0000038026"},
            {
                "name": "TC Energy Corporation",
                "ticker": "TRP.TO",
                "sedar_id": "0000038027",
            },
            {"name": "Shopify Inc.", "ticker": "SHOP.TO", "sedar_id": "0000038028"},
            {
                "name": "Brookfield Asset Management",
                "ticker": "BAM.TO",
                "sedar_id": "0000038029",
            },
            {
                "name": "Manulife Financial Corporation",
                "ticker": "MFC.TO",
                "sedar_id": "0000038030",
            },
            {
                "name": "Sun Life Financial Inc.",
                "ticker": "SLF.TO",
                "sedar_id": "0000038031",
            },
            {
                "name": "Barrick Gold Corporation",
                "ticker": "ABX.TO",
                "sedar_id": "0000038032",
            },
            {"name": "Nutrien Ltd.", "ticker": "NTR.TO", "sedar_id": "0000038033"},
            {
                "name": "Canadian Natural Resources",
                "ticker": "CNQ.TO",
                "sedar_id": "0000038034",
            },
            {
                "name": "Cenovus Energy Inc.",
                "ticker": "CVE.TO",
                "sedar_id": "0000038035",
            },
            {
                "name": "Imperial Oil Limited",
                "ticker": "IMO.TO",
                "sedar_id": "0000038036",
            },
            {
                "name": "Alimentation Couche-Tard Inc.",
                "ticker": "ATD.TO",
                "sedar_id": "0000038037",
            },
        ]

        self.document_types = [
            "Annual Information Form",
            "Annual Report",
            "Management Discussion and Analysis",
            "Financial Statements",
            "Prospectus",
            "Material Change Report",
            "Management Information Circular",
        ]

    def get_company_documents(
        self,
        company: Company,
        document_types: List[str] = None,
        start_date: str = None,
        end_date: str = None,
    ) -> List[Document]:
        "Retrives documents for a specific company."

        if document_types is None:
            document_types = self.document_types

        documents = []

        try:
            self.driver.get(self.search_url)
            time.sleep(2)

            self._search_company_by_id(company.sedar_id)

            self._filter_by_document_types(document_types)

            if start_date and end_date:
                self._filter_by_date_range(start_date, end_date)

            documents = self._extract_documents_from_results(company)

            logger.info(f"Found {len(documents)} documents for {company.name}")

        except Exception as e:
            logger.error(f"Error retrieving documents for {company.name}: {str(e)}")

        return documents

    def _search_company_by_id(self, sedar_id: str):
        """Search for company by SEDAR ID"""
        try:
            company_field = self.wait.until(
                EC.presence_of_element_located((By.NAME, "company_name"))
            )
            company_field.clear()
            company_field.send_keys(sedar_id)

            search_button = self.driver.find_element(By.NAME, "submit")
            search_button.click()

            time.sleep(3)

        except TimeoutException:
            logger.error("Timeout while searching for company")
            raise

    def _filter_by_document_types(self, document_types: List[str]):
        """Filter search results by document types"""
        pass

    def _filter_by_date_range(self, start_date: str, end_date: str):
        """Filter search results by date range"""
        pass

    def _extract_documents_from_results(self, company: Company) -> List[Document]:
        """Extract document information from search results"""
        pass

    def download_document(self, document: Document) -> bool:
        """
        Download a specific document
        """
        pass

    def scrape_all_companies(
        self,
        document_types: List[str] = None,
        start_date: str = None,
        end_date: str = None,
        download_documents: bool = True,
    ) -> Dict[str, List[Document]]:
        """
        Scrape documents for all top companies
        """
        pass

    def save_results_to_json(
        self,
        documents: Dict[str, List[Document]],
        filename: str = "sedar_documents.json",
    ):
        """Save scraping results to JSON file"""
        try:
            # Convert documents to serializable format
            serializable_docs = {}
            for ticker, doc_list in documents.items():
                serializable_docs[ticker] = [
                    {
                        "company_name": doc.company_name,
                        "company_ticker": doc.company_ticker,
                        "document_type": doc.document_type,
                        "filing_date": doc.filing_date,
                        "document_url": doc.document_url,
                        "file_name": doc.file_name,
                        "file_size": doc.file_size,
                        "description": doc.description,
                    }
                    for doc in doc_list
                ]

            with open(filename, "w") as f:
                json.dump(serializable_docs, f, indent=2)

            logger.info(f"Results saved to {filename}")

        except Exception as e:
            logger.error(f"Error saving results: {str(e)}")

    def generate_report(self, documents: Dict[str, List[Document]]) -> str:
        """Generate a summary report of scraped documents"""
        report = []
        report.append("SEDAR+ Document Scraping Report")
        report.append("=" * 50)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")

        total_documents = 0
        for ticker, doc_list in documents.items():
            report.append(f"Company: {ticker}")
            report.append(f"Documents found: {len(doc_list)}")
            total_documents += len(doc_list)

            doc_types = {}
            for doc in doc_list:
                doc_type = doc.document_type
                doc_types[doc_type] = doc_types.get(doc_type, 0) + 1

            for doc_type, count in doc_types.items():
                report.append(f"  - {doc_type}: {count}")
            report.append("")

        report.append(f"Total documents: {total_documents}")
        report.append(f"Companies processed: {len(documents)}")

        return "\n".join(report)

    def cleanup(self):
        """Clean up resources"""
        if hasattr(self, "driver"):
            self.driver.quit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()


if __name__ == "__main__":
    start_date = "2025-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")

    # Document types to focus on
    target_document_types = [
        "Annual Information Form",
        "Management Discussion and Analysis",
        "Financial Statements",
    ]

    try:
        with SEDARScraper(headless=True, download_dir="sedar_documents") as scraper:
            logger.info("Starting SEDAR+ document scraping...")

            # Scrape documents for all companies
            documents = scraper.scrape_all_companies(
                document_types=target_document_types,
                start_date=start_date,
                end_date=end_date,
                download_documents=True,
            )

            scraper.save_results_to_json(documents, "sedar_documents.json")

            report = scraper.generate_report(documents)
            with open("sedar_scraping_report.txt", "w") as f:
                f.write(report)

            logger.info("Scraping completed successfully!")
            print(report)

    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
