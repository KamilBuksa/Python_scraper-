import requests
from bs4 import BeautifulSoup
from datetime import datetime
from pymongo import MongoClient
from typing import Dict, Any, Optional
import logging

class BaseScraper:
    """Base class for all job board scrapers"""
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1.1 Safari/605.1.15",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",    
        "Accept-Language": "pl-PL,pl;q=0.9",
    }

    def __init__(self, mongodb_uri: str = 'mongodb://localhost:27017/'):
        """
        Initialize the scraper with MongoDB connection
        
        Args:
            mongodb_uri (str): MongoDB connection string
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Initializing scraper")
        
        # MongoDB connection
        try:
            self.client = MongoClient(mongodb_uri)
            self.db = self.client['JobMarket']
            self.collection = self.db['job_offers']
            self.logger.info("Connected to MongoDB")
        except Exception as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def scrape(self, url: str) -> Optional[str]:
        """
        Scrape the given URL and return the HTML content
        
        Args:
            url (str): URL to scrape
            
        Returns:
            Optional[str]: HTML content if successful, None otherwise
        """
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            self.logger.info(f"Successfully scraped URL: {url}")
            return response.text
        except requests.RequestException as e:
            self.logger.error(f"Error scraping {url}: {e}")
            return None

    def save_to_db(self, url: str, html_content: str, processed_data: Dict[str, Any] = None) -> bool:
        """
        Save the scraped data to MongoDB
        
        Args:
            url (str): Source URL
            html_content (str): Raw HTML content
            processed_data (Dict[str, Any]): Processed job offer data
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            document = {
                "url": url,
                "content": html_content,
                "date_scraped": datetime.now(),
                "processed_data": processed_data or {}
            }
            
            result = self.collection.insert_one(document)
            self.logger.info(f"Saved document to MongoDB with ID: {result.inserted_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to save to MongoDB: {e}")
            return False

    def parse_html(self, html_content: str) -> BeautifulSoup:
        """
        Parse HTML content using BeautifulSoup
        
        Args:
            html_content (str): Raw HTML content
            
        Returns:
            BeautifulSoup: Parsed HTML
        """
        return BeautifulSoup(html_content, 'html.parser')

    def process_job_offer(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Process job offer data from BeautifulSoup object
        This method should be implemented by child classes
        
        Args:
            soup (BeautifulSoup): Parsed HTML
            
        Returns:
            Dict[str, Any]: Processed job offer data
        """
        raise NotImplementedError("This method should be implemented by child classes")

    def __del__(self):
        """Cleanup method to close MongoDB connection"""
        try:
            self.client.close()
            self.logger.info("Closed MongoDB connection")
        except:
            pass 