from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup
import re
from datetime import datetime
from .base_scraper import BaseScraper

class PracujScraper(BaseScraper):
    """Scraper for pracuj.pl job board"""

    def __init__(self, mongodb_uri: str = 'mongodb://localhost:27017/'):
        super().__init__(mongodb_uri)
        self.base_url = "https://www.pracuj.pl"

    def get_job_urls(self, query: str = "python developer", page: int = 1) -> List[str]:
        """
        Get list of job offer URLs from search results
        
        Args:
            query (str): Search query
            page (int): Page number
            
        Returns:
            List[str]: List of job offer URLs
        """
        search_url = f"{self.base_url}/praca/{query};kw?pn={page}"
        html_content = self.scrape(search_url)
        if not html_content:
            return []
            
        soup = self.parse_html(html_content)
        job_links = soup.find_all("a", {"class": "offer-details__title-link"})
        return [link.get("href") for link in job_links if link.get("href")]

    def extract_salary(self, text: str) -> Dict[str, Optional[float]]:
        """
        Extract salary range from text
        
        Args:
            text (str): Salary text
            
        Returns:
            Dict[str, Optional[float]]: Dictionary with min and max salary
        """
        result = {
            "salary_min": None,
            "salary_max": None,
            "currency": "PLN"
        }
        
        if not text:
            return result
            
        # Common patterns: "10 000 - 15 000 PLN", "od 10 000 PLN", "do 15 000 PLN"
        text = text.lower().replace(" ", "")
        numbers = re.findall(r'\d+(?:\s*\d+)*', text)
        
        if len(numbers) >= 2:
            result["salary_min"] = float(numbers[0])
            result["salary_max"] = float(numbers[1])
        elif "od" in text and numbers:
            result["salary_min"] = float(numbers[0])
        elif "do" in text and numbers:
            result["salary_max"] = float(numbers[0])
            
        # Extract currency
        currencies = re.findall(r'(pln|eur|usd)', text)
        if currencies:
            result["currency"] = currencies[0].upper()
            
        return result

    def extract_skills(self, description: str) -> List[str]:
        """
        Extract technical skills from job description
        
        Args:
            description (str): Job description
            
        Returns:
            List[str]: List of extracted skills
        """
        # Common programming languages and technologies
        common_skills = [
            "python", "java", "javascript", "js", "typescript", "c++", "c#",
            "php", "ruby", "swift", "kotlin", "go", "rust", "scala",
            "react", "angular", "vue", "node", "django", "flask", "spring",
            "docker", "kubernetes", "aws", "azure", "gcp", "sql", "nosql",
            "mongodb", "postgresql", "mysql", "redis", "elasticsearch"
        ]
        
        found_skills = []
        description_lower = description.lower()
        
        for skill in common_skills:
            # Match whole words only
            pattern = r'\b' + re.escape(skill) + r'\b'
            if re.search(pattern, description_lower):
                found_skills.append(skill)
                
        return found_skills

    def process_job_offer(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Process job offer data from BeautifulSoup object
        
        Args:
            soup (BeautifulSoup): Parsed HTML
            
        Returns:
            Dict[str, Any]: Processed job offer data
        """
        data = {
            "title": "",
            "company": "",
            "location": "",
            "salary_min": None,
            "salary_max": None,
            "currency": "PLN",
            "skills": [],
            "experience_level": "",
            "description": "",
            "employment_type": "",
            "remote_options": "",
            "post_date": None
        }
        
        try:
            # Basic job info
            title_elem = soup.find("h1", {"class": "offer-viewkHIhn3"})
            if title_elem:
                data["title"] = title_elem.text.strip()
            
            company_elem = soup.find("h2", {"class": "employer-name"})
            if company_elem:
                data["company"] = company_elem.text.strip()
            
            location_elem = soup.find("span", {"class": "workplace__location"})
            if location_elem:
                data["location"] = location_elem.text.strip()
            
            # Salary
            salary_elem = soup.find("span", {"class": "salary"})
            if salary_elem:
                salary_data = self.extract_salary(salary_elem.text)
                data.update(salary_data)
            
            # Job description
            description_elem = soup.find("div", {"class": "description"})
            if description_elem:
                description_text = description_elem.text.strip()
                data["description"] = description_text
                data["skills"] = self.extract_skills(description_text)
            
            # Additional details
            details_elem = soup.find("dl", {"class": "offer-details"})
            if details_elem:
                dt_elements = details_elem.find_all("dt")
                dd_elements = details_elem.find_all("dd")
                
                for dt, dd in zip(dt_elements, dd_elements):
                    key = dt.text.strip().lower()
                    value = dd.text.strip()
                    
                    if "doświadczenie" in key:
                        data["experience_level"] = value
                    elif "umow" in key:  # "umowa"
                        data["employment_type"] = value
                    elif "prac" in key and "zdaln" in key:  # "praca zdalna"
                        data["remote_options"] = value
            
            # Post date
            date_elem = soup.find("span", {"class": "offer-date"})
            if date_elem:
                date_text = date_elem.text.strip()
                try:
                    data["post_date"] = datetime.strptime(date_text, "%d.%m.%Y")
                except ValueError:
                    self.logger.warning(f"Could not parse date: {date_text}")
            
        except Exception as e:
            self.logger.error(f"Error processing job offer: {e}")
            
        return data

    def scrape_job_offer(self, url: str) -> bool:
        """
        Scrape single job offer and save to database
        
        Args:
            url (str): Job offer URL
            
        Returns:
            bool: True if successful, False otherwise
        """
        html_content = self.scrape(url)
        if not html_content:
            return False
            
        soup = self.parse_html(html_content)
        processed_data = self.process_job_offer(soup)
        
        return self.save_to_db(url, html_content, processed_data) 