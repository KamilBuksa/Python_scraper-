import requests
from bs4 import BeautifulSoup
from time import sleep
import pandas as pd
from datetime import datetime
import random
import re

class JobScraper:
    def __init__(self):
        self.base_url = "https://ogloszenia.trojmiasto.pl/praca-zatrudnie/"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7',
        }
        self.jobs = []
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def get_page(self, page=1):
        """Fetch a single page of job listings"""
        try:
            if page > 1:
                url = f"{self.base_url}?strona={page}"
            else:
                url = self.base_url
                
            print(f"Scraping page {page}")
            response = self.session.get(url)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            print(f"Error fetching page {page}: {e}")
            return None

    def parse_job_listing(self, job_item):
        """Parse a single job listing"""
        try:
            # Find the job title and URL
            title_element = job_item.find('h2')
            if not title_element:
                return None
                
            title = title_element.get_text(strip=True)
            url = title_element.find('a')['href'] if title_element.find('a') else None
            if not url:
                return None
            if not url.startswith('http'):
                url = 'https://ogloszenia.trojmiasto.pl' + url

            # Get basic job details
            location = None
            location_div = job_item.find('div', class_='list__location')
            if location_div:
                location = location_div.get_text(strip=True)

            company = None
            company_div = job_item.find('div', class_='list__company')
            if company_div:
                company = company_div.get_text(strip=True)

            # Get salary if available
            salary = None
            salary_div = job_item.find('div', class_='list__salary')
            if salary_div:
                salary = salary_div.get_text(strip=True)

            # Get posting date
            date_posted = None
            date_div = job_item.find('div', class_='list__date')
            if date_div:
                date_posted = date_div.get_text(strip=True)

            # Get job details from the job's page
            job_details = self.get_job_description(url) if url else {}

            # Combine all information
            job_data = {
                'title': title,
                'location': location or job_details.get('location'),
                'url': url,
                'salary': salary or job_details.get('salary'),
                'date_posted': date_posted or job_details.get('date_posted'),
                'date_updated': job_details.get('date_updated'),
                'work_mode': job_details.get('work_mode'),
                'contract_type': job_details.get('contract_type'),
                'work_time': job_details.get('work_time'),
                'industry': job_details.get('industry'),
                'position_level': job_details.get('position_level'),
                'experience_required': job_details.get('experience_required'),
                'description': job_details.get('description'),
                'scraped_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            return job_data

        except Exception as e:
            print(f"Error parsing job listing: {e}")
            return None

    def get_job_description(self, url):
        """Get full job description from the job's page"""
        try:
            sleep(random.uniform(0, 0.5))  # Zmniejszony czas oczekiwania
            response = self.session.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            job_details = {}
            
            # Find the main job description content
            content = soup.find('div', class_='ogl__description')
            if content:
                description_text = content.get_text(strip=True)
                job_details['description'] = description_text
                
                # Extract salary range from description
                salary_match = re.search(r'Kwota od (\d+)\s*(?:tysięcy|tys\.?)\s*do\s*(\d+)\s*(?:tysięcy|tys\.?)', description_text)
                if salary_match:
                    job_details['salary_range'] = {
                        'min': int(salary_match.group(1)) * 1000,
                        'max': int(salary_match.group(2)) * 1000,
                        'currency': 'PLN'
                    }
                
                # Extract working hours
                hours_match = re.search(r'(?:Około|Ok\.|około)\s*(\d+)\s*h(?:odzin)?\s*(?:w|na)\s*miesiąc', description_text)
                if hours_match:
                    job_details['monthly_hours'] = int(hours_match.group(1))
                
                # Extract work schedule
                schedule_match = re.search(r'(?:Zjazdy|System|Praca)\s*(\d+/\d+)(?:\s*(?:tygodnie|dni))?', description_text)
                if schedule_match:
                    job_details['work_schedule'] = schedule_match.group(1)
            
            # Extract additional details from oglDetails panel
            details_panel = soup.find('div', class_='oglDetails')
            if details_panel:
                # Find all field containers
                fields = details_panel.find_all('div', class_='oglField')
                for field in fields:
                    # Get field name and value
                    name_div = field.find('div', class_='oglField__name')
                    value_divs = field.find_all('div', class_='oglField__value')
                    
                    if name_div:
                        field_name = name_div.get_text(strip=True).lower()
                        field_values = [v.get_text(strip=True) for v in value_divs]
                        field_value = ', '.join(field_values) if field_values else None
                        
                        # Map field names to our structure
                        if 'branża' in field_name or 'kategoria' in field_name:
                            job_details['industry'] = field_value
                        elif 'poziom stanowiska' in field_name:
                            job_details['position_level'] = field_value
                        elif 'wymagane doświadczenie' in field_name:
                            job_details['experience_required'] = field_value
                        elif 'wymiar pracy' in field_name:
                            job_details['work_time'] = field_value
                        elif 'rodzaj umowy' in field_name:
                            job_details['contract_type'] = field_value
                        elif 'charakter pracy' in field_name:
                            job_details['work_mode'] = field_value
                        elif 'praca za granicą' in field_name:
                            job_details['foreign_job'] = field_value.lower() == 'tak'
            
            # Get salary information if available
            salary_div = soup.find('div', class_='list__salary')
            if salary_div:
                job_details['salary'] = salary_div.get_text(strip=True)
            
            # Get location information with full address
            location_div = soup.find('span', class_='topBar__item--address')
            if location_div:
                location_text = location_div.get_text(strip=True)
                # Split into city and street
                location_parts = location_text.split('\n')
                if len(location_parts) > 1:
                    job_details['city'] = location_parts[0].strip()
                    job_details['street'] = location_parts[1].strip()
                job_details['location'] = location_text
            
            # Get company name
            company_div = soup.find('div', class_='list__company')
            if company_div:
                job_details['company'] = company_div.get_text(strip=True)
            
            # Get posting date, expiration date and offer ID
            stats_div = soup.find('div', class_='oglStats')
            if stats_div:
                date_elements = stats_div.find_all('p')
                for date_el in date_elements:
                    date_text = date_el.get_text(strip=True)
                    if 'Data dodania' in date_text:
                        job_details['date_posted'] = date_el.find('span').get_text(strip=True)
                    elif 'Aktualizacja' in date_text:
                        job_details['date_updated'] = date_el.find('span').get_text(strip=True)
                    elif 'ID oferty' in date_text:
                        job_details['offer_id'] = date_el.find('span').get_text(strip=True)
            
            return job_details
            
        except Exception as e:
            print(f"Error getting job description: {e}")
            return {}

    def scrape_jobs(self, num_pages=5, max_jobs=10):
        """Scrape specified number of pages"""
        print(f"Starting to scrape jobs from trojmiasto.pl (max {max_jobs} jobs)...")
        
        total_jobs_scraped = 0
        errors = 0
        
        for page in range(1, num_pages + 1):
            if total_jobs_scraped >= max_jobs:
                break
                
            try:
                soup = self.get_page(page)
                if not soup:
                    print(f"\nSkipping page {page} due to error")
                    continue
                    
                # Find all job listings on the page
                job_listings = soup.find_all('div', class_='list__item')
                print(f"\nFound {len(job_listings)} jobs on page {page}")
                
                for job_item in job_listings:
                    if total_jobs_scraped >= max_jobs:
                        break
                        
                    try:
                        job_data = self.parse_job_listing(job_item)
                        if job_data:
                            self.jobs.append(job_data)
                            total_jobs_scraped += 1
                            print(f"Progress: {total_jobs_scraped}/{max_jobs} jobs scraped", end='\r')
                    except Exception as e:
                        errors += 1
                        print(f"\nError parsing job listing: {e}")
                        continue
                    
                sleep(random.uniform(0, 0.5))  # Zmniejszony czas oczekiwania
                
            except Exception as e:
                errors += 1
                print(f"\nError scraping page {page}: {e}")
                continue
            
        print(f"\nScraped {total_jobs_scraped} jobs in total")
        if errors > 0:
            print(f"Encountered {errors} errors during scraping")
        return self.jobs

    def export_to_csv(self):
        """Export scraped jobs to CSV file"""
        if not self.jobs:
            print("No jobs to export")
            return
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"jobs_{timestamp}.csv"
        
        df = pd.DataFrame(self.jobs)
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"Exported {len(self.jobs)} jobs to {filename}")
        return filename

    def save_to_csv(self, filename='jobs.csv'):
        """Save scraped jobs to CSV file"""
        if not self.jobs:
            print("No jobs to save!")
            return
            
        df = pd.DataFrame(self.jobs)
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"Saved {len(self.jobs)} jobs to {filename}")

    def get_jobs_summary(self):
        """Return basic statistics about scraped jobs"""
        if not self.jobs:
            return "No jobs scraped yet!"
            
        df = pd.DataFrame(self.jobs)
        
        summary = {
            'total_jobs': len(df),
            'unique_companies': df['company'].nunique(),
            'unique_locations': df['location'].nunique(),
            'top_sources': df['source'].value_counts().head(5).to_dict(),
            'jobs_by_location': df['location'].value_counts().head(10).to_dict(),
            'most_active_companies': df['company'].value_counts().head(5).to_dict(),
            'contract_types': df['contract_type'].value_counts().to_dict(),
            'work_modes': df['work_mode'].value_counts().to_dict(),
            'industries': df['industry'].value_counts().to_dict(),
            'position_levels': df['position_level'].value_counts().to_dict(),
            'benefits_distribution': pd.DataFrame(df['benefits'].tolist()).stack().value_counts().to_dict()
        }
        return summary 