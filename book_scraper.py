import requests
from bs4 import BeautifulSoup
from datetime import datetime
from pymongo import MongoClient
import re
from typing import Dict, Any, List, Optional
import time
import random
import json
import sys

class BookScraper:
    """Scraper for empik.com book store"""
    
    def __init__(self, mongodb_uri: str = 'mongodb://localhost:27017/'):
        """Initialize scraper with MongoDB connection"""
        self.base_url = "https://www.empik.com"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://www.empik.com/"
        }
        
        # Configure session
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # MongoDB connection
        self.client = MongoClient(mongodb_uri)
        self.db = self.client['book_store']
        self.collection = self.db['books']
        self.html_collection = self.db['raw_html']
        print("Connected to MongoDB")

    def extract_apollo_state(self, html_content: str) -> Optional[Dict]:
        """Extract Apollo state JSON from HTML"""
        try:
            apollo_match = re.search(r'window\.__APOLLO_STATE__\s*=\s*({.*?});', html_content, re.DOTALL)
            if apollo_match:
                apollo_state = json.loads(apollo_match.group(1))
                return apollo_state
            return None
        except Exception as e:
            print(f"Error extracting Apollo state: {e}")
            return None

    def extract_product_data(self, apollo_state: Dict) -> Dict[str, Any]:
        """Extract product data from Apollo state"""
        data = {
            "product_id": "",
            "title": "",
            "authors": [],
            "price": None,
            "original_price": None,
            "rating": None,
            "ratings_count": None,
            "description": "",
            "categories": [],
            "publisher": "",
            "publish_date": None,
            "cover_urls": {},
            "availability": "",
            "product_type": "",
            "binding_type": "",  # subtype in Apollo state
            "date_scraped": datetime.now()
        }
        
        try:
            # Find the product object
            product_key = next(key for key in apollo_state.keys() if key.startswith("Product:"))
            product = apollo_state[product_key]
            
            # Basic information
            data["product_id"] = product["id"]
            base_info = product["baseInformation"]
            
            # Title and URL
            data["title"] = base_info["name"]
            data["url"] = base_info["selfUrl"]
            
            # Authors
            if "smartAuthor" in base_info:
                for author_ref in base_info["smartAuthor"]:
                    author_key = author_ref["__ref"]
                    if author_key in apollo_state:
                        author_data = apollo_state[author_key]
                        data["authors"].append({
                            "name": author_data["name"],
                            "link": author_data["link"]
                        })
            
            # Categories
            if "categoryInfo" in base_info:
                for cat in base_info["categoryInfo"]["categories"]:
                    data["categories"].append({
                        "id": cat["id"],
                        "name": cat["name"],
                        "url": cat["url"]
                    })
            
            # Cover images
            if "cover" in base_info:
                data["cover_urls"] = {
                    "small": base_info["cover"]["small"],
                    "medium": base_info["cover"]["medium"],
                    "large": base_info["cover"]["large"]
                }
            
            # Rating
            if "rating" in base_info:
                data["rating"] = base_info["rating"]["score"]
                data["ratings_count"] = base_info["rating"]["count"]
            
            # Product type and binding
            data["product_type"] = base_info["type"]
            data["binding_type"] = base_info["subtype"]
            
            # Details information
            if "detailsInformation" in product:
                details = product["detailsInformation"]
                
                # Store availability
                if "storeAvailability" in details:
                    data["available_in_stores"] = details["storeAvailability"]["isAvailableInAnyStore"]
                
                # Additional attributes
                if "attributes" in details and "short" in details["attributes"]:
                    for attr in details["attributes"]["short"]:
                        label = attr["label"].lower()
                        if attr["values"]:
                            value = attr["values"][0]["value"]
                            
                            if label == "wydawnictwo":
                                data["publisher"] = value
                            elif label == "data premiery":
                                try:
                                    data["publish_date"] = datetime.strptime(value, "%Y-%m-%d")
                                except:
                                    data["publish_date"] = value
            
            # Offer information
            if "bestOffer" in product:
                offer_ref = product["bestOffer"]["__ref"]
                if offer_ref in apollo_state:
                    offer = apollo_state[offer_ref]
                    if "availability" in offer:
                        data["availability"] = offer["availability"]["name"]
            
            return data
            
        except Exception as e:
            print(f"Error extracting product data: {e}")
            return data

    def process_book(self, url: str) -> bool:
        """Process single book and save to database"""
        try:
            # Add random delay
            time.sleep(random.uniform(2, 3))
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            # Save raw HTML to MongoDB only
            html_doc = {
                "url": url,
                "html": response.text,
                "saved_at": datetime.now()
            }
            self.html_collection.update_one(
                {"url": url},
                {"$set": html_doc},
                upsert=True
            )
            
            # Extract Apollo state
            apollo_state = self.extract_apollo_state(response.text)
            if apollo_state:
                # Extract and save product data
                product_data = self.extract_product_data(apollo_state)
                if product_data:
                    self.collection.update_one(
                        {"product_id": product_data["product_id"]},
                        {"$set": product_data},
                        upsert=True
                    )
                    print(f"Saved book: {product_data['title']}")
                    return True
            
            print(f"No Apollo state data found for {url}")
            return False
            
        except Exception as e:
            print(f"Error processing book {url}: {e}")
            return False

    def get_book_urls(self, category: str, page: int = 1, limit: int = 10) -> List[str]:
        """Get book URLs from category page"""
        url = f"{self.base_url}/{category}"
        
        params = {
            "qtype": "facetForm",
            "searchCategory": category.split(",")[1],
            "hideUnavailable": "true"
        }
        
        if page > 1:
            params["page"] = str(page)
        
        try:
            # Add random delay
            time.sleep(random.uniform(1, 2))
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all product tiles
            product_tiles = soup.select("div.ta-product-tile")
            if not product_tiles:
                product_tiles = soup.select("div.search-content div.js-reco-product")
            
            urls = []
            for tile in product_tiles:
                # Try different possible link classes
                link = tile.find("a", class_="ta-product-title")
                if not link:
                    link = tile.find("a", class_="seoTitle")
                if not link:
                    link = tile.find("a", href=True)
                
                if link and link.get("href"):
                    book_url = link["href"]
                    if not book_url.startswith("http"):
                        book_url = self.base_url + book_url
                    urls.append(book_url)
                    
                    # Break if we have enough URLs
                    if len(urls) >= limit:
                        break
            
            print(f"Found {len(urls)} books on page {page} (limited to {limit})")
            return urls[:limit]  # Ensure we don't return more than limit
            
        except Exception as e:
            print(f"Error getting book URLs: {e}")
            return []

    def __del__(self):
        """Cleanup connections"""
        try:
            self.session.close()
            print("Closed session")
        except:
            pass
            
        try:
            self.client.close()
            print("Closed MongoDB connection")
        except:
            pass

def main():
    try:
        # Initialize scraper
        scraper = BookScraper()
        
        # Categories from empik.com (format: "path,category_id,s")
        categories = [
            "ksiazki/kryminal-sensacja-thriller,3175,s"  # Start with just one category
        ]
        
        books_processed = 0
        max_books = 10
        
        # Scrape each category
        for category in categories:
            print(f"\nScrapowanie kategorii: {category}")
            
            # Get books from first 2 pages
            for page in range(1, 3):
                if books_processed >= max_books:
                    break
                    
                print(f"\nPrzetwarzanie strony {page}")
                
                # Get book URLs (limit per page to remaining books)
                remaining_books = max_books - books_processed
                book_urls = scraper.get_book_urls(
                    category=category, 
                    page=page,
                    limit=remaining_books
                )
                
                if not book_urls:
                    print(f"Nie znaleziono książek na stronie {page}")
                    continue
                
                # Process each book
                for url in book_urls:
                    if books_processed >= max_books:
                        break
                        
                    print(f"\nPrzetwarzanie książki {books_processed + 1} z {max_books}: {url}")
                    success = scraper.process_book(url)
                    if success:
                        books_processed += 1
                    else:
                        print(f"Nie udało się przetworzyć książki: {url}")
                    
                    # Random delay between books (2-4 seconds)
                    time.sleep(random.uniform(2, 4))
                
                if books_processed >= max_books:
                    break
                    
                # Longer delay between pages (4-6 seconds)
                time.sleep(random.uniform(4, 6))
            
            if books_processed >= max_books:
                break
                
            # Even longer delay between categories (6-8 seconds)
            time.sleep(random.uniform(6, 8))
            
        print(f"\nZakończono scrapowanie. Przetworzono {books_processed} książek.")
            
    except KeyboardInterrupt:
        print("\nPrzerwano scrapowanie przez użytkownika")
        try:
            scraper.close()
        except:
            pass
        sys.exit(0)
    except Exception as e:
        print(f"\nWystąpił błąd: {e}")
        try:
            scraper.close()
        except:
            pass
        sys.exit(1)

if __name__ == "__main__":
    main() 