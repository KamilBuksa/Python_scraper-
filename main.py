from book_scraper.scraper import BookScraper
import time
import sys
import random

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