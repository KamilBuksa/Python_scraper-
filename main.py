from job_scraper import JobScraper
from db_manager import DatabaseManager
import argparse
from datetime import datetime

def main():
    # Konfiguracja parsera argumentów
    parser = argparse.ArgumentParser(description='Scrape job listings from pracawgdansku.com.pl')
    parser.add_argument('-p', '--pages', type=int, default=2,
                      help='Number of pages to scrape (default: 2)')
    parser.add_argument('-m', '--max-jobs', type=int, default=10,
                      help='Maximum number of jobs to scrape (default: 10)')
    parser.add_argument('-o', '--output', type=str,
                      default=f'jobs_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
                      help='Output CSV file name (default: jobs_YYYYMMDD_HHMMSS.csv)')
    parser.add_argument('--force-scrape', action='store_true',
                      help='Force scraping even if data exists in database')
    
    args = parser.parse_args()
    
    try:
        # Inicjalizacja połączenia z bazą danych
        db = DatabaseManager()
        
        # Sprawdzamy czy mamy już dane w bazie
        existing_jobs = db.get_jobs_count()
        
        if existing_jobs == 0 or args.force_scrape:
            # Jeśli baza jest pusta lub wymuszono scraping, pobieramy nowe dane
            print("Starting job scraper...")
            print(f"Will scrape max {args.max_jobs} jobs from up to {args.pages} pages")
            
            scraper = JobScraper()
            scraper.scrape_jobs(num_pages=args.pages, max_jobs=args.max_jobs)
            
            # Zapisujemy wyniki do bazy
            saved_count = db.save_jobs(scraper.jobs)
            print(f"\nSaved {saved_count} jobs to database")
        else:
            print(f"Found {existing_jobs} jobs in database")
            print("Use --force-scrape to force new data collection")
        
        # Eksportujemy dane z bazy do CSV
        exported_count = db.export_to_csv(args.output)
        print(f"Exported {exported_count} jobs to {args.output}")
        
        # Wyświetlenie podsumowania
        print("\nScraping Summary:")
        print("-" * 50)
        summary = db.get_jobs_summary()
        
        print(f"Total jobs in database: {summary['total_jobs']}")
        
        if summary['average_monthly_hours']:
            print(f"\nAverage monthly hours: {summary['average_monthly_hours']}")
            
        if summary['salary_range_statistics']:
            print("\nSalary statistics (PLN):")
            stats = summary['salary_range_statistics']
            print(f"- Minimum salary: {stats['min']:,.2f}")
            print(f"- Maximum salary: {stats['max']:,.2f}")
            print(f"- Average minimum: {stats['avg_min']:,.2f}")
            print(f"- Average maximum: {stats['avg_max']:,.2f}")
            
        print("\nTop 10 locations:")
        for location, count in summary['jobs_by_location'].items():
            print(f"- {location}: {count}")
            
        print("\nContract types:")
        for contract_type, count in summary['contract_types'].items():
            print(f"- {contract_type}: {count}")
            
        print("\nWork modes:")
        for work_mode, count in summary['work_modes'].items():
            print(f"- {work_mode}: {count}")
            
        print("\nIndustries:")
        for industry, count in summary['industries'].items():
            print(f"- {industry}: {count}")
            
        print("\nPosition levels:")
        for level, count in summary['position_levels'].items():
            print(f"- {level}: {count}")
            
        print("\nBenefits distribution:")
        for benefit, count in summary['benefits_distribution'].items():
            print(f"- {benefit}: {count}")
            
        print("\nSalary statistics:")
        for salary, count in summary['salary_stats'].items():
            print(f"- {salary}: {count}")
            
    except Exception as e:
        print(f"An error occurred: {e}")
        return 1
        
    return 0

if __name__ == "__main__":
    exit(main()) 