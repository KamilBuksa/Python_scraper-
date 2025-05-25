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
        
        print("\nTop Contract Types:")
        for contract_type, count in summary['contract_types'].items():
            print(f"- {contract_type}: {count}")
            
        print("\nTop 10 locations:")
        for location, count in summary['jobs_by_location'].items():
            print(f"- {location}: {count}")
            
        print("\nWork modes:")
        for work_mode, count in summary['work_modes'].items():
            print(f"- {work_mode}: {count}")
            
        print("\nIndustries:")
        for industry, count in summary['industries'].items():
            print(f"- {industry}: {count}")
            
        print("\nPosition levels:")
        for level, count in summary['position_levels'].items():
            print(f"- {level}: {count}")
            
        if 'benefits_distribution' in summary and summary['benefits_distribution']:
            print("\nBenefits distribution:")
            for benefit, count in summary['benefits_distribution'].items():
                print(f"- {benefit}: {count}")
                
        if 'advanced_salary_stats' in summary:
            print("\nAdvanced Salary Statistics (PLN):")
            stats = summary['advanced_salary_stats']
            print(f"- Mean salary range: {stats['mean_min_salary']:,} - {stats['mean_max_salary']:,}")
            print(f"- Median salary range: {stats['median_min_salary']:,} - {stats['median_max_salary']:,}")
            print(f"- Salary standard deviation: {stats['std_min_salary']:,} - {stats['std_max_salary']:,}")
            
        if 'work_time_analysis' in summary:
            print("\nWork Time Distribution:")
            for time, count in summary['work_time_analysis'].items():
                print(f"- {time}: {count}")
                
        if 'industry_trends' in summary:
            print("\nIndustry Trends:")
            print(f"- Most active day: {summary['industry_trends']['most_active_day']}")
            print("- Top growing industries:")
            for industry in summary['industry_trends']['top_growing_industries']:
                print(f"  * {industry}")
                
        if 'location_analysis' in summary:
            print("\nLocation Analysis:")
            print(f"- Unique locations: {summary['location_analysis']['unique_locations']}")
            print("- Location distribution:")
            dist = summary['location_analysis']['location_distribution']
            print(f"  * Top 25% locations: {dist['top_25_percent']}")
            print(f"  * Middle 50% locations: {dist['middle_50_percent']}")
            print(f"  * Bottom 25% locations: {dist['bottom_25_percent']}")
            
    except Exception as e:
        print(f"An error occurred: {e}")
        return 1
        
    return 0

if __name__ == "__main__":
    exit(main()) 