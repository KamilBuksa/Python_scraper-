from job_scraper import JobScraper
from db_manager import DatabaseManager
import argparse
from datetime import datetime

def main():
    # Konfiguracja parsera argumentów
    parser = argparse.ArgumentParser(description='Scraper ofert pracy z trojmiasto.pl')
    parser.add_argument('-p', '--pages', type=int, default=6,
                      help='Liczba stron do przescrapowania (domyślnie: 6)')
    parser.add_argument('-m', '--max-jobs', type=int, default=60,
                      help='Maksymalna liczba ofert do pobrania (domyślnie: 60)')
    parser.add_argument('-o', '--output', type=str,
                      default=f'jobs_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
                      help='Nazwa pliku wyjściowego CSV (domyślnie: jobs_YYYYMMDD_HHMMSS.csv)')
    parser.add_argument('--force-scrape', action='store_true',
                      help='Wymuś ponowne scrapowanie nawet jeśli dane istnieją w bazie')
    
    args = parser.parse_args()
    
    try:
        # Inicjalizacja połączenia z bazą danych
        db = DatabaseManager()
        
        # Sprawdzamy czy mamy już dane w bazie
        existing_jobs = db.get_jobs_count()
        
        if existing_jobs == 0 or args.force_scrape:
            # Jeśli baza jest pusta lub wymuszono scraping, pobieramy nowe dane
            print("Uruchamiam scraper ofert pracy...")
            print(f"Pobieram maksymalnie {args.max_jobs} ofert z {args.pages} stron")
            
            scraper = JobScraper()
            scraper.scrape_jobs(num_pages=args.pages, max_jobs=args.max_jobs)
            
            # Zapisujemy wyniki do bazy
            saved_count = db.save_jobs(scraper.jobs)
            print(f"\nZapisano {saved_count} ofert w bazie danych")
        else:
            print(f"Znaleziono {existing_jobs} ofert w bazie danych")
            print("Użyj --force-scrape aby wymusić ponowne pobranie danych")
        
        # Eksportujemy dane z bazy do CSV
        exported_count = db.export_to_csv(args.output)
        print(f"Wyeksportowano {exported_count} ofert do {args.output}")
        
        # Wyświetlenie podsumowania
        print("\nPodsumowanie scrapowania:")
        print("-" * 50)
        summary = db.get_jobs_summary()
        
        print(f"Całkowita liczba ofert w bazie: {summary['total_jobs']}")
        
        print("\nRodzaje umów:")
        for contract_type, count in summary['contract_types'].items():
            print(f"- {contract_type}: {count}")
            
        print("\nTop 10 lokalizacji:")
        for location, count in summary['jobs_by_location'].items():
            print(f"- {location}: {count}")
            
        print("\nTryby pracy:")
        for work_mode, count in summary['work_modes'].items():
            print(f"- {work_mode}: {count}")
            
        print("\nBranże:")
        for industry, count in summary['industries'].items():
            print(f"- {industry}: {count}")
            
        print("\nPoziomy stanowisk:")
        for level, count in summary['position_levels'].items():
            print(f"- {level}: {count}")
            
        if 'benefits_distribution' in summary and summary['benefits_distribution']:
            print("\nRozkład benefitów:")
            for benefit, count in summary['benefits_distribution'].items():
                print(f"- {benefit}: {count}")
                
        if 'advanced_salary_stats' in summary:
            print("\nZaawansowane statystyki wynagrodzeń (PLN):")
            stats = summary['advanced_salary_stats']
            print(f"- Średni zakres wynagrodzeń: {stats['mean_min_salary']:,} - {stats['mean_max_salary']:,}")
            print(f"- Mediana wynagrodzeń: {stats['median_min_salary']:,} - {stats['median_max_salary']:,}")
            print(f"- Odchylenie standardowe: {stats['std_min_salary']:,} - {stats['std_max_salary']:,}")
            
        if 'work_time_analysis' in summary:
            print("\nRozkład czasu pracy:")
            for time, count in summary['work_time_analysis'].items():
                print(f"- {time}: {count}")
                
        if 'industry_trends' in summary:
            print("\nTrendy w branżach:")
            print(f"- Najbardziej aktywny dzień: {summary['industry_trends']['most_active_day']}")
            print("- Najszybciej rosnące branże:")
            for industry in summary['industry_trends']['top_growing_industries']:
                print(f"  * {industry}")
                
        if 'location_analysis' in summary:
            print("\nAnaliza lokalizacji:")
            print(f"- Unikalne lokalizacje: {summary['location_analysis']['unique_locations']}")
            print("- Rozkład lokalizacji:")
            dist = summary['location_analysis']['location_distribution']
            print(f"  * Górne 25% lokalizacji: {dist['top_25_percent']}")
            print(f"  * Środkowe 50% lokalizacji: {dist['middle_50_percent']}")
            print(f"  * Dolne 25% lokalizacji: {dist['bottom_25_percent']}")
            
    except Exception as e:
        print(f"Wystąpił błąd: {e}")
        return 1
        
    return 0

if __name__ == "__main__":
    exit(main()) 