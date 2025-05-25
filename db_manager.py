from pymongo import MongoClient
from datetime import datetime
import pandas as pd
import numpy as np

class DatabaseManager:
    def __init__(self):
        print("Inicjalizacja połączenia z MongoDB...")
        # Tworzymy połączenie z lokalną bazą MongoDB
        self.client = MongoClient('mongodb://localhost:27017/')
        # Tworzymy/wybieramy bazę danych
        self.db = self.client['job_scraper_db']
        # Tworzymy/wybieramy kolekcję
        self.jobs = self.db['jobs']
        
    def save_jobs(self, jobs_data):
        """Zapisuje listę ofert pracy do bazy danych"""
        if not jobs_data:
            return 0
            
        saved_count = 0
        for job in jobs_data:
            # Dodajemy datę zapisu
            job['saved_to_db'] = datetime.now()
            
            # Sprawdzamy czy oferta już istnieje (po URL)
            existing_job = self.jobs.find_one({'url': job['url']})
            
            if existing_job:
                # Aktualizujemy istniejącą ofertę
                self.jobs.update_one(
                    {'url': job['url']},
                    {'$set': job}
                )
            else:
                # Dodajemy nową ofertę
                self.jobs.insert_one(job)
            saved_count += 1
            
        print(f"Zapisano {saved_count} ofert w bazie danych")
        return saved_count
        
    def get_jobs_count(self):
        """Zwraca liczbę ofert w bazie danych"""
        return self.jobs.count_documents({})
        
    def export_to_csv(self, filename):
        """Eksportuje dane z bazy do pliku CSV"""
        jobs_data = list(self.jobs.find({}, {'_id': 0, 'company': 0}))  # pomijamy pola _id i company
        if jobs_data:
            df = pd.DataFrame(jobs_data)
            df.to_csv(filename, index=False, encoding='utf-8')
            print(f"Wyeksportowano {len(jobs_data)} ofert do pliku {filename}")
            return len(jobs_data)
        return 0
        
    def get_jobs_summary(self):
        """Zwraca podsumowanie ofert z bazy danych"""
        jobs_data = list(self.jobs.find({}, {'_id': 0}))
        df = pd.DataFrame(jobs_data)
        
        total_jobs = len(df)
        
        # Basic statistics
        summary = {
            'total_jobs': total_jobs,
            'contract_types': df['contract_type'].value_counts().to_dict(),
            'jobs_by_location': df['location'].value_counts().head(10).to_dict(),
            'work_modes': df['work_mode'].value_counts().to_dict(),
            'industries': df['industry'].value_counts().to_dict(),
            'position_levels': df['position_level'].value_counts().to_dict(),
            'benefits_distribution': df['benefits'].apply(pd.Series).stack().value_counts().to_dict() if 'benefits' in df else {},
            'salary_stats': df['salary'].value_counts().to_dict(),
        }
        
        # Advanced Pandas Statistics
        if 'salary_range' in df.columns:
            salary_df = pd.json_normalize(df['salary_range'].dropna())
            if not salary_df.empty:
                summary['advanced_salary_stats'] = {
                    'mean_min_salary': int(salary_df['min'].mean()),
                    'mean_max_salary': int(salary_df['max'].mean()),
                    'median_min_salary': int(salary_df['min'].median()),
                    'median_max_salary': int(salary_df['max'].median()),
                    'std_min_salary': int(salary_df['min'].std()),
                    'std_max_salary': int(salary_df['max'].std())
                }
        
        # Work Time Analysis
        if 'work_time' in df.columns:
            summary['work_time_analysis'] = df['work_time'].value_counts().to_dict()
        
        # Industry Trends
        if 'industry' in df.columns and 'date_posted' in df.columns:
            df['date_posted'] = pd.to_datetime(df['date_posted'], dayfirst=True)
            industry_by_date = df.groupby(['industry', df['date_posted'].dt.date]).size().unstack()
            summary['industry_trends'] = {
                'most_active_day': industry_by_date.sum().idxmax().strftime('%Y-%m-%d'),
                'top_growing_industries': industry_by_date.iloc[:, -1].nlargest(5).index.tolist()
            }
        
        # Location Analysis
        if 'location' in df.columns:
            location_stats = df['location'].value_counts()
            summary['location_analysis'] = {
                'unique_locations': len(location_stats),
                'top_10_locations': location_stats.head(10).to_dict(),
                'location_distribution': {
                    'top_25_percent': len(location_stats[location_stats >= location_stats.quantile(0.75)]),
                    'middle_50_percent': len(location_stats[location_stats.between(location_stats.quantile(0.25), location_stats.quantile(0.75), inclusive='both')]),
                    'bottom_25_percent': len(location_stats[location_stats <= location_stats.quantile(0.25)])
                }
            }
        
        return summary
        
    def __del__(self):
        """Zamykamy połączenie z bazą przy usuwaniu obiektu"""
        try:
            self.client.close()
            print("Zamknięto połączenie z bazą danych")
        except:
            pass 