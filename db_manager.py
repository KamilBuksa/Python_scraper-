from pymongo import MongoClient
from datetime import datetime
import pandas as pd

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
        jobs_data = list(self.jobs.find({}, {'_id': 0}))  # pomijamy pole _id
        if jobs_data:
            df = pd.DataFrame(jobs_data)
            df.to_csv(filename, index=False, encoding='utf-8')
            print(f"Wyeksportowano {len(jobs_data)} ofert do pliku {filename}")
            return len(jobs_data)
        return 0
        
    def get_jobs_summary(self):
        """Zwraca podsumowanie ofert z bazy danych"""
        total_jobs = self.jobs.count_documents({})
        unique_companies = len(self.jobs.distinct('company'))
        unique_locations = len(self.jobs.distinct('location'))
        
        # Top 5 źródeł ogłoszeń
        top_sources = {}
        for source in self.jobs.aggregate([
            {'$group': {'_id': '$source', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}},
            {'$limit': 5}
        ]):
            top_sources[source['_id']] = source['count']
            
        # Top 10 lokalizacji
        jobs_by_location = {}
        for loc in self.jobs.aggregate([
            {'$group': {'_id': '$location', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}},
            {'$limit': 10}
        ]):
            jobs_by_location[loc['_id']] = loc['count']
            
        # Top 5 firm
        most_active_companies = {}
        for company in self.jobs.aggregate([
            {'$group': {'_id': '$company', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}},
            {'$limit': 5}
        ]):
            most_active_companies[company['_id']] = company['count']
            
        # Statystyki typów umów
        contract_types = {}
        for ct in self.jobs.aggregate([
            {'$group': {'_id': '$contract_type', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}}
        ]):
            contract_types[ct['_id']] = ct['count']
            
        # Statystyki trybów pracy
        work_modes = {}
        for wm in self.jobs.aggregate([
            {'$group': {'_id': '$work_mode', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}}
        ]):
            work_modes[wm['_id']] = wm['count']
            
        # Statystyki branż
        industries = {}
        for ind in self.jobs.aggregate([
            {'$group': {'_id': '$industry', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}}
        ]):
            industries[ind['_id']] = ind['count']
            
        # Statystyki poziomów stanowisk
        position_levels = {}
        for pl in self.jobs.aggregate([
            {'$group': {'_id': '$position_level', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}}
        ]):
            position_levels[pl['_id']] = pl['count']
            
        # Statystyki benefitów
        benefits_distribution = {}
        for benefit in self.jobs.aggregate([
            {'$unwind': '$benefits'},
            {'$group': {'_id': '$benefits', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}}
        ]):
            benefits_distribution[benefit['_id']] = benefit['count']
            
        # Statystyki wynagrodzeń
        salary_stats = {}
        for salary in self.jobs.aggregate([
            {'$group': {'_id': '$salary', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}}
        ]):
            salary_stats[salary['_id']] = salary['count']
            
        # Nowe statystyki
        
        # Statystyki pracy za granicą
        foreign_jobs = self.jobs.count_documents({'foreign_job': True})
        
        # Średnia liczba godzin w miesiącu
        avg_hours = None
        hours_pipeline = [
            {'$match': {'monthly_hours': {'$exists': True}}},
            {'$group': {'_id': None, 'avg_hours': {'$avg': '$monthly_hours'}}}
        ]
        hours_result = list(self.jobs.aggregate(hours_pipeline))
        if hours_result:
            avg_hours = round(hours_result[0]['avg_hours'], 2)
            
        # Statystyki systemów pracy
        work_schedules = {}
        for ws in self.jobs.aggregate([
            {'$match': {'work_schedule': {'$exists': True}}},
            {'$group': {'_id': '$work_schedule', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}}
        ]):
            work_schedules[ws['_id']] = ws['count']
            
        # Analiza zakresu wynagrodzeń
        salary_ranges = {
            'min': {'$min': '$salary_range.min'},
            'max': {'$max': '$salary_range.max'},
            'avg_min': {'$avg': '$salary_range.min'},
            'avg_max': {'$avg': '$salary_range.max'}
        }
        salary_range_stats = self.jobs.aggregate([
            {'$match': {'salary_range': {'$exists': True}}},
            {'$group': {'_id': None, **salary_ranges}}
        ])
        salary_range_stats = list(salary_range_stats)
        if salary_range_stats:
            salary_range_stats = salary_range_stats[0]
            del salary_range_stats['_id']
            # Zaokrąglamy wartości
            for key in salary_range_stats:
                if salary_range_stats[key]:
                    salary_range_stats[key] = round(salary_range_stats[key], 2)
        else:
            salary_range_stats = None
            
        # Top 10 miast
        top_cities = {}
        for city in self.jobs.aggregate([
            {'$match': {'city': {'$exists': True}}},
            {'$group': {'_id': '$city', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}},
            {'$limit': 10}
        ]):
            top_cities[city['_id']] = city['count']
            
        return {
            'total_jobs': total_jobs,
            'unique_companies': unique_companies,
            'unique_locations': unique_locations,
            'top_sources': top_sources,
            'jobs_by_location': jobs_by_location,
            'most_active_companies': most_active_companies,
            'contract_types': contract_types,
            'work_modes': work_modes,
            'industries': industries,
            'position_levels': position_levels,
            'benefits_distribution': benefits_distribution,
            'salary_stats': salary_stats,
            # Nowe statystyki
            'foreign_jobs_count': foreign_jobs,
            'average_monthly_hours': avg_hours,
            'work_schedules': work_schedules,
            'salary_range_statistics': salary_range_stats,
            'top_cities': top_cities
        }
        
    def __del__(self):
        """Zamykamy połączenie z bazą przy usuwaniu obiektu"""
        try:
            self.client.close()
            print("Zamknięto połączenie z bazą danych")
        except:
            pass 