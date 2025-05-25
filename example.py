# Przydatne linki
# https://myhttpheader.com 

import requests # do pobierania stron www
from bs4 import BeautifulSoup # do przetwarzania HTML
import pandas as pd # do analizy danych
import numpy as np # do analizy danych
import time # do wstrzymabua kodu na określony czas
from datetime import datetime # do pracy z datami
from pymongo import MongoClient # do pracy z bazą danych MongoDB

class WebScraping:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1.1 Safari/605.1.15",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",    
        "Accept-Language": "pl-PL,pl;q=0.9",
    }

    def __init__(self):
        print("Utworzono obiekt WebScraping")
        #obiekt klienta 
        client = MongoClient('mongodb://localhost:27017/')
        #baza danych, jeśli nie istnieje to zostanie utworzona
        self.db = client['BigData_studia']
        self.lista_cen = [] # inicjalizacja globalnej pustej listy cen
        self.lista_szczegolow = [] # inicjalizacja globalnej pustej listy szczegółów

    def scrape(self, www):
        try:
            self.html = requests.get(www, headers=self.headers).text
            self.www = www # to jest po to aby odwołać się z każdej metody do www
            print(f"Pobrano HTML z: {www}")
        except requests.RequestException as e:
            print(f"Błąd pobierania strony {www}: {e}")

    def zapisz(self):
        #wstawiamy dokument do kolekcji
        rezultat =self.db['mieszkania'].insert_one({"url": self.www, "content": self.html, "date": datetime.now()})
        print(f"Zapisano w bazie danych: {rezultat}")

    def ekstrakcja(self):
        # pobranie wszystkich dokumentów z kolekcji
        wynik =self.db['mieszkania'].find()
        for w in wynik:
            print("przetwarzam strone: ", w['url'])
            html = w['content']
            soup = BeautifulSoup(html, 'html.parser')
            ceny = soup.find_all('span', direction='horizontal') # <span direction="horizontal" class="css-2bt9f1 evk7nst0">359&nbsp;000&nbsp;zł</span> ### lepiej uywać znaczników niz klas ponieważ klasa może się zmienić by uniknąc botów            
            # for cena in ceny:
            #     print(cena.text.strip())
            szczegoly=soup.find_all('dl')
            # for pokoj in pokoje:
            #     print(pokoj.text.strip())
            for cena, szczegol in zip(ceny, szczegoly): # zip łączy dwa obiekty w jeden, iteruje tyle razy ile jest elementów w krótszyej liście
                #print(cena.text.strip(), szczegol.text.strip ())
                self.lista_cen.append(cena.text.strip())
                self.lista_szczegolow.append(szczegol.text.strip())

    def utworzDataFrame(self):
        self.df=pd.DataFrame({"cena": self.lista_cen, "szczegol": self.lista_szczegolow})
            
        #znajdź wszystkie linki
        # for link in soup.find_all('a'):
        #     print(link.get('href'))

    def oczyscCene(self,cena):
        if "pytaj" in cena: #jeśli w cenie jest słowo "pytaj" to zwróć NaN
            return np.nan
        # zamień "zł" na pusty ciąg znaków, zamień przecinek na kropkę, usuń spacje
        return "".join(cena.replace("zł", "").replace(",", ".").split())
    
    def oczyscPokoje(self,szczegol):
        # phase 1 - koi
        # print('phase 1',szczegol.split("koi"))
        # # phase 2 - pok
        # print('phase 2',szczegol.split("koi")[1].split("po"))
        # # phase

        return szczegol.split("koi")[1].split("po")[0].strip()
    
    def oczyscPowierzchnie(self,szczegol):
        return szczegol.split("owierzchnia")[1].split("m")[0].strip()
    
    def zapiszDoExcel(self, nazwa_pliku):
        self.df.to_excel(nazwa_pliku, index=False)

    def zapiszDoCSV(self, nazwa_pliku):
        self.df[['nowa_cena', 'pokoj', 'powierzchnia']].to_csv(nazwa_pliku, index=False, sep=';', decimal=',')

    def wyswietlStatystykeOpisowa(self):
        print(self.df.describe())
        
