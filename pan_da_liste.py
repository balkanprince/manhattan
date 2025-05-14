import pandas as pd

# Wczytanie danych z tabeli HTML (przykład linku – należy podmienić na właściwy, jeśli jest inny)
url = 'https://www.officialcharts.com/chart-news/the-best-selling-albums-of-all-time-on-the-official-uk-chart__15551/'
tables = pd.read_html(url, header=0)

# Sprawdzenie, która tabela zawiera właściwe dane – np. pierwsza
df = tables[0]

# Podgląd nagłówków
print(df.columns)

# Zmiana nagłówków kolumn na polskie odpowiedniki (zakładam, że oryginalne to np. Title, Artist, Year, Peak position)
df = df.drop(columns=['POS'])
df.columns = ['TYTUŁ', 'ARTYSTA', 'ROK', 'MAX POZ'] + list(df.columns[4:])  # Jeśli są dodatkowe kolumny, zostają bez zmian

# Sprawdzenie liczby unikalnych pojedynczych artystów (czyli takich, którzy nie są zespołami)
# Zakładamy, że pojedynczy artysta to taki, który nie ma w nazwie „&”, „and”, „feat”, „+”, „, ” (to bardzo uproszczona heurystyka)
individual_artists = df['ARTYSTA'].dropna().unique()
individual_artists = [a for a in individual_artists if all(x not in a.lower() for x in ['&',' and ','feat','.',' + ', ','])]
print("Liczba pojedynczych artystów:", len(individual_artists))

# Zespoły, które pojawiają się najczęściej
most_common = df['ARTYSTA'].value_counts()
print("Najczęściej występujące zespoły/artystów:\n", most_common.head())

# Zmiana nagłówków – wielka litera na początku
df.columns = [col.capitalize() for col in df.columns]

# Usunięcie kolumny „Max poz” (teraz może mieć nazwę „Max poz” lub „Max poz.” – w zależności od tego, jak to wyglądało)
if 'Max poz' in df.columns:
    df = df.drop(columns=['Max poz'])

# Rok, w którym wydano najwięcej albumów
most_common_year = df['Rok'].value_counts().idxmax()
print("Najwięcej albumów wydano w roku:", most_common_year)

# Albumy z lat 1960–1990 (włącznie)
albums_60s_90s = df[(df['Rok'] >= 1960) & (df['Rok'] <= 1990)]
print("Liczba albumów z lat 1960–1990:", len(albums_60s_90s))

# Najnowszy album na liście
youngest_album_year = df['Rok'].max()
print("Najmłodszy album został wydany w roku:", youngest_album_year)

# Najwcześniejszy album każdego artysty
first_albums = df.sort_values(by='Rok').drop_duplicates(subset='Artysta', keep='first')

# Eksport do pliku CSV
first_albums.to_csv('najwczesniejsze_albumy.csv', index=False)
