import pandas as pd
import matplotlib.pyplot as plt

# 1. Wczytanie danych o śmiertelnych interwencjach
url_incidents = r"C:\Users\mpiesio\Desktop\KODILLA\manhattan\fatal-police-shootings-data.csv"
df = pd.read_csv(url_incidents)

# 2. Liczba ofiar według rasy i oznak choroby psychicznej
grouped = df.groupby(['race', 'signs_of_mental_illness']).size().unstack(fill_value=0)
grouped['% z chorobą psychiczną'] = grouped[True] / (grouped[True] + grouped[False]) * 100
print("\nOfiary wg rasy i chorób psychicznych:\n", grouped)

# 3. Rasa z najwyższym odsetkiem chorób psychicznych
highest_rate = grouped['% z chorobą psychiczną'].idxmax()
print("\nRasa z najwyższym odsetkiem chorób psychicznych:", highest_rate)

# 4. Dodanie dnia tygodnia i zliczenie interwencji
df['date'] = pd.to_datetime(df['date'])
df['weekday'] = df['date'].dt.day_name()
weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
weekday_counts = df['weekday'].value_counts().reindex(weekday_order)

# 5. Wykres kolumnowy
plt.figure(figsize=(10, 6))
weekday_counts.plot(kind='bar', color='skyblue')
plt.title('Liczba interwencji policji wg dnia tygodnia')
plt.ylabel('Liczba interwencji')
plt.xlabel('Dzień tygodnia')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# 6. Wczytanie danych o populacji i skrótach stanów
url_population = 'https://simple.wikipedia.org/wiki/List_of_U.S._states_by_population'
df_pop = pd.read_html(url_population, header=0)[0]

url_abbr = 'https://en.wikipedia.org/wiki/List_of_U.S._state_and_territory_abbreviations'
df_abbr = pd.read_html(url_abbr, header=0)[0]

# 7. Przygotowanie danych: wybór odpowiednich kolumn
# Populacja
population = df_pop[['State', 'Population estimate, July 1, 2019[2]']].copy()
population.columns = ['State', 'Population']
population['Population'] = population['Population'].str.replace(',', '').astype(int)

# Skróty
abbr = df_abbr[['State Name', 'US']].copy()
abbr.columns = ['State', 'Abbreviation']

# 8. Połączenie danych o populacji i skrótach
pop_combined = population.merge(abbr, on='State', how='inner')

# 9. Zliczenie incydentów w poszczególnych stanach
incidents_by_state = df['state'].value_counts().reset_index()
incidents_by_state.columns = ['Abbreviation', 'Incidents']

# 10. Połączenie wszystkich danych i obliczenie incydentów na 1000 mieszkańców
df_final = pop_combined.merge(incidents_by_state, on='Abbreviation', how='left')
df_final['Incidents'].fillna(0, inplace=True)
df_final['Incidents'] = df_final['Incidents'].astype(int)
df_final['Per_1000'] = df_final['Incidents'] / df_final['Population'] * 1000

# 11. Wynik posortowany malejąco
df_final_sorted = df_final.sort_values(by='Per_1000', ascending=False)
print("\nIncydenty na 1000 mieszkańców:\n", df_final_sorted[['State', 'Abbreviation', 'Per_1000']])
