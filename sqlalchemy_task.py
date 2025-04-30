import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.exc import SQLAlchemyError
import os

# Ścieżki do plików CSV
STATIONS_CSV = r"C:\Users\mpiesio\Desktop\KODILLA\manhattan\clean_stations.csv"
MEASURES_CSV = r"C:\Users\mpiesio\Desktop\KODILLA\manhattan\clean_measure.csv"

# 1. Połączenie z bazą danych SQLite
def create_db_engine(db_file='stations.db', echo=False):
    """Tworzy silnik SQLAlchemy dla bazy SQLite."""
    try:
        engine = create_engine(f'sqlite:///{db_file}', echo=echo)
        print(f"Połączono z bazą danych: {db_file}")
        return engine
    except SQLAlchemyError as e:
        print(f"Błąd połączenia z bazą: {e}")
        return None

# 2. Definicja tabel
def define_tables(engine):
    """Definiuje tabele stations i measures."""
    meta = MetaData()

    stations = Table(
        'stations', meta,
        Column('station_id', Integer, primary_key=True),
        Column('name', String, nullable=False),
        Column('latitude', Float),
        Column('longitude', Float)
    )

    measures = Table(
        'measures', meta,
        Column('measure_id', Integer, primary_key=True),
        Column('station_id', Integer, ForeignKey('stations.station_id'), nullable=False),
        Column('date', DateTime, nullable=False),
        Column('value', Float)
    )

    # Tworzenie tabel w bazie danych
    try:
        meta.create_all(engine)
        print("Tabele utworzone pomyślnie.")
    except SQLAlchemyError as e:
        print(f"Błąd podczas tworzenia tabel: {e}")

    return stations, measures

# 3. Wczytywanie danych z CSV
def load_csv_to_table(engine, table, csv_file):
    """Wczytuje dane z pliku CSV do tabeli."""
    if not os.path.exists(csv_file):
        print(f"Błąd: Plik {csv_file} nie istnieje!")
        return
    try:
        # Wczytaj CSV za pomocą pandas
        df = pd.read_csv(csv_file)
        print(f"Wczytano {len(df)} wierszy z pliku {csv_file}")

        # Wstaw dane do tabeli
        with engine.connect() as conn:
            df.to_sql(table.name, conn, if_exists='append', index=False)
            conn.execute("COMMIT")
        print(f"Dane wstawione do tabeli {table.name}")
    except Exception as e:
        print(f"Błąd podczas wczytywania danych z {csv_file}: {e}")

# 4. Przykładowe zapytanie
def execute_query(engine, query):
    """Wykonuje zapytanie SQL i zwraca wyniki."""
    try:
        with engine.connect() as conn:
            result = conn.execute(query).fetchall()
            print("Wynik zapytania:", result)
            return result
    except SQLAlchemyError as e:
        print(f"Błąd podczas wykonywania zapytania: {e}")
        return []

# Główny blok programu
if __name__ == "__main__":
    # Utwórz silnik bazy danych
    engine = create_db_engine('stations.db', echo=True)  # echo=True dla debugowania SQL
    if engine is None:
        exit(1)

    # Zdefiniuj tabele
    stations_table, measures_table = define_tables(engine)

    # Wczytaj dane z CSV
    load_csv_to_table(engine, stations_table, STATIONS_CSV)
    load_csv_to_table(engine, measures_table, MEASURES_CSV)

    # Wykonaj przykładowe zapytanie
    query = "SELECT * FROM stations LIMIT 5"
    execute_query(engine, query)

    # Sprawdź dostępne tabele
    print("Dostępne tabele:", engine.table_names())