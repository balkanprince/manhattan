import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    """połączenie z bazą danych SQLite."""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Połączono z {db_file}, wersja SQLite: {sqlite3.version}")
        return conn
    except Error as e:
        print(f"Błąd: {e}")
    return conn

def execute_sql(conn, sql):
    """zapytanie SQL."""
    try:
        c = conn.cursor()
        c.execute(sql)
        conn.commit()
        print("Zapytanie SQL wykonane pomyślnie.")
    except Error as e:
        print(f"Błąd: {e}")

def create_tables(conn):
    sql_create_projects_table = """
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nazwa TEXT NOT NULL,
            start_date TEXT,
            end_date TEXT
        );
    """
    sql_create_tasks_table = """
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            nazwa TEXT NOT NULL,
            opis TEXT,
            status TEXT,
            start_date TEXT,
            end_date TEXT,
            FOREIGN KEY (project_id) REFERENCES projects (id)
        );
    """
    execute_sql(conn, sql_create_projects_table)
    execute_sql(conn, sql_create_tasks_table)

def add_project(conn, project):
    sql = '''INSERT INTO projects(nazwa, start_date, end_date) VALUES(?,?,?)'''
    try:
        cur = conn.cursor()
        cur.execute(sql, project)
        conn.commit()
        return cur.lastrowid
    except Error as e:
        print(f"Błąd: {e}")
        return None

def add_task(conn, task):
    sql = '''INSERT INTO tasks(project_id, nazwa, opis, status, start_date, end_date) 
             VALUES(?,?,?,?,?,?)'''
    try:
        cur = conn.cursor()
        cur.execute(sql, task)
        conn.commit()
        return cur.lastrowid
    except Error as e:
        print(f"Błąd: {e}")
        return None

def select_all(conn, table):
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table}")
        return cur.fetchall()
    except Error as e:
        print(f"Błąd: {e}")
        return []

def select_where(conn, table, **query):
    try:
        cur = conn.cursor()
        qs = [f"{k}=?" for k in query]
        q = " AND ".join(qs)
        values = tuple(query.values())
        cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
        return cur.fetchall()
    except Error as e:
        print(f"Błąd: {e}")
        return []

def update(conn, table, id, **kwargs):
    try:
        parameters = [f"{k}=?" for k in kwargs]
        parameters = ", ".join(parameters)
        values = tuple(kwargs.values()) + (id,)
        sql = f'''UPDATE {table} SET {parameters} WHERE id=?'''
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        print("Aktualizacja zakończona pomyślnie.")
    except Error as e:
        print(f"Błąd: {e}")

def delete_where(conn, table, **kwargs):
    try:
        qs = [f"{k}=?" for k in kwargs]
        q = " AND ".join(qs)
        values = tuple(kwargs.values())
        sql = f"DELETE FROM {table} WHERE {q}"
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        print("Usunięto wiersze pomyślnie.")
    except Error as e:
        print(f"Błąd: {e}")

def delete_all(conn, table):
    try:
        sql = f"DELETE FROM {table}"
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        print(f"Usunięto wszystkie wiersze z tabeli {table}.")
    except Error as e:
        print(f"Błąd: {e}")

if __name__ == "__main__":
    # Połączenie z bazą
    conn = create_connection("database.db")
    if conn is not None:
        # Tworzenie tabel
        create_tables(conn)

        # Dodanie projektu
        project = ("Powtórka z angielskiego", "2025-05-01 00:00:00", "2025-05-03 00:00:00")
        project_id = add_project(conn, project)
        print(f"Dodano projekt o ID: {project_id}")

        # Dodanie zadania
        task = (project_id, "Czasowniki nieregularne", "Nauka 50 czasowników", "started", 
                "2025-05-01 12:00:00", "2025-05-01 15:00:00")
        task_id = add_task(conn, task)
        print(f"Dodano zadanie o ID: {task_id}")

        # Pobieranie danych
        print("Wszystkie projekty:", select_all(conn, "projects"))
        print("Zadania dla projektu ID=1:", select_where(conn, "tasks", project_id=1))
        print("Zadania ze statusem 'started':", select_where(conn, "tasks", status="started"))

        # Aktualizacja zadania
        update(conn, "tasks", task_id, status="ended", end_date="2025-05-01 16:00:00")

        # Usunięcie zadania
        delete_where(conn, "tasks", id=task_id)

        # Zamknięcie połączenia
        conn.close()
