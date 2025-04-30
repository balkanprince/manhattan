import sqlite3
from sqlite3 import Error

DB_FILE = "my_tasks.db"

def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        print(f"Connected to {db_file}")
        return conn
    except Error as e:
        print(e)
    return None

def execute_sql(conn, sql):
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)

def create_tables(conn):
    sql_projects = """
    CREATE TABLE IF NOT EXISTS projects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nazwa TEXT NOT NULL,
        start_date TEXT,
        end_date TEXT
    );
    """

    sql_tasks = """
    CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        description TEXT,
        status TEXT,
        start_date TEXT,
        end_date TEXT,
        FOREIGN KEY (project_id) REFERENCES projects (id)
    );
    """
    execute_sql(conn, sql_projects)
    execute_sql(conn, sql_tasks)

def add_project(conn, project):
    sql = '''INSERT INTO projects(nazwa, start_date, end_date) VALUES (?, ?, ?)'''
    cur = conn.cursor()
    cur.execute(sql, project)
    conn.commit()
    return cur.lastrowid

def add_task(conn, task):
    sql = '''INSERT INTO tasks(project_id, name, description, status, start_date, end_date)
             VALUES (?, ?, ?, ?, ?, ?)'''
    cur = conn.cursor()
    cur.execute(sql, task)
    conn.commit()
    return cur.lastrowid

def select_all(conn, table):
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    return cur.fetchall()

def select_where(conn, table, **kwargs):
    cur = conn.cursor()
    query = " AND ".join([f"{k}=?" for k in kwargs])
    values = tuple(kwargs.values())
    cur.execute(f"SELECT * FROM {table} WHERE {query}", values)
    return cur.fetchall()

def update(conn, table, id, **kwargs):
    fields = ", ".join([f"{k}=?" for k in kwargs])
    values = tuple(kwargs.values()) + (id,)
    sql = f"UPDATE {table} SET {fields} WHERE id=?"
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        print("Update OK")
    except Error as e:
        print(e)

def delete(conn, table, id):
    sql = f"DELETE FROM {table} WHERE id=?"
    cur = conn.cursor()
    cur.execute(sql, (id,))
    conn.commit()
    print("Delete OK")

# --- Główne działanie
if __name__ == "__main__":
    conn = create_connection(DB_FILE)
    if conn is not None:
        create_tables(conn)

        # Dodajemy projekt
        project_id = add_project(conn, ("Nauka SQLite", "2025-04-30", "2025-05-02"))
        print(f"Dodano projekt o ID: {project_id}")

        # Dodajemy zadania
        add_task(conn, (project_id, "Przeczytać dokumentację", "Oficjalna dokumentacja sqlite3", "new", "2025-04-30 10:00", "2025-04-30 12:00"))
        add_task(conn, (project_id, "Napisać kod", "Stworzyć projekt CRUD", "started", "2025-04-30 13:00", "2025-04-30 16:00"))

        # Odczyt danych
        print("\nWszystkie projekty:")
        for row in select_all(conn, "projects"):
            print(row)

        print("\nZadania dla projektu:")
        for row in select_where(conn, "tasks", project_id=project_id):
            print(row)

        # Aktualizacja
        update(conn, "tasks", 1, status="ended")

        # Usunięcie zadania
        delete(conn, "tasks", 2)

        conn.close()
