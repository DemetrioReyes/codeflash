import sqlite3
from pathlib import Path

# define DB_PATH as a global constant
DB_PATH = Path(__file__).parent / "test_discovery.db"

def get_db_connection():
    # SQLite database setup
    if not DB_PATH.exists():
        print(f"Database file {DB_PATH} does not exist. Creating a new one.")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS test_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT NOT NULL UNIQUE
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS tests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id INTEGER NOT NULL,
                test_class TEXT,
                test_function TEXT NOT NULL,
                test_type INTEGER NOT NULL,
                FOREIGN KEY (file_id) REFERENCES test_files (id),
                UNIQUE(file_id, test_function)
            )
            """
        )

        conn.commit()
        conn.close()
        print(f"Database file {DB_PATH} created successfully.")


# impedir duplicados
def add_test_file(file_to_test_map):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    total_records = 0
    total_duplicates = 0
    total_added = 0

    for test_file, tests in file_to_test_map.items():
        cursor.execute(
            "INSERT OR IGNORE INTO test_files (file_path) VALUES (?)",
            (str(test_file),)
        )
        if cursor.rowcount == 0:
            total_duplicates += 1
        else:
            total_added += 1

        cursor.execute(
            "SELECT id FROM test_files WHERE file_path = ?",
            (str(test_file),)
        )
        file_id = cursor.fetchone()[0]

        for test in tests:
            cursor.execute(
                """
                INSERT OR IGNORE INTO tests (file_id, test_class, test_function, test_type)
                VALUES (?, ?, ?, ?)
                """,
                (
                    file_id,
                    test.test_class,
                    test.test_function,
                    test.test_type.value,
                ),
            )
            if cursor.rowcount == 0:
                total_duplicates += 1
            else:
                total_added += 1

    total_records = total_added + total_duplicates

    conn.commit()
    conn.close()

    print(f"Total records processed: {total_records}")
    print(f"Total duplicates: {total_duplicates}")
    print(f"Total added: {total_added}")
