import psycopg2
from config import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER
import json
import sys

# Подключение к базе данных
DB_CONFIG = {
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASS,
    "host": DB_HOST,
    "port": DB_PORT,
}


def create_tables():
    """Создает таблицу offices в базе данных"""
    query = """
    CREATE TABLE IF NOT EXISTS offices (
        id SERIAL PRIMARY KEY,
        parent_id INTEGER,
        name TEXT NOT NULL,
        type INTEGER NOT NULL
    );
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()


def import_json_to_db(json_file):
    """Импортирует данные из JSON-файла в базу данных"""
    with open(json_file, "r", encoding="utf-8") as file:
        data = json.load(file)

    query = """
    INSERT INTO offices (id, parent_id, name, type) 
    VALUES (%s, %s, %s, %s);
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            for entry in data:
                cursor.execute(
                    query,
                    (entry["id"], entry["ParentId"], entry["Name"], entry["Type"]),
                )
        conn.commit()


def get_office_employees(employee_id):
    """Получает список всех сотрудников из того же офиса, что и указанный сотрудник"""
    query = """
    WITH RECURSIVE hierarchy AS (
        SELECT id, parent_id, name, type FROM offices WHERE id = %s
        UNION ALL
        SELECT o.id, o.parent_id, o.name, o.type FROM offices o
        JOIN hierarchy h ON o.id = h.parent_id
    )
    SELECT id FROM hierarchy WHERE type = 1;
    """

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (employee_id,))
            office_id = cursor.fetchone()
            if not office_id:
                print("Сотрудник не найден")
                return
            office_id = office_id[0]

    query = """
    WITH RECURSIVE hierarchy AS (
        SELECT id, parent_id, name, type FROM offices WHERE parent_id = %s
        UNION ALL
        SELECT o.id, o.parent_id, o.name, o.type FROM offices o
        JOIN hierarchy h ON o.parent_id = h.id
    )
    SELECT name FROM hierarchy WHERE type = 3;
    """

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (office_id,))
            employees = cursor.fetchall()

    employee_names = [emp[0] for emp in employees]
    print("Сотрудники в офисе:", ", ".join(employee_names))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python main.py <employee_id>")
        sys.exit(1)

    employee_id = int(sys.argv[1])
    create_tables()
    import_json_to_db("mock_data.json")
    get_office_employees(employee_id)
