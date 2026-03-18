import os
import random
import string
import time
from datetime import datetime, timedelta
from decimal import Decimal

import psycopg2
from faker import Faker
from psycopg2.extras import execute_values

fake = Faker()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "business_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

INSERT_INTERVAL_SEC = int(os.getenv("INSERT_INTERVAL_SEC", "2"))


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def create_tables(conn):
    ddl = """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        first_name VARCHAR(100) NOT NULL,
        last_name VARCHAR(100) NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL,
        phone VARCHAR(50),
        city VARCHAR(100),
        loyalty_points INTEGER DEFAULT 0,
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS products (
        product_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        sku VARCHAR(50) UNIQUE NOT NULL,
        product_name VARCHAR(255) NOT NULL,
        category VARCHAR(100),
        price NUMERIC(10,2) NOT NULL,
        stock_quantity INTEGER NOT NULL,
        weight_kg NUMERIC(8,3),
        available BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS orders (
        order_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        customer_id UUID NOT NULL REFERENCES customers(customer_id),
        product_id UUID NOT NULL REFERENCES products(product_id),
        quantity INTEGER NOT NULL,
        total_amount NUMERIC(12,2) NOT NULL,
        order_status VARCHAR(50) NOT NULL,
        order_date TIMESTAMP NOT NULL,
        shipped_at TIMESTAMP,
        notes TEXT,
        paid BOOLEAN DEFAULT FALSE
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()

def seed_customers(conn, n=20):
    rows = []
    for _ in range(n):
        rows.append((
            fake.first_name(),
            fake.last_name(),
            fake.unique.email(),
            fake.phone_number()[:50],
            fake.city(),
            random.randint(0, 5000),
            random.choice([True, True, True, False]),
            fake.date_time_between(start_date="-1y", end_date="now")
        ))

    sql = """
    INSERT INTO customers (
        first_name, last_name, email, phone, city, loyalty_points, is_active, created_at
    ) VALUES %s
    ON CONFLICT (email) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()

def seed_products(conn, n=30):
    categories = ["electronics", "books", "home", "sports", "beauty"]
    rows = []
    for _ in range(n):
        sku = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        price = Decimal(str(round(random.uniform(10, 500), 2)))
        weight = Decimal(str(round(random.uniform(0.1, 10.0), 3)))
        rows.append((
            sku,
            fake.word().capitalize() + " " + fake.word().capitalize(),
            random.choice(categories),
            price,
            random.randint(1, 300),
            weight,
            random.choice([True, True, True, False]),
            fake.date_time_between(start_date="-1y", end_date="now")
        ))

    sql = """
    INSERT INTO products (
        sku, product_name, category, price, stock_quantity, weight_kg, available, created_at
    ) VALUES %s
    ON CONFLICT (sku) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()

def fetch_ids(conn, table, id_col):
    with conn.cursor() as cur:
        cur.execute(f"SELECT {id_col} FROM {table}")
        return [r[0] for r in cur.fetchall()]

def insert_random_order(conn):
    customer_ids = fetch_ids(conn, "customers", "customer_id")
    product_ids = fetch_ids(conn, "products", "product_id")

    if not customer_ids or not product_ids:
        return

    customer_id = random.choice(customer_ids)
    product_id = random.choice(product_ids)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT price, stock_quantity FROM products WHERE product_id = %s",
            (product_id,)
        )
        result = cur.fetchone()
        if not result:
            conn.rollback()
            return
        price, stock_quantity = result

        if stock_quantity <= 0:
            conn.commit()
            return

        quantity = random.randint(1, min(5, stock_quantity))
        total_amount = price * quantity
        order_date = fake.date_time_between(start_date="-30d", end_date="now")
        status = random.choice(["NEW", "PAID", "SHIPPED", "CANCELLED"])
        shipped_at = None
        if status == "SHIPPED":
            shipped_at = order_date + timedelta(days=random.randint(1, 5))

        notes = fake.sentence(nb_words=8)
        paid = status in ["PAID", "SHIPPED"]

        cur.execute(
            """
            INSERT INTO orders (
                customer_id, product_id, quantity, total_amount,
                order_status, order_date, shipped_at, notes, paid
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                customer_id, product_id, quantity, total_amount,
                status, order_date, shipped_at, notes, paid
            )
        )

        cur.execute(
            "UPDATE products SET stock_quantity = stock_quantity - %s WHERE product_id = %s",
            (quantity, product_id)
        )

    conn.commit()


def update_random_customer(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
        row = cur.fetchone()
        if not row:
            conn.commit()
            return
        customer_id = row[0]
        new_points = random.randint(0, 10000)
        cur.execute(
            "UPDATE customers SET loyalty_points = %s, is_active = %s WHERE customer_id = %s",
            (new_points, random.choice([True, False]), customer_id)
        )
    conn.commit()

def update_random_product(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT product_id, price FROM products ORDER BY random() LIMIT 1")
        row = cur.fetchone()
        if not row:
            conn.commit()
            return
        product_id, price = row
        factor = Decimal(str(round(random.uniform(0.95, 1.10), 2)))
        new_price = (price * factor).quantize(Decimal("0.01"))
        cur.execute(
            "UPDATE products SET price = %s, available = %s WHERE product_id = %s",
            (new_price, random.choice([True, False]), product_id)
        )
    conn.commit()

def main():

    while True:
        print("Connecting to PostgreSQL...")
        try:
            conn = get_conn()
            break
        except Exception as e:
            print(f"PostgreSQL not ready yet: {e}")
            time.sleep(3)

    create_tables(conn)
    seed_customers(conn, 25)
    seed_products(conn, 40)
    print("Seed data inserted.")

    actions = [insert_random_order, update_random_customer, update_random_product]

    while True:
        action = random.choice(actions)
        try:
            action(conn)
            print(f"[{datetime.now().isoformat()}] Executed: {action.__name__}")
        except Exception as e:
            conn.rollback()
            print(f"Error in {action.__name__}: {e}")
        time.sleep(INSERT_INTERVAL_SEC)


if __name__ == "__main__":
    main()











