#!/usr/bin/env python3
"""Seed the source database and generate valid ecommerce changes."""

from __future__ import annotations

import argparse
import os
import random
import signal
import sys
import time
from decimal import Decimal

import psycopg
from faker import Faker
from psycopg import Connection
from psycopg.errors import CheckViolation, OperationalError

from config import ConfigError, PostgresConfig, load_source_app_config


VALID_TRANSITIONS = {
    "pending": ("paid",),
    "paid": ("shipping",),
    "shipping": ("completed", "cancelled"),
}
PRODUCT_CATEGORIES = ("books", "electronics", "home", "sports", "beauty")
OPERATION_WEIGHTS = ("insert", "update", "delete"), (0.60, 0.30, 0.10)


class NoAvailableData(RuntimeError):
    """Raised when an operation has no eligible source row."""


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value is None else int(value)


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return default if value is None else float(value)


def nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return parsed


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def nonnegative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return parsed


def probability(value: str) -> float:
    parsed = float(value)
    if not 0 <= parsed <= 1:
        raise argparse.ArgumentTypeError("must be between zero and one")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    seed = commands.add_parser("seed", help="insert deterministic customers and products")
    seed.add_argument("--customers", type=positive_int, default=25)
    seed.add_argument("--products", type=positive_int, default=20)
    seed.add_argument("--seed", type=int, default=env_int("GENERATOR_SEED", 42))

    run = commands.add_parser("run", help="generate source transactions continuously")
    run.add_argument("--iterations", type=nonnegative_int, default=0)
    run.add_argument(
        "--interval-seconds",
        type=nonnegative_float,
        default=env_float("GENERATOR_INTERVAL_SECONDS", 1.0),
    )
    run.add_argument("--seed", type=int, default=env_int("GENERATOR_SEED", 42))
    run.add_argument(
        "--invalid-rate",
        type=probability,
        default=probability(os.getenv("GENERATOR_INVALID_RATE", "0.02")),
    )

    smoke = commands.add_parser("smoke", help="exercise insert, update, delete and rejection")
    smoke.add_argument("--seed", type=int, default=env_int("GENERATOR_SEED", 42))
    return parser


def connect_with_retry(
    config: PostgresConfig,
    attempts: int = 15,
    delay_seconds: float = 2.0,
) -> Connection:
    last_error: OperationalError | None = None
    for attempt in range(1, attempts + 1):
        try:
            return psycopg.connect(
                host=config.host,
                port=config.port,
                dbname=config.database,
                user=config.user,
                password=config.password,
                autocommit=True,
            )
        except OperationalError as error:
            last_error = error
            if attempt == attempts:
                raise
            print(
                f"Waiting for PostgreSQL ({attempt}/{attempts - 1})...",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(delay_seconds)
    raise last_error or RuntimeError("could not connect to PostgreSQL")


def seed_database(
    conn: Connection,
    customer_count: int,
    product_count: int,
    seed: int,
) -> None:
    fake = Faker("en_US")
    fake.seed_instance(seed)
    rng = random.Random(seed)

    customers = [
        (customer_id, fake.name(), fake.city())
        for customer_id in range(1, customer_count + 1)
    ]
    products = [
        (
            product_id,
            f"{fake.word().title()} {fake.word().title()} {product_id:03d}",
            rng.choice(PRODUCT_CATEGORIES),
            Decimal(rng.randint(500, 50000)) / Decimal("100"),
            rng.randint(100, 250),
        )
        for product_id in range(1, product_count + 1)
    ]

    with conn.transaction():
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO customers (customer_id, name, city)
                VALUES (%s, %s, %s)
                ON CONFLICT (customer_id) DO NOTHING
                """,
                customers,
            )
            cur.executemany(
                """
                INSERT INTO products
                    (product_id, name, category, price, stock_quantity)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO NOTHING
                """,
                products,
            )
            cur.execute(
                """
                SELECT setval(
                    pg_get_serial_sequence('public.customers', 'customer_id'),
                    GREATEST(COALESCE((SELECT MAX(customer_id) FROM customers), 1), 1),
                    true
                )
                """
            )
            cur.execute(
                """
                SELECT setval(
                    pg_get_serial_sequence('public.products', 'product_id'),
                    GREATEST(COALESCE((SELECT MAX(product_id) FROM products), 1), 1),
                    true
                )
                """
            )

    print(
        f"Seed complete: customers={customer_count}, products={product_count}",
        flush=True,
    )


def require_seed_data(conn: Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM customers),
                (SELECT COUNT(*) FROM products)
            """
        )
        customer_count, product_count = cur.fetchone()
    if customer_count == 0 or product_count == 0:
        raise RuntimeError("source data is empty; run the seed command first")


def create_order(conn: Connection, rng: random.Random) -> int:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
            customer = cur.fetchone()
            if customer is None:
                raise NoAvailableData("no customer is available")

            cur.execute(
                """
                SELECT product_id, price, stock_quantity
                FROM products
                WHERE stock_quantity > 0
                ORDER BY random()
                LIMIT 3
                FOR UPDATE SKIP LOCKED
                """
            )
            available_products = cur.fetchall()
            if not available_products:
                raise NoAvailableData("no product with stock is available")

            items = []
            total_amount = Decimal("0.00")
            for product_id, unit_price, stock_quantity in available_products:
                quantity = rng.randint(1, min(3, stock_quantity))
                items.append((product_id, quantity, unit_price))
                total_amount += unit_price * quantity

            cur.execute(
                """
                INSERT INTO orders (customer_id, status, total_amount)
                VALUES (%s, 'pending', %s)
                RETURNING order_id
                """,
                (customer[0], total_amount),
            )
            order_id = cur.fetchone()[0]
            cur.executemany(
                """
                INSERT INTO order_items (order_id, product_id, quantity, unit_price)
                VALUES (%s, %s, %s, %s)
                """,
                [
                    (order_id, product_id, quantity, unit_price)
                    for product_id, quantity, unit_price in items
                ],
            )
            cur.executemany(
                """
                UPDATE products
                SET stock_quantity = stock_quantity - %s
                WHERE product_id = %s
                """,
                [(quantity, product_id) for product_id, quantity, _ in items],
            )
    return order_id


def transition_random_order(conn: Connection, rng: random.Random) -> int | None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT order_id, status::text
                FROM orders
                WHERE status IN ('pending', 'paid', 'shipping')
                ORDER BY random()
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            )
            order = cur.fetchone()
            if order is None:
                return None
            order_id, current_status = order
            next_status = rng.choice(VALID_TRANSITIONS[current_status])
            cur.execute(
                "UPDATE orders SET status = %s WHERE order_id = %s",
                (next_status, order_id),
            )
            if next_status == "cancelled":
                restock_order(cur, order_id)
    return order_id


def set_order_status(conn: Connection, order_id: int, next_status: str) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status::text
                FROM orders
                WHERE order_id = %s
                FOR UPDATE
                """,
                (order_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise NoAvailableData(f"order {order_id} does not exist")
            current_status = row[0]
            if next_status not in VALID_TRANSITIONS.get(current_status, ()):
                raise ValueError(f"invalid transition requested: {current_status} -> {next_status}")
            cur.execute(
                "UPDATE orders SET status = %s WHERE order_id = %s",
                (next_status, order_id),
            )
            if next_status == "cancelled":
                restock_order(cur, order_id)


def restock_order(cur: psycopg.Cursor, order_id: int) -> None:
    cur.execute(
        """
        UPDATE products AS products
        SET stock_quantity = products.stock_quantity + items.quantity
        FROM order_items AS items
        WHERE items.order_id = %s
          AND products.product_id = items.product_id
        """,
        (order_id,),
    )


def delete_random_order(conn: Connection) -> int | None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT order_id
                FROM orders
                WHERE status IN ('completed', 'cancelled')
                ORDER BY random()
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            )
            order = cur.fetchone()
            if order is None:
                return None
            cur.execute("DELETE FROM orders WHERE order_id = %s", (order[0],))
    return order[0]


def delete_order(conn: Connection, order_id: int) -> bool:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("DELETE FROM orders WHERE order_id = %s", (order_id,))
            return cur.rowcount == 1


def attempt_invalid_transition(conn: Connection, order_id: int | None = None) -> bool:
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                if order_id is None:
                    cur.execute(
                        """
                        SELECT order_id
                        FROM orders
                        WHERE status = 'pending'
                        ORDER BY random()
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                        """
                    )
                    row = cur.fetchone()
                    if row is None:
                        return False
                    order_id = row[0]
                else:
                    cur.execute(
                        "SELECT status::text FROM orders WHERE order_id = %s FOR UPDATE",
                        (order_id,),
                    )
                    row = cur.fetchone()
                    if row is None or row[0] != "pending":
                        return False
                cur.execute(
                    "UPDATE orders SET status = 'completed' WHERE order_id = %s",
                    (order_id,),
                )
    except CheckViolation:
        return True
    return False


def run_smoke(conn: Connection, seed: int) -> None:
    seed_database(conn, customer_count=10, product_count=10, seed=seed)
    rng = random.Random(seed)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT customer_id, updated_at FROM customers ORDER BY customer_id LIMIT 1"
        )
        customer_id, previous_updated_at = cur.fetchone()
        time.sleep(0.01)
        cur.execute(
            "UPDATE customers SET city = city WHERE customer_id = %s",
            (customer_id,),
        )
        cur.execute(
            "SELECT updated_at FROM customers WHERE customer_id = %s",
            (customer_id,),
        )
        current_updated_at = cur.fetchone()[0]
    if current_updated_at <= previous_updated_at:
        raise AssertionError("customer updated_at trigger did not advance")

    order_id = create_order(conn, rng)
    if not attempt_invalid_transition(conn, order_id):
        raise AssertionError("invalid transition was not rejected")

    for status in ("paid", "shipping", "completed"):
        set_order_status(conn, order_id, status)

    with conn.cursor() as cur:
        cur.execute("SELECT status::text FROM orders WHERE order_id = %s", (order_id,))
        if cur.fetchone()[0] != "completed":
            raise AssertionError("order did not reach completed status")

    if not delete_order(conn, order_id):
        raise AssertionError("created order could not be deleted")

    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM orders WHERE order_id = %s", (order_id,))
        if cur.fetchone() is not None:
            raise AssertionError("deleted order is still present")
        cur.execute("SELECT 1 FROM order_items WHERE order_id = %s", (order_id,))
        if cur.fetchone() is not None:
            raise AssertionError("order items were not removed by cascade delete")

    print(
        f"SMOKE OK: customer updated_at trigger, insert order={order_id}, "
        "valid transitions, invalid transition rejected, delete",
        flush=True,
    )


def run_generator(
    conn: Connection,
    iterations: int,
    interval_seconds: float,
    seed: int,
    invalid_rate: float,
) -> None:
    require_seed_data(conn)
    rng = random.Random(seed)
    operation_names, operation_weights = OPERATION_WEIGHTS
    iteration = 0

    while iterations == 0 or iteration < iterations:
        if rng.random() < invalid_rate and attempt_invalid_transition(conn):
            print("REJECTED controlled invalid transition", flush=True)

        operation = rng.choices(operation_names, weights=operation_weights, k=1)[0]
        try:
            if operation == "insert":
                order_id = create_order(conn, rng)
                print(f"INSERT order={order_id}", flush=True)
            elif operation == "update":
                order_id = transition_random_order(conn, rng)
                if order_id is None:
                    order_id = create_order(conn, rng)
                    print(f"INSERT order={order_id} (update fallback)", flush=True)
                else:
                    print(f"UPDATE order={order_id}", flush=True)
            else:
                order_id = delete_random_order(conn)
                if order_id is None:
                    order_id = create_order(conn, rng)
                    print(f"INSERT order={order_id} (delete fallback)", flush=True)
                else:
                    print(f"DELETE order={order_id}", flush=True)
        except NoAvailableData as error:
            print(f"SKIP: {error}", file=sys.stderr, flush=True)

        iteration += 1
        if interval_seconds:
            time.sleep(interval_seconds)


def install_signal_handlers() -> None:
    def stop_generator(_signum: int, _frame: object) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, stop_generator)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, stop_generator)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        config = load_source_app_config()
    except ConfigError as error:
        parser.error(str(error))

    try:
        install_signal_handlers()
        with connect_with_retry(config) as conn:
            if args.command == "seed":
                seed_database(conn, args.customers, args.products, args.seed)
            elif args.command == "smoke":
                run_smoke(conn, args.seed)
            else:
                run_generator(
                    conn,
                    args.iterations,
                    args.interval_seconds,
                    args.seed,
                    args.invalid_rate,
                )
    except KeyboardInterrupt:
        print("Generator stopped cleanly", flush=True)
    except (OperationalError, psycopg.Error, RuntimeError, ValueError, AssertionError) as error:
        print(f"ERROR: {error}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
