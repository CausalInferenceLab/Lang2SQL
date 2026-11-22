"""Freeze independent questions and gold SQL results before calling a model."""

import hashlib
import json
import random
import sqlite3
from pathlib import Path

TERM = "인정매출"
DEFINITION = "결제상태가 PAID인 주문의 주문상품만 대상으로, 상품별 수량×단가에서 그 행의 할인액과 환불액을 각각 한 번씩 뺀 금액의 합계. 주문일 기준."
PROBES = {30: 3, 60: 7, 90: 11}
REGIONS = ["서울", "부산", "대구", "광주"]


def database(path, seed):
    rng = random.Random(seed)
    with sqlite3.connect(path) as db:
        db.executescript("""
            CREATE TABLE customers(customer_id INTEGER PRIMARY KEY, region TEXT);
            CREATE TABLE orders(order_id INTEGER PRIMARY KEY, customer_id INTEGER, order_date TEXT, payment_status TEXT);
            CREATE TABLE items(order_id INTEGER, product_group TEXT, qty INTEGER, unit_price INTEGER, discount_amount INTEGER, refund_amount INTEGER);
        """)
        db.executemany(
            "INSERT INTO customers VALUES (?,?)",
            [(i, REGIONS[i % 4]) for i in range(1, 97)],
        )
        for oid in range(1, 1201):
            month = (oid - 1) % 12 + 1
            db.execute(
                "INSERT INTO orders VALUES (?,?,?,?)",
                (
                    oid,
                    rng.randint(1, 96),
                    f"2025-{month:02}-{rng.randint(1, 28):02}",
                    rng.choice(["PAID", "PAID", "PENDING", "VOID"]),
                ),
            )
            for _ in range(rng.randint(1, 4)):
                db.execute(
                    "INSERT INTO items VALUES (?,?,?,?,?,?)",
                    (
                        oid,
                        rng.choice(["생활", "식품", "의류"]),
                        rng.randint(1, 5),
                        100 * rng.randint(2, 90),
                        rng.randrange(100),
                        rng.choice([0, 0, 5, 100, 200]),
                    ),
                )


def make_cases():
    rng = random.Random(267)
    choices = [
        (kind, month, region)
        for kind in range(8)
        for month in range(1, 13)
        for region in REGIONS
    ]
    rng.shuffle(choices)
    cases = []
    for turn in range(1, 101):
        if turn in PROBES:
            month = PROBES[turn]
            question = f"2025년 {month}월 전체 지역의 인정매출은 얼마인가요?"
            sql = f"SELECT SUM(qty*unit_price-discount_amount-refund_amount) FROM orders JOIN items USING(order_id) WHERE payment_status='PAID' AND order_date LIKE '2025-{month:02}-%'"
        else:
            kind, month, region = choices.pop()
            prompt, projection, extra, predicate = [
                ("주문 건수는?", "COUNT(*)", "", ""),
                (
                    "주문한 서로 다른 고객 수는?",
                    "COUNT(DISTINCT orders.customer_id)",
                    "",
                    "",
                ),
                (
                    "결제상태 PAID인 주문 건수는?",
                    "COUNT(*)",
                    "",
                    " AND payment_status='PAID'",
                ),
                (
                    "주문에 포함된 모든 상품 수량의 합은?",
                    "SUM(qty)",
                    " JOIN items USING(order_id)",
                    "",
                ),
                (
                    "주문상품의 할인액 합계는? 행별 할인액을 한 번씩 더하세요.",
                    "SUM(discount_amount)",
                    " JOIN items USING(order_id)",
                    "",
                ),
                (
                    "주문상품의 환불액 합계는? 행별 환불액을 한 번씩 더하세요.",
                    "SUM(refund_amount)",
                    " JOIN items USING(order_id)",
                    "",
                ),
                (
                    "식품을 포함하는 서로 다른 주문 수는?",
                    "COUNT(DISTINCT orders.order_id)",
                    " JOIN items USING(order_id)",
                    " AND product_group='식품'",
                ),
                (
                    "주문상품의 할인·환불 전 총액(수량×단가 합계)은?",
                    "SUM(qty*unit_price)",
                    " JOIN items USING(order_id)",
                    "",
                ),
            ][kind]
            question = f"2025년 {month}월 주문일 기준, 고객 지역이 {region}인 {prompt}"
            sql = f"SELECT {projection} FROM orders JOIN customers USING(customer_id){extra} WHERE region='{region}' AND order_date LIKE '2025-{month:02}-%'{predicate}"
        cases.append(
            {
                "turn": turn,
                "question": question,
                "gold_sql": sql,
                "probe": turn in PROBES,
            }
        )
    assert len({c["question"] for c in cases}) == 100
    return cases


def freeze(directory: Path):
    directory.mkdir(parents=True, exist_ok=False)
    cases = make_cases()
    for name, seed in [("primary", 267), ("shadow", 728)]:
        path = directory / (name + ".sqlite")
        database(path, seed)
        with sqlite3.connect(path) as db:
            for c in cases:
                c[name] = db.execute(c["gold_sql"]).fetchone()[0]
    manifest = {"definition": DEFINITION, "register_after": 10, "cases": cases}
    path = directory / "manifest.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    files = {
        f.name: hashlib.sha256(f.read_bytes()).hexdigest()
        for f in sorted(directory.iterdir())
    }
    (directory / "hashes.json").write_text(json.dumps(files, indent=2) + "\n")
