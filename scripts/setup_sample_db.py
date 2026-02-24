"""
샘플 데이터베이스 세팅 스크립트
================================
quickstart.md 튜토리얼용 샘플 데이터를 SQLite 또는 PostgreSQL에 생성합니다.

사용법
------
# SQLite (기본, 별도 서버 불필요)
python scripts/setup_sample_db.py

# PostgreSQL (Docker 컨테이너가 먼저 실행 중이어야 함)
python scripts/setup_sample_db.py --db postgres
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import random

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    create_engine,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Session

# ── 모델 정의 ────────────────────────────────────────────────────────────────


class Base(DeclarativeBase):
    pass


class Customer(Base):
    __tablename__ = "customers"
    customer_id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    email = Column(String(200), nullable=False, unique=True)
    joined_at = Column(DateTime, nullable=False)
    grade = Column(String(20), nullable=False)  # bronze / silver / gold


class Product(Base):
    __tablename__ = "products"
    product_id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    category = Column(String(100), nullable=False)  # electronics / clothing / food
    price = Column(Numeric(10, 2), nullable=False)
    stock = Column(Integer, nullable=False)


class Order(Base):
    __tablename__ = "orders"
    order_id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey("customers.customer_id"), nullable=False)
    order_date = Column(DateTime, nullable=False)
    amount = Column(Numeric(10, 2), nullable=False)
    status = Column(
        String(20), nullable=False
    )  # pending / confirmed / shipped / cancelled


class OrderItem(Base):
    __tablename__ = "order_items"
    item_id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey("orders.order_id"), nullable=False)
    product_id = Column(Integer, ForeignKey("products.product_id"), nullable=False)
    quantity = Column(Integer, nullable=False)
    unit_price = Column(Numeric(10, 2), nullable=False)


# ── 샘플 데이터 ──────────────────────────────────────────────────────────────

CUSTOMERS = [
    (1, "김철수", "chulsoo.kim@example.com", "2022-03-15", "gold"),
    (2, "박영희", "younghee.park@example.com", "2022-07-22", "gold"),
    (3, "이민준", "minjun.lee@example.com", "2023-01-10", "gold"),
    (4, "최수연", "sooyeon.choi@example.com", "2023-04-05", "silver"),
    (5, "정우진", "woojin.jung@example.com", "2023-06-18", "silver"),
    (6, "강지훈", "jihoon.kang@example.com", "2023-08-30", "silver"),
    (7, "윤서현", "seohyun.yoon@example.com", "2023-10-12", "bronze"),
    (8, "임도현", "dohyun.lim@example.com", "2024-01-25", "bronze"),
    (9, "한소희", "sohee.han@example.com", "2024-03-08", "bronze"),
    (10, "오준혁", "junhyuk.oh@example.com", "2024-05-17", "bronze"),
]

PRODUCTS = [
    (1, "무선 마우스", "electronics", 29900, 3),
    (2, "기계식 키보드", "electronics", 89000, 15),
    (3, "27인치 모니터", "electronics", 320000, 8),
    (4, "USB-C 허브", "electronics", 45000, 7),
    (5, "노이즈캔슬링 이어폰", "electronics", 159000, 22),
    (6, "면 반팔 티셔츠", "clothing", 19900, 50),
    (7, "청바지", "clothing", 59900, 30),
    (8, "운동화", "clothing", 89000, 9),
    (9, "후드 집업", "clothing", 69900, 4),
    (10, "유기농 아몬드", "food", 18900, 100),
    (11, "그래놀라", "food", 12500, 60),
    (12, "프로틴 바 (12개)", "food", 24900, 45),
]


def _make_orders_and_items():
    """지난 3개월간 주문 데이터를 생성합니다."""
    today = datetime(2026, 2, 24)
    orders, items = [], []
    item_id = 1
    order_id = 1

    random.seed(42)

    for cid, _, _, _, grade in CUSTOMERS:
        # 등급별 주문 빈도
        n_orders = {"gold": 8, "silver": 4, "bronze": 2}[grade]

        for _ in range(n_orders):
            days_ago = random.randint(1, 90)
            order_date = today - timedelta(days=days_ago)
            status = random.choice(
                ["confirmed", "confirmed", "shipped", "pending", "cancelled"]
            )

            # 주문에 1~3개 상품
            n_items = random.randint(1, 3)
            selected = random.sample(PRODUCTS, n_items)
            total = 0

            for pid, _, _, price, _ in selected:
                qty = random.randint(1, 3)
                unit_price = price
                total += unit_price * qty
                items.append((item_id, order_id, pid, qty, unit_price))
                item_id += 1

            orders.append((order_id, cid, order_date, total, status))
            order_id += 1

    return orders, items


# ── 세팅 함수 ────────────────────────────────────────────────────────────────


def setup(db_url: str) -> None:
    print(f"연결 중: {db_url}")
    engine = create_engine(db_url, echo=False)

    # 기존 테이블 삭제 후 재생성
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    print("테이블 생성 완료: customers, products, orders, order_items")

    orders, items = _make_orders_and_items()

    with Session(engine) as session:
        # customers
        session.add_all(
            [
                Customer(
                    customer_id=cid,
                    name=name,
                    email=email,
                    joined_at=datetime.fromisoformat(joined_at),
                    grade=grade,
                )
                for cid, name, email, joined_at, grade in CUSTOMERS
            ]
        )

        # products
        session.add_all(
            [
                Product(
                    product_id=pid, name=name, category=cat, price=price, stock=stock
                )
                for pid, name, cat, price, stock in PRODUCTS
            ]
        )

        # orders
        session.add_all(
            [
                Order(
                    order_id=oid,
                    customer_id=cid,
                    order_date=odate,
                    amount=amount,
                    status=status,
                )
                for oid, cid, odate, amount, status in orders
            ]
        )

        # order_items
        session.add_all(
            [
                OrderItem(
                    item_id=iid,
                    order_id=oid,
                    product_id=pid,
                    quantity=qty,
                    unit_price=up,
                )
                for iid, oid, pid, qty, up in items
            ]
        )

        session.commit()

    print(f"  고객:       {len(CUSTOMERS)}명")
    print(f"  상품:       {len(PRODUCTS)}개")
    print(f"  주문:       {len(orders)}건")
    print(f"  주문 항목:  {len(items)}개")
    print()

    # 간단 검증 쿼리
    with engine.connect() as conn:
        cnt = conn.execute(text("SELECT COUNT(*) FROM orders")).scalar()
        gold = conn.execute(
            text("SELECT COUNT(*) FROM customers WHERE grade = 'gold'")
        ).scalar()
        low_stock = conn.execute(
            text("SELECT name, stock FROM products WHERE stock < 10 ORDER BY stock")
        ).fetchall()

    print("─── 검증 쿼리 결과 ───────────────────────────────")
    print(f"  전체 주문 수:          {cnt}건")
    print(f"  gold 등급 고객 수:     {gold}명")
    print(f"  재고 10개 미만 상품:   {len(low_stock)}개")
    for name, stock in low_stock:
        print(f"    - {name}: {stock}개")
    print("─────────────────────────────────────────────────")
    print()
    print("완료! 아래 URL로 quickstart.md를 따라해 보세요:")
    print(f"  {db_url}")


# ── CLI ──────────────────────────────────────────────────────────────────────

DB_URLS = {
    "sqlite": "sqlite:///sample.db",
    "postgres": "postgresql://postgres:postgres@localhost:5432/postgres",
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="quickstart.md 샘플 DB 세팅")
    parser.add_argument(
        "--db",
        choices=["sqlite", "postgres"],
        default="sqlite",
        help="대상 DB (기본값: sqlite)",
    )
    parser.add_argument(
        "--url",
        default=None,
        help="직접 SQLAlchemy URL 지정 (--db 보다 우선)",
    )
    args = parser.parse_args()

    url = args.url or DB_URLS[args.db]
    setup(url)
