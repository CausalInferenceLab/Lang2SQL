"""Show the real CO fixture and independent answers with the read-only DB account."""

import json
import os

from sqlalchemy import create_engine, text

QUERIES = {
    "account": "SELECT USER AS account FROM dual",
    "tables": "SELECT table_name FROM all_tables WHERE owner='CO' ORDER BY table_name",
    "sample_orders": "SELECT order_id,order_tms,order_status FROM co.orders ORDER BY order_id FETCH FIRST 5 ROWS ONLY",
    "counts": "SELECT (SELECT COUNT(*) FROM co.orders) orders,(SELECT COUNT(*) FROM co.order_items) order_items,(SELECT COUNT(*) FROM co.customers) customers FROM dual",
    "october_reference": "SELECT COUNT(DISTINCT o.order_id) order_count, SUM(CASE WHEN o.order_status='COMPLETE' THEN i.unit_price*i.quantity ELSE 0 END) sales, SUM(CASE WHEN o.order_status IN ('COMPLETE','REFUNDED') THEN i.unit_price*i.quantity ELSE 0 END) finance FROM co.orders o JOIN co.order_items i ON o.order_id=i.order_id WHERE o.order_tms>=TIMESTAMP '2021-10-01 00:00:00' AND o.order_tms<TIMESTAMP '2021-11-01 00:00:00'",
}

if __name__ == "__main__":
    engine = create_engine(os.environ["LANG2SQL_DB_URL"], hide_parameters=True)
    try:
        with engine.connect() as conn:
            for label, query in QUERIES.items():
                print(label, "\nSQL>", query)
                print(
                    json.dumps(
                        [dict(row) for row in conn.execute(text(query)).mappings()],
                        ensure_ascii=False,
                        default=str,
                        indent=2,
                    )
                )
    finally:
        engine.dispose()
