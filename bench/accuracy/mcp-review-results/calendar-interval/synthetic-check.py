"""Independent offline oracle check; never included in model input."""
import json
import random
import sqlite3
from datetime import date, timedelta

from dateutil.relativedelta import relativedelta
from lang2sql.tools.calendar_interval_sql import CALENDAR_CTES

rng = random.Random(42)
rows = []
expected = []
for i in range(1000):
    start = date(1900, 1, 1) + timedelta(days=rng.randrange(73000))
    end = start + timedelta(days=rng.randrange(36525))
    delta = relativedelta(end, start)
    rows.append((i, start.isoformat(), end.isoformat()))
    expected.append((i, 1, delta.years, delta.months, delta.days))
with sqlite3.connect(':memory:') as conn:
    conn.execute('CREATE TABLE calendar_input(row_id, start_date, end_date)')
    conn.executemany('INSERT INTO calendar_input VALUES(?,?,?)', rows)
    actual = conn.execute('WITH ' + CALENDAR_CTES + ' SELECT * FROM calendar_interval ORDER BY row_id').fetchall()
assert actual == expected
print(json.dumps({'seed': 42, 'pairs': len(rows), 'mismatches': 0, 'sqlite': sqlite3.sqlite_version}))
