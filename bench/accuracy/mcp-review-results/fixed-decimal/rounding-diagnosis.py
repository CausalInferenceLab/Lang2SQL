"""Offline diagnosis of a prior failure; never passed to inference."""
import csv
import hashlib
import json
import sqlite3
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from lang2sql.tools.fixed_decimal_sql import fixed_decimal_ctes

root = Path('/Users/seyoung/workspace/benchmarks')
db = root / 'spider2-databases/imdb_movies.sqlite'
prior = root / 'codex-mcp-astra-spider-backlog-20261127/spider-gpt-6.2-astra'
rows = list(csv.DictReader((prior / 'codex-mcp/csv/local152.csv').open()))
with sqlite3.connect('file:' + str(db) + '?mode=ro', uri=True) as conn:
    precision = conn.execute('SELECT MAX(ABS(avg_rating * 10 - ROUND(avg_rating * 10))) FROM ratings').fetchone()[0]
    query = ('WITH decimal_input AS (SELECT d.name_id AS group_id, '
             'CAST(ROUND(r.avg_rating * 10) AS INTEGER) AS scaled_value '
             'FROM director_mapping d JOIN ratings r ON r.movie_id = d.movie_id), '
             + fixed_decimal_ctes(1, 2, 'avg') + ' SELECT group_id, status, rounded_value FROM decimal_result')
    exact = {r[0]: r[1:] for r in conn.execute(query)}
    diagnostic = []
    for row in rows:
        values = [x[0] for x in conn.execute('SELECT r.avg_rating FROM director_mapping d JOIN ratings r ON r.movie_id = d.movie_id WHERE d.name_id = ?', (row['director_id'],)) if x[0] is not None]
        dec = sum(Decimal(str(v)) for v in values) / len(values)
        rounded = dec.quantize(Decimal('.01'), rounding=ROUND_HALF_UP)
        assert exact[row['director_id']] == ('ok', float(rounded))
        diagnostic.append({'director_id': row['director_id'], 'values': values, 'decimal_average': str(dec), 'decimal_rounded': str(rounded), 'prior_sql_average': row['avg_rating'], 'compiler_sql_average': exact[row['director_id']][1]})
print(json.dumps({'status':'offline only; not a model prediction or official score', 'db_sha256':hashlib.sha256(db.read_bytes()).hexdigest(), 'max_source_scale_residual': precision, 'query':query, 'rows':diagnostic}, indent=2))
