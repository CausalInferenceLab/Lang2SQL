import pathlib,json,sqlite3,hashlib,zipfile,math,ast,collections
r=pathlib.Path('/Users/seyoung/workspace/benchmarks');run=r/'codex-mcp-integrated-20261127/spider-gpt-6.2-sol';m=json.loads((run/'manifest.json').read_text());case=next(c for c in m['cases'] if c['instance_id']=='local010');db=r/'spider2-databases/Airlines.sqlite';doc=r/'Spider2/spider2-lite/resource/documents'/case['external_knowledge'];ref=r/'Spider2/spider2-lite/evaluation_suite/gold/exec_result/local010.csv'
sha=hashlib.sha256(db.read_bytes()).hexdigest();assert sha==m['db_sha256']['Airlines'];assert hashlib.sha256(doc.read_bytes()).hexdigest()==case['document_sha256']
with zipfile.ZipFile(r/'spider2-localdb.zip') as z:assert hashlib.sha256(z.read('Airlines.sqlite')).hexdigest()==sha
sql=(run/'codex-mcp/sql/local010.sql').read_text();counter=sql.replace('MIN(departure.city, arrival.city) AS city1','departure.city AS city1').replace('MAX(departure.city, arrival.city) AS city2','arrival.city AS city2');assert counter!=sql
c=sqlite3.connect(f'file:{db}?mode=ro',uri=True);variants=[]
for label,s in [('documented_unordered_pairs',sql),('directional_pairs_counterfactual',counter)]:
 bins=s[:s.index('SELECT pair_count\nFROM range_counts')]+'SELECT lower_km,pair_count FROM range_counts ORDER BY lower_km'
 variants.append({'label':label,'sql':s,'result':[list(x) for x in c.execute(s)],'bins':[list(x) for x in c.execute(bins)]})
airports={}
for code,city,coordinates in c.execute('SELECT airport_code,city,coordinates FROM airports_data'):
 lon,lat=ast.literal_eval(coordinates);airports[code]=(json.loads(city)['en'],math.radians(lon),math.radians(lat))
pairs=collections.defaultdict(list)
for dep,arr in c.execute('SELECT departure_airport,arrival_airport FROM flights'):
 name1,lon1,lat1=airports[dep];name2,lon2,lat2=airports[arr]
 v=math.sin((lat2-lat1)/2)**2+math.cos(lat1)*math.cos(lat2)*math.sin((lon2-lon1)/2)**2
 distance=2*6371*math.asin(math.sqrt(min(1,v)))
 pairs[tuple(sorted((name1,name2)))].append(distance)
bins={i:0 for i in range(0,7000,1000)}
for values in pairs.values():
 avg=sum(values)/len(values);bins[min(6000,int(avg//1000)*1000)]+=1
assert list(bins.items())==[tuple(x) for x in variants[0]['bins']]
assert all(b[1]==2*a[1] for a,b in zip(variants[0]['bins'],variants[1]['bins']))
report={'instance_id':'local010','scope':'Offline reference/input audit; not a model prediction, no grading override and no inference feedback','question':case['question'],'database_sha256':sha,'zip_member':'spider2-localdb.zip:Airlines.sqlite','document':case['external_knowledge'],'document_sha256':case['document_sha256'],'document_rule':'The Symmetry of Routes section explicitly standardizes city order so a flight A->B is treated the same as B->A.','reference_sha256':hashlib.sha256(ref.read_bytes()).hexdigest(),'reference_csv':ref.read_text(),'variants':variants,'independent_python_bins':bins,'python_check':'Independent Haversine calculation, averaging all flight distances by sorted city-name pair, reproduces every SQL unordered-pair bin.','conclusion':'Under the pinned database and supplied symmetry rule the minimum is3. The reference6 is obtained when counting the two directions separately; all seven bins exactly double. This is a documented reference/input inconsistency, not a local DB mutation. Official grade remains0.'}
out=pathlib.Path('bench/accuracy/experiments/astra-multistage/airlines-reference-audit.json');out.write_text(json.dumps(report,indent=2)+'\n');print('verified',bins,'minimum',min(bins.values()))
