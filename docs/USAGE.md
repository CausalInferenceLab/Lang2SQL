# Lang2SQL 사용 가이드

Discord에서 자연어로 질문하면 SQL을 생성하고 결과를 돌려주는 분석 에이전트입니다.

---

## 빠른 시작

봇이 서버에 있다면 바로 질문할 수 있습니다.

```
@Lang2SQL 이번 달 매출 상위 고객 10명 알려줘
```

처음 사용한다면 아래 순서대로 세팅합니다.

---

## 1단계: DB 연결

DB가 연결되지 않으면 SQL을 실행할 수 없습니다.

```
/setup
```

DB 종류(PostgreSQL, MySQL, SQLite 등)를 선택하는 안내가 나타납니다. 접속 정보를 입력하면 연결됩니다. DSN을 직접 알고 있다면 `/connect dsn:...`으로 바로 입력할 수도 있습니다.


---

## 2단계: 비즈니스 용어 등록

"월매출", "활성고객"처럼 회사 내부 용어를 등록해두면 LLM이 SQL을 훨씬 정확하게 만듭니다.

### 방법 A — 텍스트에서 자동 추출

문서나 정의집 내용을 붙여넣으면 후보를 자동으로 뽑아줍니다.

```
/ingest content:월매출은 SUM(orders.amount)이고, 활성고객은 30일 내 로그인한 users, 환불제외는 status != 'cancelled'
```

후보 목록이 표시되면 확인 후 등록합니다.

```
/confirm_ingest ref:inline:xxxx accept:all layer:channel
```

- `accept`: `all`이면 전체, `1,3`처럼 번호를 지정하면 선택 등록
- `layer`: `channel`(이 채널 전용), `guild`(전사 공통, 관리자 전용), `member`(개인)

### 방법 B — 직접 등록

```
/term_custom
```

안내에 따라 용어명, 정의, 종류(metric/rule/dimension/table)를 입력합니다.

### 방법 C — DB 스캔으로 자동 추출

```
/org_setup org:회사명
```

DB 스키마를 분석해 비즈니스 용어 후보를 자동으로 뽑습니다.

---

## 3단계: 질문하기

용어를 등록한 뒤 자연어로 질문합니다.

```
@Lang2SQL 월매출 기준 이번 달 상위 고객 10명 보여줘
@Lang2SQL 환불제외 기준으로 채널별 주문 수 알려줘
```

봇이 SQL을 생성해 실행하고 결과를 표시합니다.

---

## 용어 우선순위 (Federation)

같은 용어가 여러 레이어에 등록된 경우 **좁은 범위가 우선** 적용됩니다.

```
개인(member) > 채널(channel) > 전사(org)
```

예를 들어 "활성고객"을 전사에서는 "30일 내 로그인"으로 정의했더라도, 마케팅 채널에서 "14일 내 로그인"으로 따로 등록하면 마케팅 채널 안에서만 그 정의가 우선 적용됩니다. 다른 채널에는 영향이 없습니다.

---

## 전체 커맨드 목록

| 커맨드 | 설명 |
|---|---|
| `/setup` | DB 연결 마법사 |
| `/connect dsn:...` | DSN으로 직접 DB 연결 |
| `/ingest content:...` | 텍스트에서 용어 후보 추출 |
| `/confirm_ingest ref:... accept:... layer:...` | 추출된 후보 검토 후 등록 |
| `/term_custom` | 용어 직접 등록 (위저드) |
| `/term_custom action:show` | 등록된 용어 전체 조회 |
| `/term_custom action:remove term:용어명` | 용어 삭제 |
| `/org_setup org:...` | 전사 조직 등록 + DB 스캔 |
| `/org_setup team:...` | 팀(채널) 등록 |
| `/enrich` | DB 컬럼 메타데이터 자동 보강 |
| `/remember text:...` | 사실 기억 저장 |
| `/audit_me` | 내 활동 이력 조회 |
| `/help` | 사용 방법 안내 |

---

## 자주 묻는 질문

**Q. 질문했는데 엉뚱한 SQL이 나와요.**
등록된 용어가 없거나 DB 메타데이터가 부족한 경우입니다. `/enrich`로 컬럼 설명을 보강하거나 `/term_custom`으로 관련 용어를 등록해보세요.

**Q. "org 용어는 관리자만 등록 가능" 오류가 나요.**
`layer:org`는 관리자 권한이 필요합니다. `layer:team`으로 채널 범위로 등록하거나 관리자에게 요청하세요.

**Q. 이전 대화 내용을 기억하나요?**
같은 채널(또는 DM 스레드)에서 이어지는 대화는 맥락이 유지됩니다. `/remember`로 중요한 사실을 명시적으로 저장할 수도 있습니다.

### Qdrant로 관련 테이블 찾기

`search_schema`는 자연어 질문과 유사한 테이블을 최대 5개 반환합니다. 에이전트는 후보 테이블의 컬럼을 `explore_schema`로 확인한 뒤 SQL을 작성할 수 있습니다. CLI, Discord, MCP에서 같은 검색 도구를 사용합니다. 검색을 켠 내장 에이전트는 프롬프트에 전체 테이블 목록을 미리 넣지 않습니다. 검색 결과가 부족하면 인자 없는 `explore_schema`로 전체 목록을 조회할 수 있습니다.

아래는 로컬 Qdrant와 Ollama의 OpenAI 호환 임베딩 API를 쓰는 예입니다. 임베딩 서버에는 테이블명·컬럼명·타입·DB 설명과 검색 질문이 전송됩니다. 데이터 행은 색인하지 않습니다.

```sh
docker compose -f docker/docker-compose-qdrant.yml up -d
ollama pull bge-m3
export LANG2SQL_QDRANT_URL=http://localhost:6333
export LANG2SQL_EMBEDDING_URL=http://localhost:11434/v1/embeddings
export LANG2SQL_EMBEDDING_MODEL=bge-m3
export LANG2SQL_DB_URL=sqlite:////absolute/path/to/analytics.sqlite

uv run --extra tools lang2sql-tools --org-id analytics --user-id operator index-schema
uv run --extra tools lang2sql-tools --org-id analytics --user-id analyst \
  call search_schema --args '{"query":"고객별 재구매율"}'
```

Ollama 서버를 먼저 실행하세요. 인증이 필요한 배포에서는 각각 `LANG2SQL_QDRANT_API_KEY`와 `LANG2SQL_EMBEDDING_API_KEY`를 설정합니다. Qdrant 주소를 생략하면 기존 도구 목록을 사용합니다.

색인을 만드는 운영자와 검색하는 프로세스에서 같은 조직 ID, DB 연결 문자열·스키마, 임베딩 URL·모델을 사용하세요. Discord에 조직별 연결을 등록했다면 그 연결을 색인 명령에 지정합니다. 인메모리 SQLite는 프로세스 간에 공유되지 않아 지원 대상에서 제외합니다. 컬럼 변경·테이블 삭제 후에는 같은 명령을 다시 실행하세요. 완성된 새 컬렉션으로 별칭을 교체하므로 검색 중에는 완성된 색인만 보입니다. 게시 시점의 연결 단절이나 동시에 수행한 갱신으로 남은 컬렉션은 활성 별칭을 확인한 뒤 정리할 수 있습니다.
