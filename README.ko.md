# Lang2SQL

> **자연어로 물으면 SQL을 짜주는 오픈소스 데이터 에이전트.**
> 단, 깨끗하게 정리된 DB가 아니라 — 컬럼 설명이 비어 있고, 팀마다 용어가 다른
> **현실의 지저분한 DB**에서도 동작하는 걸 목표로 합니다.

📄 English: [`README.md`](README.md) · 🧭 전체 그림: [`docs/PROJECT.md`](docs/PROJECT.md) · 🏗️ 구조: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)

---

## 한 줄 요약

디스코드에서 봇에게 **자연어로 질문하면 SQL을 만들어 실행하고 답**해줍니다.
다른 텍스트-투-SQL과 다른 점은 "질문→SQL" 그 자체가 아니라, 그 **주변**입니다:

- **🧩 빈 메타데이터 자동 채우기 (enrich)** — 컬럼 설명이 없어도, 에이전트가 *실제 값*을 읽어 "이 컬럼이 무슨 뜻인지 / 어느 테이블과 이어지는지"를 추론해 채웁니다.
- **🗂️ 팀마다 다른 용어 정의 (federation)** — 같은 "활성 고객"이 마케팅과 재무에서 다른 뜻이어도 충돌 없이 공존합니다. 회사 공통 정의 위에 팀별 정의를 얹고, **가까운 정의가 이깁니다(개인 > 팀 > 전사)**.
- **🛡️ 안전장치** — 모든 쿼리는 실행 전 검사를 통과해야 하고, 읽기(SELECT)만 허용합니다.

> Discord는 1단계(Phase 1) 인터페이스일 뿐, 본질이 아닙니다. Slack/Web은 같은 코어 위에 어댑터만 추가합니다.

---

## 빠른 시작 1 — 오프라인 데모 (토큰·DB 불필요)

가장 빠르게 핵심을 보는 방법. 디스코드 토큰도, 실제 DB도 필요 없습니다.

```bash
uv sync                                   # 가상환경 + 의존성 설치
.venv/bin/python bench/ecommerce_demo.py  # federation + safety 데모
```

같은 용어가 채널마다 다른 정의로 풀리는 federation 장면과, 위험한 쿼리(DROP/INSERT)가 막히고 SELECT만 통과하는 안전장치를 보여줍니다.

## 빠른 시작 2 — CLI (개발자용)

```bash
.venv/bin/lang2sql "테이블 목록 보여줘"
```

`OPENAI_API_KEY`가 있으면 `gpt-4.1-mini`로, 없으면 오프라인 `FakeLLM`(정해진 동작만, 실제 추론 X)으로 동작합니다.

---

## 디스코드 봇 셋업 (자세히)

### 0. 준비물
- Python **3.10 이상**, [uv](https://docs.astral.sh/uv/)
- 디스코드 계정 + 봇을 초대할 서버(길드)

### 1. 설치
```bash
git clone https://github.com/CausalInferenceLab/lang2sql.git
cd lang2sql
uv sync
```

### 2. 디스코드 봇 만들기
1. [Discord Developer Portal](https://discord.com/developers/applications) → **New Application**
2. 왼쪽 **Bot** 탭 → **Reset Token** → 토큰 **복사** (이게 `DISCORD_BOT_TOKEN`)
3. 같은 화면에서 **Privileged Gateway Intents → MESSAGE CONTENT INTENT** 켜기 (멘션 질문을 읽으려면 필요)
4. **OAuth2 → URL Generator** → scopes에 `bot` + `applications.commands` 체크 → 권한(읽기/메시지 보내기 등) 선택 → 생성된 URL로 봇을 **테스트 서버에 초대**

### 3. 환경변수 설정
`.env.example`을 복사해 `.env`를 만들고 채웁니다:

```bash
cp .env.example .env
```

```ini
DISCORD_BOT_TOKEN=여기에_봇_토큰        # 필수
OPENAI_API_KEY=sk-...                  # 실제 답변용 (없으면 가짜 LLM으로 떨어짐)
LANG2SQL_SECRET_KEY=                   # 선택 — 비밀(예: DB 비번) 암호화용 Fernet 키
LANG2SQL_DATA_PATH=lang2sql_data.db    # 선택 — 정의·세션 영속화 파일 (없으면 기본값)
LANG2SQL_SYNC_COMMANDS=true            # 슬래시 명령(/setup 등) 등록
LANG2SQL_DB_URL=                       # 선택 — 모든 채널이 쓸 기본 DB (아래 참고)
```

Fernet 키가 필요하면 생성:
```bash
.venv/bin/python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

> ⚠️ 앱은 `.env`를 자동으로 읽지 않습니다. 실행 직전에 셸에 로드하세요:
> `set -a; source .env; set +a`

### 4. 봇 실행
```bash
set -a; source .env; set +a   # .env를 환경변수로 로드
.venv/bin/lang2sql-bot
```
`DISCORD_BOT_TOKEN`이 없으면 명확한 에러를 내고 종료합니다. 정상이면 게이트웨이에 연결돼 서빙을 시작합니다.

### 5. DB 연결
두 가지 방법:

**(A) 디스코드에서 `/setup`** — 비개발자용 가이드 폼. DSN을 직접 타이핑하지 않아도 됩니다.
- `/setup` → DB 종류 선택 → 폼 작성 → 연결 테스트 후 **암호화 저장**
- 지원: **PostgreSQL · MySQL · BigQuery · Snowflake · DuckDB · Cloudflare D1**
- 예) DuckDB → path 칸에 `/절대경로/파일.duckdb`

**(B) 환경변수 `LANG2SQL_DB_URL`** — 봇 실행 전에 걸면 모든 채널이 그 DB를 씁니다.
```ini
LANG2SQL_DB_URL=postgresql://user:pw@host:5432/db
# 또는
LANG2SQL_DB_URL=duckdb:////절대/경로/파일.duckdb   # 슬래시 4개 = 절대경로
```

> `/connect`는 V1에서 **저장만 하고 실제 연결은 안 되는** 미완성 명령입니다. 실제 연결은 `/setup`을 쓰세요.

### 6. 사용
- **자연어 질문** — 채널에서 봇을 멘션하거나 DM: `@Lang2SQL 국가별 매출 알려줘`
- **`/enrich`** — 컬럼 의미·테이블 관계 자동 보강 (질문 품질이 크게 올라감)
- **`/term_custom`** — 비즈니스 용어 등록/조회/삭제
- **`/org_setup`** — DB 스캔으로 용어 자동 추출 (`org:`=전사, `team:`=이 채널)

---

## 슬래시 명령어

| 명령 | 설명 |
|---|---|
| `/setup` | DB 연결 (가이드 폼, DSN 불필요) — **실제 연결 경로** |
| `/enrich` | 컬럼 메타데이터 자동 보강 (`clear:true`로 초기화) |
| `/term_custom` | 비즈니스 용어 등록·조회(`action:show`)·삭제(`action:remove`) |
| `/org_setup` | 조직(`org:`)/팀(`team:`) 등록 + DB 스캔 용어 자동 추출 |
| `/remember` | 사실/선호를 기억 |
| `/ingest` | 문서에서 정의 후보 제안 |
| `/audit_me` | 내 최근 활동 보기 |
| `/connect` | (V1 미완성 — 저장만 함, 쓰지 말 것) |

자연어 질문은 슬래시가 아니라 **멘션/DM**으로. 에이전트가 필요하면 위 도구들을 스스로 호출합니다.

---

## 지금 되는 것 / 아직인 것 (정직하게)

**됩니다**
- 3계층 federation (전사/팀/개인) + 가까운 정의 우선 + 대화로 정의 등록
- 실제 외부 DB 연결 (PostgreSQL/MySQL/DuckDB/BigQuery/Snowflake/D1, SQLAlchemy 기반)
- enrich — 실제 값 샘플 기반 컬럼 의미·관계 자동 추론
- 안전장치 (읽기 전용, 위험 쿼리 차단), 도구 8종, 암호화 비밀 저장, SQLite 영속화

**아직입니다**
- 실제 사내 프로덕션 DB에 대규모 검증 (벤치마크로 추적 중)
- 자동 메타데이터 보강 고도화, 벡터 recall, URL/Notion 문서 입력, 비용 게이트 등 (V1.5+)

---

## 기여하기

```bash
uv sync
.venv/bin/pytest -q     # 전체 테스트 통과 확인
```
- 새 기능엔 테스트(`tests/test_<layer>.py`) 추가
- PR은 `master` 대상, 커밋 메시지에 `feat:`/`fix:`/`docs:` prefix
- 어디를 손대면 좋은지: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)

## 라이선스 / 커뮤니티

[가짜연구소](https://pseudo-lab.com/) 인과추론팀에서 개발 중. [MIT License](https://opensource.org/licenses/MIT). 💬 [Discord](https://discord.gg/EPurkHVtp2)
