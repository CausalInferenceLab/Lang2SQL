# Oracle 뷰에 팀 지표 연결하기

이 데모는 한 행이 주문 한 건인 뷰에서 확정 주문의 순매출 합계를 조회합니다. 담당자는 숫자 컬럼, 날짜 컬럼, 집계 단위, 집계 방법, 필수 조건을 먼저 확인합니다. 지원 계약은 Oracle 일반 뷰 하나, NUMBER 컬럼의 SUM, DATE 컬럼의 시작일 포함·종료일 제외 범위, 문자 컬럼의 등호 조건입니다. 조건이 없으면 `filters`에 빈 객체를 명시합니다. 뷰 로직의 업무 의미와 신뢰성은 등록 담당자가 검토해야 합니다. Oracle 함수·패키지·프로시저 연결은 후속 범위입니다.

## 빈 데모 DB 준비

아래 명령을 저장소 루트에서 실행합니다. fixture는 새 사용자 두 명과 합성 데이터만 생성하며, 같은 사용자가 이미 있으면 중단합니다. Oracle과 호환되는 Docker 엔진, uv, openssl이 필요합니다.

```sh
uv sync --extra oracle --extra tools --extra mcp
export ORACLE_PASSWORD="$(openssl rand -hex 18)"
docker run -d --rm --name l2s-view-demo -e ORACLE_PASSWORD -p 127.0.0.1:11522:1521 gvenzl/oracle-free:23.9-slim-faststart
until docker exec l2s-view-demo healthcheck.sh >/dev/null 2>&1; do sleep 3; done
docker cp bench/oracle_view/fixture.sql l2s-view-demo:/tmp/fixture.sql
docker exec l2s-view-demo bash -c 'sqlplus -s /nolog <<SQL
CONNECT / AS SYSDBA
ALTER SESSION SET CONTAINER=FREEPDB1;
@/tmp/fixture.sql "$ORACLE_PASSWORD"
SQL'
export LANG2SQL_DB_URL="oracle+oracledb://L2S_VIEW_READER:${ORACLE_PASSWORD}@127.0.0.1:11522/?service_name=FREEPDB1"
export LANG2SQL_DB_SCHEMA=l2s_view_owner
```

## 담당자 등록과 동료 조회

`--store`는 등록·조회 프로세스가 함께 사용하는 메타데이터 파일입니다. 기존 텍스트 정의가 정확히 일치해야 연결할 수 있습니다. 운영자가 CLI로 연결을 확정하며, 등록 시점과 조회 시점 모두 설정된 DB 계정의 권한을 사용합니다. 확인한 계약은 [contract.json](contract.json)에 있습니다.

```sh
export TERM_STORE="$(mktemp)"
definition='확정 주문의 할인·반품 차감액 합계'
uv run lang2sql-tools --store "$TERM_STORE" --org-id demo --team-id sales --user-id registrar call term_custom \
  --args '{"term":"순매출","kind":"metric","layer":"team","definition":"확정 주문의 할인·반품 차감액 합계"}'
uv run lang2sql-tools --store "$TERM_STORE" --org-id demo --team-id sales --user-id registrar bind-oracle-view \
  --term 순매출 --layer team --definition "$definition" --contract bench/oracle_view/contract.json
uv run lang2sql-tools --store "$TERM_STORE" --org-id demo --team-id sales --user-id colleague call run_sql \
  --args '{"metric_term":"순매출","period_start":"2025-10-01","period_end":"2025-11-01"}'
```

10월 결과는 **118**입니다. 10월 1일 주문은 85, 10월 31일 23시 주문은 33입니다. 취소된 주문 999와 11월 1일 주문은 제외합니다. 9월은 25, 11월은 10, 주문이 없는 12월은 SQL SUM의 NULL입니다.

MCP에서는 동료가 새 세션에서 `resolve_business_terms({"terms":["순매출"]})`를 호출하고, 받은 `definition_check_id`와 두 날짜를 `run_sql`에 전달합니다. 뷰가 연결된 정의로 자유 SQL을 제출하면 중단합니다. 결과에는 정의·레이어·출처·조건·바인드 값·실행 SQL과 행이 들어갑니다. 에이전트가 사용자에게 보여주는 요약은 이 도구 결과보다 짧을 수 있습니다.

조직·팀·개인 우선순위는 기존 정의 조회 규칙을 따릅니다. DB 접속 경로와 서비스, 뷰 식별자·DDL 시각·정의·출력 컬럼이 바뀌면 운영자가 다시 확인하고 연결합니다. 메타데이터 파일이나 조직 ID만으로 DB 권한을 부여하지 않습니다. 기존 Markdown 용어 번들은 텍스트를 전달하며 새 환경에서 뷰 연결을 새로 확인해야 합니다. 이 기능은 명시적으로 해결한 업무 용어의 계약을 검사합니다. 임의 SQL의 업무 의미를 추론하는 일반 권한 통제 장치는 아닙니다.

## 실제 검증 결과

두 개의 별도 Oracle Free 23.9 인스턴스에 같은 fixture를 설치했습니다. 동료 계정으로 새 세션에서 네 달을 조회해 **4/4** 기대값을 확인했습니다. 다른 팀에서는 정의를 찾을 수 없었습니다. 동명 뷰를 둔 두 번째 DB, SELECT 권한 회수, 금액 컬럼 변경, 뷰 삭제를 각각 검사해 4회 모두 조회가 중단됐습니다. 감사 기록에서 성공한 지표 SQL 4건, 차단 4건, 원본 테이블 대체 실행 0건을 확인했습니다.

별도 자연어 실험은 GPT-4.1 mini(2025-04-14), 월별 질문 4개, 질문당 최대 8회 호출, 같은 조회 계정·DB·스키마 정보를 사용했습니다. 각 질문은 새로운 MCP 프로세스에서 실행했습니다. OpenAI API 키가 있는 환경에서 아래 명령으로 반복할 수 있습니다.

```sh
uv run python bench/oracle_view/evaluate.py > comparison.json
```

| 최종 설정, 각 4문항 | SQL 실행 | 기대 결과 일치 | 뷰 사용 | 원본 테이블 대체 | 모델 호출 | 자동 등록 시간 |
|---|---:|---:|---:|---:|---:|---:|
| 텍스트 정의 | 1 | 1 | 1 | 0 | 12 | 0.20초 |
| 같은 정의 + 뷰 연결 | 2 | 2 | 2 | 0 | 10 | 0.44초 |

[문항별 SQL·결과·모델 응답](results.json)에 실패도 포함했습니다. 텍스트 조건은 11월, 연결 조건은 9월과 12월에 정답을 냈습니다. 후속 답변을 제공한 담당자는 양쪽 모두 0명이었습니다. 텍스트 조건에서 해결되지 않은 3건, 연결 조건에서 2건은 모델이 사용자에게 기간이나 정의를 다시 확인하려 했습니다. 계정 권한 설정과 사람의 계약 검토 시간은 위 자동 등록 시간에 포함되지 않습니다.

예비 실행에서는 MCP 서버 안내를 클라이언트 프롬프트에 전달하지 않았을 때 두 조건 모두 0/4였습니다. 서버 안내를 전달하되 소유자 스키마를 지정하지 않은 실행은 텍스트 0/4, 연결 2/4였습니다. 작은 합성 데이터셋의 설정별 단일 실행이므로 고객 정확도나 시간 절감률을 추정할 수 없습니다.

## 발표 리허설과 Codex 연결

발표 데모는 **Codex → Lang2SQL MCP → Oracle** 직접 연결입니다. SQLcl MCP나 Select AI를 경유하지 않습니다. 위 fixture는 2025년 10월 순매출 **118원**이며, 이전 `bench/oracle_demo.py`의 170만 원 데이터와 별개입니다. 소스 DB 권한과 팀별 용어 범위도 별개입니다. `L2S_VIEW_READER`는 fixture의 테이블·뷰 SELECT만 받으며, 뷰 연결 확정은 모델에게 노출되지 않는 운영자 CLI로 수행합니다.

소스 DB에는 `fixture.sql`을 한 번만 설치합니다. 초기화할 때 기존 고객 DB나 사용자 객체를 삭제하지 마세요. 데모 metadata는 매번 새로운 경로를 지정하면 미정의 상태부터 재현할 수 있습니다.

```sh
# 위 DB 준비 및 LANG2SQL_DB_URL/LANG2SQL_DB_SCHEMA 설정 후 저장소 루트에서 실행
uv run python bench/oracle_view/rehearse.py \
  --store /absolute/path/rehearsal-01.db \
  --output /absolute/path/rehearsal-01.json
```

이 명령은 실제 stdio MCP에서 뷰 발견·대문자 스키마 한정 이름의 컬럼 조회, 미정의/위조 ID 차단, 운영자 등록, 동료의 새 세션별 4개월 조회, 확인 ID 재사용 차단, 원본 테이블 SQL 대체 차단, 다른 팀의 정의 미조회와 감사 횟수를 확인합니다. 정답을 생성하는 모델은 없으며, 실패하면 종료 코드가 0이 아닙니다. 기존 출력/metadata 파일은 덮어쓰지 않습니다.

Codex CLI가 설치되고 로그인된 환경에서 실제 자연어 조회도 검사할 수 있습니다. 모델을 지정하지 않으면 CLI 기본 모델을 사용합니다. 기대값은 채점에만 사용하며 Codex 질문에는 전달하지 않습니다.

```sh
uv run python bench/oracle_view/codex_demo.py \
  --store /absolute/path/rehearsal-01.db \
  --output /absolute/path/codex-sales-01.json --expect-value 118
uv run python bench/oracle_view/codex_demo.py \
  --store /absolute/path/rehearsal-01.db \
  --output /absolute/path/codex-finance-01.json --team finance --expect-clarification
```

이 검사는 사용자 Codex 설정을 변경하지 않는 별도 실행입니다. 빈 작업 디렉터리, read-only shell sandbox, 조회 도구 세 개만 노출하며 이번 실행에 한해 해당 도구 사용을 승인합니다. 첫 연결 실험에서는 비대화형 approval_policy=never와 도구 기본 승인 정책 때문에 조회가 거부되었습니다. 전체 권한을 해제하지 않고 요청된 데모 조회 도구에만 실행별 승인을 지정했습니다. 원문 도구 결과·최종 답변·감사 기록의 SQL 횟수를 함께 보존합니다. `--expect-clarification`은 도구 호출과 실행 0회를 자동 검사하며, 질문의 적절성은 저장된 답변을 읽어 확인해야 합니다. CLI 종료 코드만으로 성공을 판정하지 않습니다.

### Codex에 지속 연결

현재 사용하는 checkout에서 위 store를 만든 뒤 다음처럼 등록합니다. `uv`와 파일 경로는 모두 실제 절대 경로로 바꾸세요. `.env`는 사용자만 읽을 수 있게 저장하고 Git에 추가하지 않습니다.

```sh
chmod 600 /absolute/path/Lang2SQL/.env
codex mcp add lang2sql-oracle -- \
  /absolute/path/uv run --project /absolute/path/Lang2SQL \
  --env-file /absolute/path/Lang2SQL/.env --no-sync lang2sql-mcp \
  --store /absolute/path/rehearsal-01.db \
  --org-id demo --team-id sales --user-id presenter
```

새 Codex 세션을 열고 `lang2sql-oracle` 연결에서 “2025년 10월 순매출은 얼마야? 적용 기준과 실행 SQL도 보여줘”라고 질문합니다. 클라이언트 도구 승인 정책은 그대로 유지하므로 요청 시 조회 도구 사용을 승인합니다. 사용자·팀은 서버 실행 인자로 고정되며, 다른 팀 데모는 별도 프로세스/설정으로 실행해야 합니다. `.env`에는 이 데모 전용 연결 정보만 두세요. MCP 서버는 LLM 키를 요구하지 않습니다.

### 청중에게 설명할 경계

- 업무 뷰의 계산식은 Oracle에 있고, Lang2SQL은 확정된 뷰·필터·집계·기간 계약으로 SQL을 구성합니다.
- 문장 정의를 저장하는 것과 뷰 사용 계약을 확정하는 것은 다른 단계입니다.
- 뷰 계약은 NUMBER 합계·DATE 기간·문자 등호 필터로 제한됩니다. 함수/패키지 실행은 지원하지 않습니다.
- 미정의 용어를 빠짐없이 제출하는지, 텍스트 정의 SQL의 의미가 맞는지는 별도 검증 대상입니다. 원본 테이블 사용 차단은 확인된 뷰 지표 요청에 적용되며 일반 DB 접근통제가 아닙니다.
- Codex에 정의·스키마·질의 결과가 전달됩니다. 합성 데이터 외의 환경에서는 어떤 데이터를 제공할지 운영자가 검토해야 합니다.
- 단일 데모 성공은 정확도나 시간 절감률의 일반적인 보장이 아닙니다.
