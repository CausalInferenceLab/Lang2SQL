# Codex × Lang2SQL × Oracle

Codex CLI가 질문 해석·도구 선택·최종 답변을 맡고, Lang2SQL MCP가 부서별 정의·스키마·SQL 안전 검사·Oracle 실행을 제공한다. 기존 `agent_loop`를 안쪽에서 다시 돌리지 않는다. 기존 CLI/Discord는 호환성을 유지한다.

## 실행

Codex CLI 0.166.0, MCP Python SDK 1.30.0, Oracle Free 23.9에서 검증했다. Codex CLI를 설치하고 `codex login`을 먼저 실행한다. 이 경로는 기존 OPENAI_API_KEY 기반 내부 LLM을 호출하지 않는다. Codex의 인증·이용 한도가 적용된다.

```bash
uv sync --extra codex --extra oracle
export LANG2SQL_DB_URL='oracle+oracledb://readonly_user:password@localhost:11521/?service_name=FREEPDB1'
export LANG2SQL_DB_SCHEMA=l2s_demo_owner
export LANG2SQL_DATA_PATH=/absolute/path/context.sqlite
export LANG2SQL_USER_ID=presenter
export LANG2SQL_ORG_ID=oracle-demo
export LANG2SQL_TEAM_ID=sales
uv run --extra codex --extra oracle lang2sql-codex '2026년 10월 매출 얼마야?' --timeout 180 --output /tmp/answer.json
```

URL은 실제 조회 전용 계정으로 교체한다. `.env` 자동 로드는 하지 않으므로 환경변수를 export한다. DB URL과 신원 3개는 필수이며 누락 시 실행 전에 실패한다. 정의는 기존 `SemanticFederationTool`/저장소로 등록한다. 빈 저장소에 데모 정의가 자동으로 생기지 않는다. 데모 벤치마크는 정의 등록까지 수행한다.

`--model`을 지정하지 않으면 Codex CLI 기본 모델을 쓴다. 결과의 `Codex CLI default`는 모델 ID를 추정한 값이 아니다. 재현 실험에서는 사용 가능한 모델 ID를 명시하는 편이 좋다.

## 요청 경로와 중단

1. wrapper가 빈 임시 디렉터리에서 1회용 `codex exec`를 실행한다. 사용자 config를 제외하고 read-only sandbox, shell/web/apps/multi-agent 비활성화, 필수 Lang2SQL MCP만 설정한다.
2. MCP 신원과 DB 계정은 프로세스 시작 환경으로 고정된다. 도구 인자로 변경할 수 없다. `get_context` 전 SQL은 거부한다.
3. `get_context`는 기존 시스템 프롬프트와 부서 정의를 재사용한다. `explore_schema`, `run_sql`, `ask_user`도 기존 구현을 재사용한다. 정의 쓰기·설정 변경 도구는 노출하지 않는다.
4. `run_sql`은 기존 SafetyPipeline → SqlAlchemyExplorer → Oracle을 통과한다. 실제 테이블 SELECT 권한은 Oracle 계정이 결정한다.
5. `ask_user` 또는 권한 오류의 `stop_turn` 이후에는 MCP가 모든 추가 호출을 차단한다. 호출은 lock으로 직렬화한다. 이미 먼저 시작된 SQL을 뒤늦은 clarification으로 취소하는 구조는 아니다.
6. wrapper는 정상 완료 이벤트·답변·Lang2SQL context 도구 호출을 확인한다. timeout/cancel 시 POSIX 프로세스 그룹을 종료한다. 이는 Oracle 서버 측 실행시간 제한을 대체하지 않는다.

이 서버는 **한 질문당 한 프로세스**다. 다음 질문·추가 설명은 wrapper를 새로 실행한다. 영구 등록한 MCP 서버의 다중 대화·세션 reset은 이번 범위가 아니다. 대화 기록도 요청 간 자동 승계하지 않는다.

## 실제 Oracle 평가 재현

[Oracle 준비 문서](oracle_demo.md)의 Docker 생성과 `seed`를 한 번 실행한다. 기존 seed는 이미 존재하는 사용자를 지우지 않고 중단한다. 아래 벤치마크의 DB URL은 폐기 가능한 fixture DB의 관리 연결이며, **Codex 자식에는 persona별 읽기 계정 URL만 전달**한다.

```bash
uv run --extra codex --extra oracle python bench/codex_oracle_demo.py --repeats 3 --output /tmp/codex-oracle.json
uv run --extra codex --extra oracle pytest -q
```

| 사례 | 판정 근거 |
|---|---|
| 영업 매출 | 실제 SQL 재실행 결과 2,000,000원 |
| 재무 매출 | 실제 SQL 재실행 결과 1,700,000원 |
| 충성고객 미정의 | 업무 SQL 0회 + ask_user 중단 |
| HR 급여 | sales 3,000,000 / finance 4,000,000 |
| 영업 급여 | 독립 권한 probe ORA-00942/01031 + 실제 에이전트 조회 실패 + 결과 없음 |

`bench/codex/results/first.json`은 첫 5/5 통과 기록이다. JSON에는 실제 MCP 호출, SQL, DB 사용자, 재조회한 행, Codex 답변과 JSONL 이벤트를 남긴다. 모든 데이터는 합성이며, 기존 `bench/oracle/results/live.json`의 gpt-4.1-mini 결과와 섞지 않는다. 고정 5개 사례를 반복하는 검증이지 일반 정확도 추정이 아니다.

## 운영 전 남은 범위

인증된 사용자→팀→DB 계정의 운영 라우팅, 비밀 관리, VPD/행·컬럼 단위 정책, 서버 측 질의 비용·실행시간 제한, 장시간 세션과 넓은 질문 평가가 남아 있다. 신원 환경변수는 로컬 운영자가 설정하는 입력이지 인증 시스템이 아니다. 모호함의 발견은 모델에 의존하며, 발견 후 중단만 서버가 강제한다. audit에는 질문·SQL·결과가 저장되므로 실제 데이터 도입 시 접근권한과 보존 정책을 정해야 한다.

## 발표 파일 재생성

`docs/presentations/codex_oracle.cjs`는 편집 가능한 도형·텍스트·표로 16장과 발표자 노트를 생성한다. `pptxgenjs` 설치 후 저장소 루트에서 `node docs/presentations/codex_oracle.cjs`를 실행한다. 최종 파일은 `docs/presentations/Oracle_Lang2SQL_Codex_v2.pptx`다.

## 공식 참고

- [Codex MCP 구성](https://learn.chatgpt.com/docs/extend/mcp.md?surface=cli)
- [Codex 비대화형 실행](https://learn.chatgpt.com/docs/non-interactive-mode.md)
- [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)

## 코드 검증 기록

2026-11-14 기준 Python 3.10과 3.13에서 각각 pytest 188개 통과. 실제 stdio handshake, context 선행, 읽기 도구 목록, SQL 안전 검사, 중단 뒤 동시/추가 호출 차단, 내부 LLM 미호출, Codex 완료·실패·timeout을 검사한다. Black 및 변경 파일 Ruff(E4/E7/E9/F) 통과. mypy는 기반 Oracle 브랜치와 동일한 기존 오류 19개이며 신규 오류는 없다.

첫 실행 5/5 이후 3회 반복 중 로컬 Python 환경을 교체해 MCP 시작 실패가 발생했다. 이 중단된 실행은 성공률에 합산하지 않는다. 이후 Python 테스트 환경을 별도로 분리하여 최종 재검증 **5/5 통과**. [최종 기록](../bench/codex/results/verified.json)을 기준으로 발표한다.
