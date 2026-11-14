# Oracle × Lang2SQL 데모

> Codex 기반 새 발표와 실행은 [Codex 전용 안내](codex_oracle_demo.md)를 사용한다. 이 문서는 공용 Oracle fixture 준비와 기존 내부 에이전트 평가를 설명한다.

같은 질문에 부서별 정의를 적용하고, 정의가 없으면 되묻고, Oracle 조회 권한을 지키는 CLI 데모다. `bench/oracle_demo.py`는 기존 `ContextConcierge → agent_loop → RunSQL → SqlAlchemyExplorer` 경로를 실행한다. 별도 SQL 생성 엔진이나 Oracle 전용 에이전트는 없다.

## 보여줄 장면과 합격 조건

| 장면 | 입력 | 합격 조건 |
|---|---|---|
| 영업 매출 | 2026년 10월 매출 얼마야? | 주문 총액 2,000,000원 |
| 재무 매출 | 동일 질문 | 할인·반품 차감 후 1,700,000원 |
| 정의 없는 용어 | 2026년 10월 충성고객 매출 | `ask_user`, 업무 SQL 실행 0회 |
| 인사 급여 | 부서별 급여 평균 | sales 3,000,000원, finance 4,000,000원 |
| 영업 급여 | 동일 급여 질문 | 실제 DB 차단, 급여 결과 없음, 접근 불가 안내 |

데이터는 전부 합성이다. 10월 주문 3건 외에 9월·11월 주문을 넣어 기간 조건 누락을 검출한다. `--output` JSON에는 접속 사용자, 선택할 부서 정의, 실제 SQL과 DB 결과, 답변, 도구 호출을 남긴다. 급여 차단은 동일 SQL의 독립적인 권한 검사에서 `ORA-00942` 또는 `ORA-01031`이 나오는지 확인하고, 별도로 에이전트의 거절 응답을 검사한다. 모델이 스키마 탐색에서 권한 부족을 알아내면 업무 SQL 없이 거절할 수도 있다.

## 로컬 재현

Docker와 uv가 필요하다. **새로 만든 폐기 가능한 데모 DB에서만 seed를 실행한다.** 생성할 사용자가 이미 있으면 seed는 중단하며 기존 사용자를 삭제하지 않는다. Oracle DDL은 자동 커밋되므로 중간 실패 후에는 새 컨테이너로 다시 준비한다.

```bash
uv sync --extra oracle
export ORACLE_PASSWORD="$(openssl rand -hex 16)"
export ORACLE_DEMO_PORT=11521
export LANG2SQL_DB_URL="oracle+oracledb://system:${ORACLE_PASSWORD}@127.0.0.1:${ORACLE_DEMO_PORT}/?service_name=FREEPDB1"

docker compose -p lang2sql-oracle-demo -f bench/oracle/compose.yaml up -d --wait
uv run python bench/oracle_demo.py seed
uv run python bench/oracle_demo.py verify --output /tmp/oracle-scripted.json
```

기본 verify는 **실제 Oracle + 명시적인 ScriptedLLM**으로 연결·안전장치·도구 동작을 검증한다. 이 결과는 LLM 정확도가 아니다.

실제 모델로 같은 사례를 반복하려면 `OPENAI_API_KEY`를 환경에 설정한 뒤 실행한다. 키를 명령행 인수나 결과 파일에 넣지 않는다.

```bash
export LANG2SQL_LLM_MODEL=gpt-4.1-mini
uv run python bench/oracle_demo.py verify --live --repeats 3 --output /tmp/oracle-live.json
```

`--live`는 키가 없으면 실패하며 FakeLLM으로 대체하지 않는다. 모델 호출은 실제 API 요금이 발생한다. 하나라도 판정에 실패하면 종료 코드 1을 반환한다. 결과 JSON의 답변과 SQL도 발표 전에 읽어 확인한다.

```bash
# DB를 남겨두고 중지 / 다시 시작
docker compose -p lang2sql-oracle-demo -f bench/oracle/compose.yaml stop
docker compose -p lang2sql-oracle-demo -f bench/oracle/compose.yaml start
```

## 권한과 범위

- `l2s_demo_owner`가 주문·급여 테이블을 소유한다. 영업·재무·인사 계정은 CREATE SESSION 및 필요한 테이블의 SELECT만 가진다. 에이전트에는 SYSTEM 계정을 전달하지 않는다.
- 이번 CLI 데모는 고정된 persona와 DB 계정을 조립점에서 연결한다. 실제 Discord의 guild별 DSN 설정을 사용자별 인증·계정 라우팅으로 확장한 것은 아니다. 운영 도입 시 인증된 사용자와 DB 신원의 연결이 별도로 필요하다.
- 각 persona는 서로 다른 채널/세션을 사용한다. 결과 캐시는 사용하지 않는다. 기존 전체 서비스의 세션·캐시 격리를 보증하는 테스트는 아니다.
- 모호한 표현의 발견은 모델과 프롬프트에 의존한다. 모델이 `ask_user`를 요청한 뒤의 중단은 루프가 강제하며 같은 도구 배치에 섞인 SQL도 취소한다. 모든 자연어의 모호함을 결정적으로 판별하는 규칙 엔진은 아니다.
- 기존 다중 정의 주입 정책은 유지한다. 부서별 정의는 SQL 생성의 맥락이며 DB 접근 권한을 대신하지 않는다.
- Oracle의 ORA-00942/ORA-01031은 adapter에서 DataAccessError로 변환한다. RunSQL은 실패를 audit에 남기고 도구 턴을 종료한다. 루프는 같은 배치의 나머지 도구도 취소하며, 모델이 다른 테이블을 탐색하거나 “테이블이 없다”고 단정하지 않도록 중립적인 안내를 직접 반환한다. 다른 SQL 오류는 기존 복구 경로를 유지한다.
- 이번 검증은 테이블 SELECT 권한이다. VPD, Oracle Graph/Vector, 장기 기억, 자동 관계 추출, 운영 보안 전체는 검증 범위 밖이다.
- 기존 TimeoutLayer의 값 설정과 DB 실행 타임아웃 강제는 별개다. 이 PR에서 전체 실행시간 제한 기능을 추가하지 않는다.

## Oracle 연결 설정

`/setup`의 Oracle Database 항목은 host(선택 포트 포함), service_name, user, password를 받는다. `uv sync --extra oracle`로 드라이버를 설치한다. `oracle://` URL도 factory에서 `oracle+oracledb://`로 정규화한다. Thin 모드로 Oracle Client 설치 없이 연결한다.

SQLAlchemy adapter가 연결을 열지 않고 dialect를 알린다. 시스템 프롬프트에는 Oracle 날짜·행 제한 문법을 넣고, RunSQL이 safety context에 dialect를 전달해 `FETCH FIRST ... ROWS ONLY`를 사용한다. 명시된 FETCH는 중복 추가하지 않으며, 하위 질의·문자열·주석 속 FETCH는 바깥 결과 제한으로 취급하지 않는다.

## 참고 자료

- [SQLAlchemy Oracle dialect와 python-oracledb 연결](https://docs.sqlalchemy.org/en/20/dialects/oracle.html)
- [Oracle 객체 권한과 역할](https://docs.oracle.com/en/database/oracle/oracle-database/21/dbseg/configuring-privilege-and-role-authorization.html)
- [Oracle Free 컨테이너 사용법](https://github.com/gvenzl/oci-oracle-free)

## 검증 기록 (2026-11-11)

Oracle Free 23.9 ARM64 컨테이너, Python 3.13, SQLAlchemy 2.0.50, python-oracledb 3.4.2에서 실행했다.

- [실제 모델 실행 기록](../bench/oracle/results/live.json): gpt-4.1-mini, 5개 사례 × 3회 = **15/15 통과**. 실제 답변도 읽어 매출·급여 수치와 거절 문구를 확인했다.
- [고정 도구 호출 실행 기록](../bench/oracle/results/scripted.json): 실제 Oracle, **5/5 통과**. 모델 추론 없이 DB/하네스 연결을 검증한다.
- [접근 오류 처리 보완 전 기록](../bench/oracle/results/before-access-fix.json): **13/15 통과**. DB 차단은 성공했으나 2회에 모델이 테이블 부재로 단정했다. 접근 오류 뒤 턴을 종료하도록 고친 후 위 15회를 다시 실행했다.
- pytest **176 passed**, 전체 Black 검사 통과, 변경 파일 Ruff(E4/E7/E9/F) 통과.
- mypy는 기존 master와 동일한 오류 19개가 남아 있다. 신규 타입 오류는 없다. CODE MAP 생성 스크립트는 이 저장소에 없다.

이는 고정된 데모 사례의 반복 검증이다. 독립 질문 15개 평가나 일반 정확도 100%라는 의미가 아니다. 발표에서는 JSON의 SQL·결과·db_user를 함께 보여준다.
