# Oracle 커뮤니티 미팅: 실행 가능한 데모

## 참석자가 가져갈 구조

Codex → Lang2SQL MCP → Oracle 업무 조회 계정. 같은 MCP가 별도의 Oracle 메타데이터 계정으로 정의·기억·후보·감사를 저장한다. Lang2SQL 자체 MCP이며 SQLcl MCP/Select AI/Oracle Agent Memory를 호출하는 구현은 아니다. 이 기능들의 소개는 후속 Oracle 세션과 연결한다.

기존 DB 데이터는 있어도 AI를 위한 정의는 비어 있을 수 있다. 질문 → 미정의 시 재질문 → 사용자 발언에서 Codex가 후보 추출 → 근거와 함께 보관 → 운영자 승인 → Oracle 뷰 계약 확인 → 새 대화에서 재사용하는 순서로 시연한다. 모델 가중치를 학습하는 과정은 아니다.

## 설치와 계정

1. [기본 Oracle fixture](../oracle_view/README.md)의 새 컨테이너와 두 업무 계정을 준비한다. 기존 운영 DB에 fixture를 실행하지 않는다.
2. `metadata.sql`을 PDB 관리자로 실행한다. 인수는 새 난수 비밀번호다. `LANG2SQL_METADATA_URL`에 그 계정 URL을 넣는다. 예시는 `.env.example`이며 실제 파일 권한은 `chmod 600`으로 제한한다.
3. `uv sync --extra oracle --extra mcp --extra tools` 후 아래 초기화를 한 번 수행한다.

```sh
uv run --env-file .env lang2sql-tools --org-id demo --team-id sales --user-id speaker \
  --metadata-url-env LANG2SQL_METADATA_URL --initialize-metadata list
```

`L2S_METADATA`와 `L2S_AUDIT` 테이블을 생성한다. 이후 MCP에서는 `--initialize-metadata`를 빼며, 관리자는 초기화 계정의 CREATE TABLE 권한을 회수할 수 있다. 업무 조회 계정에는 메타데이터 쓰기 권한을 주지 않는다. 설정된 Oracle 연결 실패 시 SQLite로 조용히 전환하지 않는다.

저장 형식은 **Oracle CLOB에 직렬화한 JSON**이다. JSON-B/JSONB라고 소개하지 않는다. `--metadata-url-env`가 없으면 기존 `--store` SQLite 경로를 유지한다. 기존 SQLite 자료의 자동 이전은 하지 않으며, 새 저장소는 빈 상태로 시작한다.

## Codex 연결

Codex MCP 설정의 서버 command는 `uv`, args는 다음과 같이 구성한다. 프로젝트와 env 파일은 실제 절대 경로로 바꾼다.

```toml
[mcp_servers.lang2sql-meeting]
command = "uv"
args = ["run", "--project", "/absolute/Lang2SQL", "--env-file", "/absolute/demo.env", "lang2sql-mcp", "--org-id", "demo", "--team-id", "sales", "--user-id", "speaker", "--metadata-url-env", "LANG2SQL_METADATA_URL"]
```

MCP 연결만으로 모든 클라이언트가 대화 내용을 자동 저장하지는 않는다. 이 데모는 `codex/AGENTS.md`의 대화 지침도 함께 사용한다. Codex에서 해당 폴더를 열거나 `codex -C /absolute/Lang2SQL/bench/oracle_meeting/codex`로 시작한다. 일반 업무 정의 발언에도 후보 추출을 수행하도록 명시한 클라이언트 설정이며, 모든 Codex 세션에 전역 적용하지 않는다.

대화 예시: “우리 팀 순매출은 확정 주문에서 할인·반품을 차감한 합계야. 다음 대화에서도 쓸 수 있게 남겨줘.” MCP 지침에 따라 Codex는 `context_candidates`에 용어·정의·원문 근거를 제안한다. 후보 ID와 내용을 확인한 운영자가 동일 조직/팀/사용자 범위로 실행한다.

```sh
uv run --env-file .env lang2sql-tools --org-id demo --team-id sales --user-id speaker \
  --metadata-url-env LANG2SQL_METADATA_URL decide-context CANDIDATE_ID --approve
# 잘못된 후보는 동일 명령에서 --reject
```

후보 승인 명령은 MCP 도구로 공개하지 않는다. 직접 명시한 정의를 등록하는 기존 `term_custom` 도구와 구별한다. 에이전트의 사용자 발언 인용이 맞는지는 운영자가 검토한다. 목록에는 처리 상태도 남는다. 프로세스가 승인 중 중단되어 `processing`으로 남으면 감사와 정의를 확인한 후 새 후보로 처리한다.

승인 후 [뷰 계약 등록](../oracle_view/README.md)의 `bind-oracle-view`에도 같은 `--metadata-url-env`를 사용한다. 새 Codex 대화에서 “2025년 10월 순매출”을 물으면 정의·기간·SQL·결과를 함께 확인한다.

## 팀별 결과와 PL/SQL

기본 fixture 이후 `fixture.sql`을 같은 전용 PDB에서 실행한다. 마케팅의 활성고객은 해당 월 활동 고객, 재무는 결제 중인 고객이다. 10월 예상값은 각각 **3 / 2**다. 두 팀이 같은 질문을 해도 각 팀의 승인된 뷰를 조회한다.

패키지 함수는 검토한 뷰 안에서 호출한다. `V_PACKAGE_NET`의 10월 순매출은 **118**이다. 임의 PL/SQL 호출/프로시저 실행은 공개하지 않는다. 함수·패키지와 종속 소스 변경은 재승인 사유다. PACKAGE BODY 소스/상태가 계정에 보이지 않으면 연결을 차단한다. EXECUTE 권한만으로 본문이 보이지 않을 수 있다. 이 사례의 로컬 검증은 패키지 소유자 계정을 별도로 사용했으며, 그 계정을 읽기 전용이라고 주장하지 않는다.

```sh
uv run --env-file .env python bench/oracle_meeting/rehearse.py \
  --package-url-env LANG2SQL_PACKAGE_DB_URL --output /tmp/meeting-evidence.json
```

`LANG2SQL_PACKAGE_DB_URL`은 패키지 검증용 가시성이 있는 별도 계정이며 패키지 사례에만 사용한다. 일반 두 팀 사례는 기존 조회 계정을 유지한다. 패키지 함수의 무부작용성/업무 의미는 운영자 검토 대상이다.

## 장기 대화 평가

```sh
uv run --env-file .env python bench/oracle_view/long_context.py \
  --store /absolute/registered-demo.db --turns 100 --output /tmp/long-context-result
```

기존 SQLite 데모 저장소에 순매출 뷰가 등록된 상태에서 실행한다. 실제 Codex 대화를 resume하며 9·10·11월의 세 질문 패턴을 반복하며 다음 달 지시어가 포함된 100턴을 순서대로 보낸다. 정답은 프롬프트에 넣지 않는다. 1·10·50·100턴의 명시적 질문을 새 세션에서도 비교한다. 도구가 반환한 값과 기간을 채점하며 원문 응답·도구 호출·사용량 이벤트·소요 시간을 보존한다. 요청 모델, Codex 버전, 질문셋 해시를 기록한다.

이것은 합성 데이터 한 시나리오의 재현 평가다. #267 전체의 독립 질문 100개 통계 평가나 #268의 모든 후속 질문 유형을 대신하지 않는다. 100턴 전체 통과도 정확도 향상이나 자동 학습을 의미하지 않는다. 기준/집계 변경, 결과 참조 등의 새로운 질문셋을 추가할 수 있으며 운영 데이터의 일반화 평가는 별도다.

## 데이터 전달·권한

Codex에는 도구 설명, 사용자 발언, 해당 요청에서 반환한 정의·스키마·조회 결과가 전달된다. 비밀번호는 환경 파일로만 주입한다. MCP identity는 운영자 설정이며 최종 사용자 인증을 구현한 것은 아니다. 조직별 서버/계정 분리와 DB 권한을 먼저 적용한다. 이 데모는 합성 데이터만 사용한다.

## 실제 Codex 시연 재현

`rehearse.py`가 만든 JSON을 입력으로 실제 모델을 호출한다. 자동 후보 추출은 위 Codex 작업 폴더 지침을 임시 평가 폴더에 동일하게 적용한다. 자동 승인은 제품 기능이 아니라, 고정된 합성 정의와 정확히 일치하는 후보만 승인하는 평가 코드다.

```sh
uv run --env-file .env python bench/oracle_meeting/codex_rehearse.py \
  --rehearsal /tmp/meeting-evidence.json --output /tmp/codex-meeting-result
```

패키지 변경 차단을 확인하려면 전용 fixture 컨테이너와 패키지 본문 가시성 계정을 사용해 `package_fault.py --container NAME --output NEW.json`을 실행한다. 테스트는 MEETING_RULES 본문을 변경한 뒤 finally에서 복원한다.

Oracle 메타데이터로 같은 평가를 실행하려면 `long_context.py --metadata-url-env LANG2SQL_METADATA_URL --org-id ORG --team-id TEAM --turns 100 --output NEW_DIR`을 사용한다. 해당 팀에 순매출과 기본 fixture 뷰 계약이 먼저 등록되어 있어야 한다. 완료 후 `summarize_context.py --run NEW_DIR --expected-turns 100 --output NEW_REPORT.json`으로 시작·종료일, 실제 값, 동일 세션 유지와 새 세션 비교를 독립적으로 재검사한다.
