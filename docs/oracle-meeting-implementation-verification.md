# 구현 원칙 검증

재사용: MemoryService, PersistentMemoryStore, InjectAllRecall, SemanticFederationTool/FedEntry, ResolveBusinessTerms/DefinitionCheckedSQL, bind_view/oracle_metric, ToolRegistry 및 기존 Codex 실행 도구 설정. Oracle용 DB 입출력 차이는 어댑터에 두고 도구 흐름은 공유한다. #292 대비 런타임(src) 순증은 431줄이다.

| 항목 | 판정 | 근거 |
|---|---|---|
| 재사용 | O | 정의·기억·승인된 SQL 생성 경로를 재사용, CLI/MCP 옵션과 Codex 설정 조립 공유 |
| 원인 대응 | O | Oracle 저장 부재, 후보 승인 부재, 재시작·팀 격리·장기 대화 증거 부족에 대응 |
| 로깅 | O | MCP call_id, 후보 ID/상태, SQL/정의 감사 기록; 실제 실행 원문 보존 |
| 오류 처리 | O | 실패는 호출 결과/CLI 종료 코드로 노출, Oracle 설정 실패 시 SQLite 폴백 없음 |
| 설정 주입 | O | 프런트엔드 공통 입력부에서 env를 읽고 조립부에 주입 |
| 주석 이슈번호 | O | 변경 주석은 제약과 이유를 설명하며 이슈 번호에 의존하지 않음 |
| 검증 | O | Python 3.10/3.13 실DB 포함 217개, Black, 변경 코드 Ruff; CODE MAP 도구는 저장소에 없음 |
| 승인 범위 | O | 사용자 요청에 따라 이슈·PR 작성, 머지·릴리스는 하지 않음 |
| 실호출 연결 | O | 등록 MCP → 공유 런타임 → Oracle KV/정의 확인 → 승인 뷰 SQL을 실제 실행 |
| 동기/비동기 | O | MCP 공용 이벤트 루프는 worker dispatch 사용; memory/audit 네트워크 I/O는 to_thread; 업무 SQL은 기존 adapter 경로 재사용 |
| 하드코딩·배포 | O | URL은 이름을 지정한 env에서 주입, 평가 timeout/턴 수 CLI 옵션, 전용 합성 fixture 상수; .env.example/초기화 SQL/계정·Codex 설정 문서 제공 |

여러 저장 엔진을 지원하기 위해 MetadataStorePort를 추가했다. Oracle KV의 동시성 충돌은 CAS 결과로 전달하고, DB 오류를 숨기지 않는다. 일반 기억과 미승인 후보를 승인된 업무 정의로 취급하지 않는다.
