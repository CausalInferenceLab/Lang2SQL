# Oracle 미팅 설명과 구현 상태

회의의 제품명 음성 전사와 JSON-B 표현을 구현 사실로 취급하지 않는다.

| 약속 | 현재 구현/근거 | 남은 작업 |
|---|---|---|
| Codex MCP → Oracle | PR #282, Lang2SQL 자체 MCP | #282 리뷰/병합 |
| 미정의 용어 재질문 | resolve_business_terms + 실행 receipt | 모델이 용어를 누락하는 경우 일반적 보장 없음 |
| Oracle 뷰로 계산 | 운영자가 승인한 뷰 계약, 기간·필터 적용 | 함수·패키지 #266 |
| 맥락 지속 저장 | MCP remember를 파일 --store SQLite에 영속화; 조직/팀/사용자 격리 | Oracle 저장 어댑터 |
| 대화에서 자동 지식 축적 | 명시적 remember/term_custom 호출 | 후보 추출·승인 워크플로 |
| 100턴 뒤 강화 | 100개 노트 저장·재시작 회귀 검증만 제공 | 실제 모델 100턴 정확도·비용 #267 |
| 다음 질문 재사용 | 저장 노트 recall_memory, 정의 resolve_business_terms | 지시어/조건 변경 모델 평가 #268 |
| 팀별 의미 | semantic federation, 기억도 팀 격리 | 서로 다른 팀의 SQL 결과 비교 시나리오 확장 |
| Semantic Layer를 해당 Oracle DB에 JSON-B 저장 | 현재 사실 아님: SQLite metadata + Oracle 업무 데이터 | Oracle 저장 포맷·권한·마이그레이션 구현 |

## 발표에서 사용할 정확한 표현

“사용자가 확인한 정의와 명시적으로 저장한 기억을 재사용합니다. 현재 MCP의 메타데이터 저장소는 SQLite이며 업무 데이터는 Oracle에서 조회합니다.”

“100개 기억이 재시작 후 남는 것을 테스트했습니다”는 “100턴 대화 정확도가 유지되거나 향상된다”를 뜻하지 않습니다. 모든 노트를 반환하는 InjectAllRecall은 대규모 검색 최적화가 아닙니다. 일반 노트는 SQL 실행을 허용하는 승인된 업무 정의가 아닙니다.

## 재현

`uv run --extra mcp pytest -q tests/test_persistent_memory.py tests/test_mcp.py`

실제 MCP subprocess 종료/재시작 및 팀 변경을 검증한다. 파일 --store는 같은 경로를 재사용해야 하며 :memory:는 의도적으로 휘발성이다. 조직·팀·사용자 ID는 운영자 설정이므로 인증 수단이 아니며 저장 파일의 접근 제어가 필요하다. Oracle MCP 검증과 모델 평가 결과는 이 테스트로 대체하지 않는다.
