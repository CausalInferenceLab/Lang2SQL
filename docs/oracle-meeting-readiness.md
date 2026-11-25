# Oracle 사전 미팅 요구사항 점검

상위 추적: #291, 잔여 구현: #293. 기존 #267/#268 전체 연구 범위와 이번 발표 시나리오의 검증 범위를 구분한다.

| 회의 요구사항 | 구현 | 검증 |
|---|---|---|
| Codex MCP → Oracle | 기존 자체 MCP + Oracle adapter | 실제 Codex 조회 |
| 미정의 용어는 추측하지 않고 재질문 | resolve_business_terms + receipt guard | 미정의 팀 SQL 미실행 |
| 대화에서 업무 맥락 추출 | Codex가 context_candidates에 정의·원문 근거 제안 | 실제 Codex 후보 생성 |
| 확인한 맥락 축적 | 운영자 decide-context 승인/거절 → 기존 federation 저장 | 미승인 후보는 조회 기준으로 사용되지 않음 |
| 다음 대화 재사용 | Oracle/SQLite에 저장, 새 MCP 프로세스에서 읽기 | 실제 새 Codex 세션에서 승인 정의 조회 |
| Semantic Layer를 해당 DB에 저장 | --metadata-url-env로 Oracle KV·audit·기억·후보 저장 | Oracle 실DB 한글·긴 JSON·CAS·재시작·감사 |
| 팀별 다른 의미와 SQL | 동일 활성고객 용어를 서로 다른 Oracle 뷰에 연결 | 마케팅 3명/재무 2명, SQL·정의 근거 기록 |
| 기존 업무 함수 재사용 | 운영자가 검토한 뷰에서 함수·패키지 호출 | 패키지 순매출 118, 본문 변경 시 차단 |
| 10~100턴 이후 맥락 유지 | 실제 Codex resume 평가 실행기 | 결과는 bench/oracle_meeting 결과 문서에 기록 |
| 청중 환경 재현 | 설치·계정·MCP 설정·후보 승인·fixture·명령 | bench/oracle_meeting/README.md |
| 외부 모델 전달 범위 | 정의·스키마·도구 결과 전달을 명시 | 동일 README의 계정/데이터 경계 |
| Oracle 발표 비중 | DB 연결, 뷰 계약, 팀별 SQL, Oracle 메타데이터, 패키지 재사용 | 20분 중 이 흐름에 8분 이상 배정 가능 |

## 발표 표현

“사용자가 확인한 정의를 Oracle에 구조화해 저장하고 새 대화에서 재사용합니다. 추출은 Codex가 수행하며, 후보는 운영자가 승인합니다.”

Oracle 저장은 CLOB에 직렬화한 JSON이며 JSON-B라고 부르지 않는다. 자동 가중치 학습/시간에 따른 정확도 향상을 주장하지 않는다. 함수는 승인된 뷰를 통해 사용하며 임의 PL/SQL 실행 도구가 아니다. 패키지 본문 가시성이 없는 계정은 연결을 거부한다.

승인되지 않은 후보, 일반 remember 노트, 승인된 업무 정의는 서로 다르다. MCP identity는 운영자 설정이며 사용자 인증을 대체하지 않는다. 후보 승인 도구를 MCP에 공개하지 않지만 기존 term_custom으로 명시적 정의를 등록하는 경로는 유지한다.

## 행사 운영 항목

참석자 목록 공유, 루마 승인, 도착 시간, 타 발표자의 자료는 개발 요구사항이 아니며 이번 PR이 대신 처리하지 않는다. Oracle 제품 기능을 우리 구현이라고 소개하지 않는다.
