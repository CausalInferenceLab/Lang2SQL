# V1 측정: 현실의 지저분함에 견디는가?

> **2026-06-02**. *"현실의 지저분함에 견딘다"* 라는 헤드라인을 *근거가 있는 주장*으로 바꾸려고 한 측정.

## 1. 무엇을 측정했나

같은 자연어 질문 8개 / 같은 ground truth 를 두 종류의 DuckDB 스키마와 4가지 조건의 시스템 상태에서 돌렸습니다. 두 모델 비교:
- **`gpt-4.1-mini`** (V1 plan 가정 모델)
- **`mlx-community/Qwen3-14B-4bit`** (MLX 로컬 양자화)

### 스키마
| 종류 | 컬럼명 | description | enum 값 |
|---|---|---|---|
| **Clean** | `users.id`, `orders.amount`, `orders.status`, `subscriptions.ended_at` | (없음) | `'paid' / 'cancelled'` |
| **Dirty** | `usr.u_id`, `ord_tx.amt`, `ord_tx.st`, `sb_mst.canc_dt` | (없음, 약어) | `'P' / 'Paid' / 'PAID' / 'paid' / '결제완료'` / `'C' / 'cancelled' / '취소'` |

Dirty 는 *실제 production 누적 cruft* 시뮬레이션 — 컬럼명 약어, description 부재, enum 값이 표기/언어/대소문자 카오스, 일부 컬럼은 의미 모호 (`canc_dt` vs `e_at` 등).

### 시스템 조건
- **no help**: V1 harness 그대로
- **β prewarm**: `ContextConcierge.build_context` 가 길드 첫 호출에서 LLM에게 *컬럼 설명을 추정시켜* `ScopeResolverPort.define` 으로 시멘틱 레이어 자동 채움. 새 포트 추가 0, 기존 federation 메커니즘만 사용.
- **★④ predefine**: 사람이 `/define_metric` 으로 박았을 비즈니스 매핑 시뮬레이션 — `paid_orders_filter`, `cancelled_orders_filter`, `active_subscription` 의 SQL 조각.

## 2. 결과 매트릭스

| 조건 | gpt-4.1-mini | Qwen3-14B-4bit |
|---|---|---|
| Clean, no help | **10/10** | 4/10 |
| Dirty, no help | 5/8 | 1/8 |
| Dirty + β prewarm | 5/8 (도구 호출 **1/3**) | 1/8 |
| Dirty + prewarm + ★④ `/define_metric` | **8/8** | **3/8** |

원본 stdout: `/tmp/bench_result.txt` · `/tmp/bench_qwen_result.txt` · `/tmp/bench_dirty_gpt.txt` · `/tmp/bench_dirty_qwen.txt` · `/tmp/bench_dirty_predefine.txt`.

## 3. 무엇을 발견했나

### ① V1 아키텍처는 *플랜이 가정한 모델*(gpt-4.1-mini)에서 작동
깨끗한 DB·도움 없이 10/10. safety 게이트, federation 의 *기제(mechanism)* 자체가 동작.

### ② 지저분한 DB는 *모든 모델을 깎음*
깨끗 → 지저분으로 가면 gpt-4.1 도 10/10 → 5/8. 주범:
- **enum 값 카오스** (Q3 매출, Q4 취소). 모델이 `SELECT DISTINCT` 까지 가도 표기 변형(`P`/`Paid`/`PAID`/`paid`/`결제완료`)을 *모두* 잡지는 못함.

### ③ β prewarm = *효율 향상*, 정확도는 못 끌어올림
gpt-4.1 의 dirty 정확도는 5/8 → 5/8 로 동일하지만, **도구 호출 수 36 → 10 (3.6×)**. 즉 *컬럼 의미 추정에 드는 탐색 비용*은 prewarm 이 흡수. enum 매핑처럼 *데이터 안에 있는 사실*은 prewarm 으로 못 풀음.

Qwen 은 prewarm 받아도 1/8 → 1/8. 작은 양자화 모델은 *multi-step tool reasoning* 자체에서 막힘 (explore_schema 다음 run_sql 로 못 이어감, 답이 빈 문자열). 이건 시멘틱 레이어 보강과 *다른 차원* 의 문제.

### ④ ★④ `/define_metric` 이 *진짜* 강건성 메커니즘
사람이 enum 매핑을 박으면:
- gpt-4.1: 5/8 → **8/8** (모두 1 도구 호출로 정답).
- Qwen: 1/8 → **3/8** — 그리고 정답인 3개는 *정확히 사전 정의된 metric 이 답을 가지고 있던 질문* (Q3 paid sum / Q4 cancel / Q5 active subs).

이게 v4.1 plan §3.5 가 *원래 약속한 것*: *"같은 용어 다른 정의"의 충돌을 git-like 분기로 푼다*. 측정이 그 약속을 직접 검증.

## 4. 솔직한 한계
- **Qwen 의 빈-답 문제** (multi-step tool reasoning) 는 ★①/★④ 어느 것으로도 못 풀음. 작은 양자화 모델 지원은 별도 트랙 (모델별 prompt fallback / 자동 재시도 / fine-tuning) — V1.5+ 의 새 작업거리.
- 측정은 **합성 dirty** 데이터에서 수행. 실제 production 의 *오랜 누적 messiness* 와는 다름. BIRD / 한국 공공데이터로 확장 검증이 다음 단계.
- 측정 질문 8개는 *내가 정의*. 골든 쿼리셋 표준화는 별도 과제.

## 5. 한 줄 요약
> *"현실의 지저분함에 견디는"* 의 V1 메커니즘은 **★④ federation** 이다. 시멘틱 레이어가 *사람·문서가 박은 정의* 를 들고 있으면 모델은 그 정의를 쓴다 — 깨끗한 모델은 완벽(8/8) 으로, 작은 모델도 3배 개선. ★① prewarm 은 *효율 보조* 수단으로 자리잡음.

## 6. 재현 (직접 돌려보려면)
```bash
# 환경
uv sync --extra duckdb
export OPENAI_API_KEY=...   # 또는 .env 의 OPEN_AI_KEY 매핑

# 깨끗한 DB 생성 + 깨끗한 측정
python bench/seed_clean.py    # → /tmp/lang2sql_demo.duckdb
LANG2SQL_DB_URL=duckdb:////tmp/lang2sql_demo.duckdb \
  python bench/quality_clean.py --gpt
  # (선택) mlx_lm.server --model mlx-community/Qwen3-14B-4bit 띄운 뒤
  python bench/quality_clean.py --qwen

# 지저분한 DB 측정
python bench/seed_dirty.py    # → /tmp/lang2sql_dirty.duckdb
python bench/dirty.py --gpt --qwen --prewarm both
python bench/dirty.py --gpt --qwen --prewarm on --predefine
```
(현재는 `/tmp/bench_*.py` 에 ad-hoc 스크립트로 존재. 정식 bench/ 통합은 후속 PR.)
