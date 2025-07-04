# Lang2SQL

<div align="center">
<a href="https://pseudo-lab.com"><img src="https://img.shields.io/badge/PseudoLab-S10-3776AB" alt="PseudoLab"/></a>
<a href="https://discord.gg/EPurkHVtp2"><img src="https://img.shields.io/badge/Discord-BF40BF" alt="Discord Community"/></a>
<a href="https://github.com/CausalInferenceLab/lang2sql/stargazers"><img src="https://img.shields.io/github/stars/CausalInferenceLab/lang2sql" alt="Stars Badge"/></a>
<a href="https://github.com/CausalInferenceLab/lang2sql/network/members"><img src="https://img.shields.io/github/forks/CausalInferenceLab/lang2sql" alt="Forks Badge"/></a>
<a href="https://github.com/CausalInferenceLab/lang2sql/pulls"><img src="https://img.shields.io/github/issues-pr/CausalInferenceLab/lang2sql" alt="Pull Requests Badge"/></a>
<a href="https://github.com/CausalInferenceLab/lang2sql/issues"><img src="https://img.shields.io/github/issues/CausalInferenceLab/lang2sql" alt="Issues Badge"/></a>
<a href="https://github.com/CausalInferenceLab/lang2sql/graphs/contributors"><img alt="GitHub contributors" src="https://img.shields.io/github/contributors/CausalInferenceLab/lang2sql?color=2b9348"></a>
<a href="https://pypi.org/project/lang2sql/"><img src="https://img.shields.io/pypi/v/lang2sql" alt="PyPI version"/></a>
<a href="https://pypi.org/project/lang2sql/"><img src="https://img.shields.io/pypi/dm/lang2sql" alt="PyPI downloads"/></a>
<a href="https://hits.sh/github.com/CausalInferenceLab/lang2sql/"><img alt="Hits" src="https://hits.sh/github.com/CausalInferenceLab/lang2sql.svg"/></a>
</div>

<p align="center">
  <strong>우리는 함께 코드와 아이디어를 나누며 더 나은 데이터 환경을 만들기 위한 오픈소스 여정을 떠납니다. 🌍💡</strong>
</p>

<p align="center">
  <em>"모두가 더 가치 있는 일에 집중할 수 있기를 바랍니다."</em>
</p>

---

## 🚀 Lang2SQL이란?

Lang2SQL은 자연어 쿼리를 최적화된 SQL 문으로 변환하는 오픈소스 도구입니다. LangGraph와 DataHub 통합으로 구축되어, 복잡한 데이터베이스 스키마에 대한 깊은 지식 없이도 데이터 사용자들이 효율적인 SQL 쿼리를 생성할 수 있도록 도와줍니다.

### 🎯 주요 기능

- **🗣️ 자연어를 SQL로 변환**: 일상 언어를 정확한 SQL 쿼리로 변환
- **📊 스마트 테이블 발견**: 의미론적 검색을 사용하여 관련 테이블을 자동으로 찾기
- **🔍 스키마 인식**: DataHub 메타데이터를 활용한 정확한 컬럼 매핑
- **🛠️ 웹 인터페이스**: 대화형 Streamlit 앱을 통한 사용
- **📈 시각화**: 생성된 SQL 쿼리 결과를 다양한 차트와 그래프로 시각화하여 데이터 인사이트를 직관적으로 파악

### 🤔 해결하는 문제

새로운 데이터팀 구성원들이 자주 직면하는 문제들:
- 🤯 "테이블이 너무 많아! 어디서부터 시작하지?"
- 🧐 "이 JOIN이 맞나요?"
- 🐌 "이 쿼리 성능이 괜찮을까요?"
- 😰 "어떻게 의미있는 인사이트를 추출하지?"

**Lang2SQL은 다음을 제공하여 이를 해결합니다:**
- ✅ 자연어 입력 → 테이블 추천
- ✅ 적절한 컬럼 조합으로 자동 SQL 생성
- ✅ 모범 사례 기반 성능 최적화

---

## 📦 설치 방법

### 빠른 설치

```bash
pip install lang2sql
```

### 소스에서 설치

```bash
git clone https://github.com/CausalInferenceLab/lang2sql.git
cd lang2sql
pip install -r requirements.txt
python setup.py install
```

---

## 🛠️ 사용법

### 명령줄 인터페이스

Streamlit 웹 인터페이스 실행:

```bash
lang2sql run-streamlit
```

사용자 정의 포트 및 DataHub 서버와 함께:

```bash
lang2sql --datahub_server http://your-datahub-server:8080 run-streamlit -p 8888
```

### 환경 설정

- 현재는 pip 패키지 설치로 프로젝트 시작이 어려운 상황입니다.
- `.env` 파일을 생성하여 설정 관리 (.env.example 참고)

---

## 🏗️ 아키텍처

Lang2SQL은 LangGraph를 사용한 다단계 접근 방식을 따릅니다:

1. **📝 자연어 처리**: 사용자 의도 파싱 및 핵심 엔티티 추출
2. **🔍 테이블 검색**: 의미론적 유사성을 사용한 관련 테이블 찾기 (Vector Search)
3. **⚙️ SQL 생성**: 최적화된 SQL 쿼리 생성
4. **🚀 쿼리 시각화**: 쿼리 결과를 시각화 합니다.

---

## 🧑‍💻 기술 스택

- **[LangGraph](https://github.com/langchain-ai/langgraph)**: LLM 워크플로우 오케스트레이션
- **[DataHub](https://datahubproject.io/)**: 메타데이터 관리 및 활용
- **[Streamlit](https://streamlit.io/)**: 대화형 웹 인터페이스

---

## 🌟 기여가 필요한 영역 (Help!)

### Containerization

- Docker를 활용하여 프로젝트를 컨테이너화하고, `pip install lang2sql` 설치 후 단일 명령어로 실행 가능하도록 개선합니다.
- CI/CD 파이프라인 구축 및 자동화된 테스트 환경 구성까지 확장할 수 있는 작업입니다.

### Agentic 아키텍처 개발

- 쿼리 생성 과정을 에이전틱하게 개선하여 더욱 지능적이고 자율적인 SQL 생성이 가능하도록 개발합니다.
- 데이터 디스커버리 기능을 강화하여 사용자가 원하는 데이터를 더 효과적으로 찾을 수 있도록 지원합니다.

### Datahub 통합 강화

- 현재 Datahub의 Glossary와 쿼리 예시를 코드로 조회하는 기능이 구현되어 있습니다.
- 이러한 메타데이터를 쿼리 생성 과정에 더욱 긴밀하게 통합하여 컨텍스트 기반의 정확한 SQL 생성을 지원하는 작업입니다.

### VectorDB 유연성 개선

- 현재는 Datahub를 통해 로컬에 FAISS VectorDB를 생성해야만 사용 가능한 구조입니다.
- 이 결합도를 낮춰서 Datahub 없이도 기존에 준비된 VectorDB만 있으면 바로 활용할 수 있도록 아키텍처를 개선하는 작업입니다.

### 출력 포맷 강화

- 현재 Streamlit을 통한 웹 인터페이스만 지원하고 있습니다.
- CLI, JSON 등 다양한 출력 포맷을 지원하여 사용자가 선호하는 환경에서 유연하게 활용할 수 있도록 개선합니다.

### 모니터링 / 로깅 강화

- 프로젝트 사용 패턴과 성능을 모니터링하고, 상세한 로깅 시스템을 구축합니다.
- 사용자 피드백 수집 및 분석 프로세스를 통해 지속적인 개선이 가능한 기반을 마련하는 작업입니다.

### 문서화 강화

- 프로젝트 기여 장벽을 낮추기 위한 포괄적인 문서화 작업입니다.
- 개발자 가이드, 튜토리얼 등을 체계적으로 정리하여 새로운 기여자들이 쉽게 참여할 수 있는 환경을 조성합니다.

---

## 🤝 기여하기

커뮤니티의 기여를 환영합니다! 여러분이 도울 수 있는 방법들:

### 🔧 개발 환경 설정

1. 저장소 포크하기
2. 포크 클론: `git clone https://github.com/YOUR_USERNAME/lang2sql.git`
3. 의존성 설치: `pip install -r requirements.txt`
4. 기능 브랜치 생성: `git checkout -b feature/amazing-feature`
5. 변경사항 커밋: `git commit -m 'Add amazing feature'`
6. 브랜치에 푸시: `git push origin feature/amazing-feature`
7. Pull Request 열기

### 🐛 이슈 신고

버그를 발견했거나 기능 요청이 있으신가요? 다음 정보와 함께 [이슈를 열어주세요](https://github.com/CausalInferenceLab/lang2sql/issues):
- 문제/기능에 대한 명확한 설명
- 재현 단계 (버그의 경우)
- 예상 동작 vs 실제 동작
- 환경 세부사항

### 📋 개발 가이드라인

- pre-commit 활성화
- 새로운 기능에 대한 테스트 작성
- 필요시 문서 업데이트
- 원자적이고 잘 설명된 커밋 유지

---

## 🎓 학습 자료

- [모두를 위한 게임 데이터 검색 시스템 / if(kakaoAI)2024](https://www.youtube.com/watch?v=8-GerpWVMis&ab_channel=kakaotech)
- [AI 데이터 분석가 '물어보새' 등장 – 1부. RAG와 Text-To-SQL 활용](https://techblog.woowahan.com/18144/)
- [테디노트 LangGraph](https://wikidocs.net/233785)
- [DataHub 문서](https://datahubproject.io/)
- [Vanna.ai](https://github.com/vanna-ai/vanna)

---

## 🏆 Our Team

| Role | Name | Skills | Interests |
|------|------|--------|-----------|
| **Project Manager** | 이동욱 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM, Open Source, Causal Inference |
| **AI Engineer** | 문찬국 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM, Agentic RAG, Open Source |
| **Data Analytics Engineer** | 박경태 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM-based Engineering |
| **AI Engineer** | 손봉균 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM, RAG, AI Planning |
| **Data Scientist** | 안재일 | ![Python](https://img.shields.io/badge/Python-Intermediate-FF6C37) | LLM, Data Analysis, RAG |
| **ML Engineer** | 이호민 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | Multi-Agent Systems |
| **AI Engineer** | 최세영 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM, RAG, Multi-Agent |
| **Full-Stack Developer** | 황윤진 | ![NextJs](https://img.shields.io/badge/NextJs-Expert-3776AB) ![React](https://img.shields.io/badge/React-Expert-3776AB) | LLM Orchestration |

---

## 🚀 배포 및 릴리스

### 수동 빌드

```bash
python setup.py sdist bdist_wheel
twine upload dist/*
```

### 자동 릴리스

`v*` 형식의 태그를 푸시하여 자동 PyPI 배포 트리거:

```bash
git tag v1.0.0
git push origin v1.0.0
```

**참고**: GitHub Secrets에 `PYPI_API_TOKEN` 설정 필요.

---

## 🙏 감사의 말

Lang2SQL은 **가짜연구소의 인과추론팀**에서 개발중인 프로젝트입니다.

---

## 📄 라이선스

- This project is licensed under the [MIT License](https://opensource.org/licenses/MIT).

---

## 🌍 가짜연구소 소개

[가짜연구소](https://pseudo-lab.com/)는 머신러닝과 AI 기술 발전에 중점을 둔 비영리 조직입니다. **공유, 동기부여, 그리고 협업의 기쁨**이라는 핵심 가치를 바탕으로 영향력 있는 오픈소스 프로젝트를 만들어갑니다.

전 세계 5,000명 이상의 연구자들과 함께, 우리는 AI 지식의 민주화와 열린 협업을 통한 혁신 촉진에 전념하고 있습니다.

**우리 커뮤니티에 참여하세요:**
- 💬 [Discord](https://discord.gg/EPurkHVtp2)

---

## 🎯 기여자들

<a href="https://github.com/CausalInferenceLab/lang2sql/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=CausalInferenceLab/lang2sql" />
</a>

---

<div align="center">
  <p><strong>⭐ 이 저장소가 도움이 되셨다면 스타를 눌러주세요!</strong></p>
  <p><em>"우리는 함께 코드와 아이디어를 나누며 더 나은 데이터 환경을 만들기 위한 오픈소스 여정을 떠납니다. 🌍💡"</em></p>
</div>