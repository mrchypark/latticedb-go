# Zig 원본과 Go 비교 — 2026-09-22

비교 대상은 `jeffhajewski/latticedb` main **827891e2c6fd55d13aa8f8284a7c7043f68b60fd**와 Go **v0.8.0(e5faec3) + 이번 변경**이다. 다른 포크의 수치를 원본 수치로 사용하지 않았다. 전체 Cypher 호환율이나 데이터베이스 전체의 속도 배수를 뜻하지 않는다.

## 문법 차이와 이번 구현

| 영역 | Zig 원본에서 확인 | Go 변경 후 |
|---|---|---|
| 내장 함수 | 20개 (`expression.zig:evaluateFunction`) | 같은 20개 이미 지원 |
| 집계 | count/sum/avg/min/max/collect | 같은 6개 이미 지원 |
| WITH | 중간 투영·그룹·정렬·필터 | 지원, 필터 왼쪽 표현식 등 범위 제한 있음 |
| 리스트 리터럴 | 중첩 표현식 리스트 | **이번에 구현**: UNWIND, IN, 함수 인수, 속성, RETURN/WITH |
| 산술식 | +, -, *, /, %, ^ | 미지원 |
| 집계 DISTINCT | 파서 및 aggregate 계획 지원 | 미지원 |
| MERGE | 노드·관계, ON CREATE/ON MATCH SET | 미지원 |
| 가변 길이 경로 | 파서 quantifier와 VarExpand 실행자 | 미지원 |
| 독립 RETURN, 일반 표현식 투영 | 원본 파서·계획 경로 있음 | 독립 RETURN/일반 상수 투영 제한 |
| OPTIONAL MATCH / UNION | 이번 원본 파서에서 지원 근거 확인 못함 | 미지원; 원본 대비 결손으로 계산하지 않음 |

리스트는 기존 값 표현식·바인딩 검증·공개 결과 변환을 재사용한다. 리스트 슬롯을 할당 전에 계상하고 요소별 작업량/취소를 확인한다. 중첩 리스트, 빈 리스트, 파라미터, 함수, 노드 포함, WITH, 속성 저장, 잘못된 구문·바인딩 및 메모리 경계 검증을 추가했다. 영속 포맷 변경은 없다.

원본 소스: [표현식](https://github.com/jeffhajewski/latticedb/blob/827891e2c6fd55d13aa8f8284a7c7043f68b60fd/src/query/expression.zig), [파서](https://github.com/jeffhajewski/latticedb/blob/827891e2c6fd55d13aa8f8284a7c7043f68b60fd/src/query/parser.zig), [실행 계획](https://github.com/jeffhajewski/latticedb/blob/827891e2c6fd55d13aa8f8284a7c7043f68b60fd/src/query/planner.zig).

## 벡터 검색 실측

Apple M3 / macOS arm64, Go 1.27.1 (`GOMAXPROCS=2`), Zig 0.16.0 ReleaseFast. 10,000개, 128차원 정규화 군집 벡터, seed=42, M=16/M0=32, efConstruction=200, efSearch=64, K=10. 양쪽 10회 warmup → 100개 쿼리 latency → 10개 쿼리 recall 순서. 각 구현을 순차 3회 실행한 중앙값이다. 소스의 RNG/분포 생성 알고리즘은 대응하지만 언어별 부동소수점 수학 연산까지 bit-identical임을 검증한 것은 아니다.

| 지표 | Go | Zig 원본 | 관찰 |
|---|---:|---:|---|
| 평균 검색 지연 | 165.780 µs | 563.710 µs | Go 약 3.40배 낮음 |
| P99 | 577.625 µs | 1,165.880 µs | Go 약 2.02배 낮음 |
| 인덱스 생성 | 10.745초 | 29.668초 | Go 약 2.76배 낮음 |
| recall@10 | 99% | 99% | 각 실행 100개 정답 슬롯 기준 |
| Go 정확검색 fallback | 0/100회 | 별도 계측 없음 | Go 3회 모두 0 |

Go 평균 지연 범위 154.153–227.879 µs, Zig 560.140–587.920 µs. P99는 실행당 100표본의 순서통계라 안정적인 서비스 SLO 추정치가 아니다. 빌드와 테스트를 측정 구간에 의도적으로 겹치지 않았지만 머신의 다른 활동·CPU 주파수를 통제하지 않았다.

**측정 경계가 다르다.** Go는 인메모리 GraphState/HNSW 경로, Zig는 페이지·버퍼풀·벡터 저장소를 포함한 HNSW 경로다. 그래서 위 수치를 언어 자체의 우열, 동등한 내구성 쓰기 성능, 전체 DB 성능으로 일반화하면 안 된다. Go `index-build-ms`는 이미 만들어진 노드/벡터에서 인덱스 연결을 구성하고, Zig Insert는 저장소 작업도 포함한다. 양쪽 메모리 지표도 서로 달라 비교하지 않았다. 100K Zig 측정은 이번에 수행하지 않았으며 이전 Go 100K 결과만으로 배수를 만들지 않았다.

Go 원시 로그: `go-10k.txt`, Zig: `zig-10k-{1,2,3}.txt`. `*-initial-excluded.txt`는 진단용 이전 실행으로 이 표에서 제외했다. Go 측정의 recall 선행으로 인한 추가 warmup을 제거했고 정확검색 fallback 계측을 추가했다. 기존 쿼리 벤치마크의 DB Close/checkpoint가 타이머에 포함되는 문제도 `b.Cleanup`으로 수정했다.

## FTS 비교 한계와 우선순위

Zig는 BM25(문서 빈도·길이 정규화), Go는 현재 토큰 빈도 기반 점수다. 정확 점수는 기존 엔진 호환 계약 밖이다. 따라서 Go의 이전 scan/postings **97.7배**는 Go 내부 최적화 비교이며 Zig 대비 수치가 아니다. 동일 데이터에서 top-K 품질/recall을 먼저 맞추지 않은 FTS 지연 배수는 제시하지 않는다. 원본의 fuzzy는 바이트 기준, Go는 rune 기준이라 비ASCII 검색 의미도 다르다.

원본 소스: [BM25](https://github.com/jeffhajewski/latticedb/blob/827891e2c6fd55d13aa8f8284a7c7043f68b60fd/src/fts/scorer.zig), [fuzzy](https://github.com/jeffhajewski/latticedb/blob/827891e2c6fd55d13aa8f8284a7c7043f68b60fd/src/fts/fuzzy.zig).

남은 구현은 (1) 산술식 및 일반 표현식 투영, (2) 집계 DISTINCT, (3) MERGE 원자성/롤백, (4) 가변 길이 경로와 작업량 제한 순으로 나누는 것이 적절하다. BM25는 기존 순위를 바꾸므로 별도 API/호환 정책과 품질 데이터가 필요하다. 이번 변경으로 이 항목들이 구현됐다고 주장하지 않는다.

CI 기준도 원본 저장소로 수정했다. CI는 **원본의 최신 안정 태그**를 해시로 고정하므로 이 보고서의 main SHA와 다를 수 있다(확인 당시 v0.15.0=9800159e22e200f2b6888c7c6be1810adb695506). CI 로그의 SHA·버전·원시 결과를 기준으로 해석해야 한다.
