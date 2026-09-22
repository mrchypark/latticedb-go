# LatticeDB Go v0.8.0 문법 범위 및 검색 벤치마크

측정일: 2026-09-22. 엔진 기준: `e5faec33e9a8ca2c07f05ba32fc3192bec64235c` (`v0.8.0`). Apple M3 / 24 GiB / macOS arm64 / Go 1.27.1 / GOMAXPROCS=2. 단일 클라이언트가 순차 실행하며, 동시 요청 처리량 측정은 아니다.

## 핵심 결과

- 문법은 WITH·그룹 집계 6종·내장 함수 20종을 지원하지만 전체 openCypher 구현은 아니다. 자체 매트릭스 203개 사례를 모두 통과했다.
- 1만 문서의 동일 FTS 질의에서 속성 인덱스로 7.297 ms → 0.075 ms, 약 97.7배 낮은 지연을 측정했다. 반환 10행 조건이다.
- 10만 노드 WITH 그룹 집계 후 상위 10행은 50.67 ms이나, 회당 68.36 MiB·1,200,620회 할당했다. 전체 스캔/집계의 할당량은 후속 최적화 후보다.
- 큰 어휘의 fuzzy 검색은 길이 차이로 쉽게 제외되는 조건에서도 10만 토큰에 39.78 ms다. 작은 어휘의 마이크로초 결과만으로 실서비스 지연을 판단하면 안 된다.
- 10만 128차원 벡터: exact 약 6.37 ms, ANN 옵션의 100질의 평균 중앙값 약 0.396 ms, 표본 recall@10 100.0%. 인덱스 구축은 약 158초이며 정확도/지연의 표본 한계는 아래에 명시했다.

## 측정 방법

- 대부분 `-benchtime=200ms -count=3`; 표는 세 실행의 중앙값과 최소–최대다. 작은 표본의 기술 통계이며 신뢰구간은 아니다.
- DB/데이터/인덱스 준비는 검색 타이머 밖, 쿼리 계획 캐시는 워밍업한다. 캐시가 차가운 첫 요청, 디스크에서 읽는 비용, 영속 쓰기 비용은 측정하지 않았다.
- 기본 QueryOptions를 사용했다. 직접 FTS 확장 벤치마크는 기존 하네스대로 MaxWork를 최대로 올린다. B/op는 누적 Go 할당량이며 최대 상주 메모리나 논리 MaxBytes가 아니다.
- 엔진 구현은 수정하지 않았다. 새 WITH/집계/함수 및 동일 데이터의 exact 벡터 측정 하네스를 추가했다. 기존 top-K/다중 홉 하네스의 `defer db.Close()`는 `b.Cleanup`으로 바꿨다. Go의 StopTimer 이후 cleanup이 실행되므로 종료/체크포인트 비용이 검색 결과에 섞이지 않는다. 수정 전 query 결과는 `query-initial-excluded.txt`에 보관하고 아래 집계에서 제외했다.
- ANN은 기존 하네스의 `-benchtime=1x -count=3`. 표의 시간은 단일 ns/op가 아니라 별도 100개 질의의 평균(mean-ns) 중앙값이다. p99도 100개 표본 기반이며 서비스 SLA 수치가 아니다. ANN은 호출 옵션 이름이며 자동 exact fallback 횟수는 이번 하네스에서 별도로 수집하지 않았다. ANN의 B/op는 단일 반복 측정이다.
- 비교는 같은 버전 내 접근 방식 비교다. Zig 원본 또는 Ladybug를 실행한 결과가 아니며, 제품 간 성능 우위를 의미하지 않는다.

## 문법 범위

문법 매트릭스: 지원 사례 **133개**, 의도적 거절 사례 **70개** 모두 통과. 선택한 문법·함수·집계·WITH 테스트의 pass 이벤트는 총 **451개**(상위 테스트 포함)다. 문법 계약과 문서, 파서 digest도 일치한다. 이는 자체 회귀 사례의 통과율이며 전체 openCypher 지원률이 아니다.

| 영역 | 지원 | 주요 제한 |
|---|---|---|
| 진입점·패턴 | MATCH, CREATE, UNWIND; 다중 라벨·속성 맵·쉼표 패턴·고정 길이 다중 홉 | 독립 RETURN 진입점, OPTIONAL MATCH, MERGE, 가변 길이 경로 미지원 |
| 관계 | 정방향·역방향·무방향 매칭, 바인딩·타입·속성 조건 | 관계 생성은 기존 바인딩 사이의 방향성 관계 |
| 조건 | =, <>, <, <=, >, >=, IN, 문자열 조건, IS [NOT] NULL, AND/OR/NOT·괄호 | WHERE 비교 왼쪽은 주로 속성 접근; id(binding)=expr 별도 지원 |
| 출력·정렬 | AS, DISTINCT, 다중 ORDER BY, SKIP, LIMIT | RETURN *, 일반 산술식, 일반 RETURN 함수 계산열의 직접 ORDER BY 미지원; WITH alias 정렬은 지원 |
| 파이프라인 | WITH 투영·스코프·WHERE·DISTINCT·집계·정렬·페이지·연속 파트 | 명시 alias/일반 바인딩 이름만 다음 파트로 전달; WITH로 종료 불가 |
| 집계 | count, sum, avg, min, max, collect; 비집계 항목으로 그룹화 | count(DISTINCT ...) 미지원; sum/avg는 float, collect는 NULL 포함 |
| 내장 함수 | id, labels, type, properties, size, head, last, tail, range, split, replace, substring, trim, toLower, toUpper, toInteger, toFloat, toString, abs, coalesce | 20개 허용 함수만 지원; 임의/UDF 호출 미지원 |
| 변경 | 노드 CREATE, 관계 CREATE, SET/REMOVE, 맵 교체·병합, DELETE/DETACH DELETE | MATCH 파트의 terminal 조합 제한; 반환 LIMIT은 쓰기 수를 제한하지 않음 |
| 검색 | `<=>` 벡터 순위, `@@` 속성 전문 검색, 명시적 ANN 옵션 | 검색 조건은 AND 결합만 가능; OR/NOT 아래 금지 |
| 값·구문 | 스칼라·맵·매개변수, 매개변수의 list/bytes/vector, backtick 식별자 | list literal, UNION, 여러 문장·주석 미지원; 키워드 대문자, 연산자 주변 공백 필요 |

정확한 계약: [EBNF](../../../internal/engine/testdata/query_grammar.ebnf), [문법 문서](../../engine_conformance.md), [지원/거절 매트릭스](../../../internal/engine/query_grammar_test.go).

## 쿼리·검색 측정

프로퍼티 동일성은 1만 노드의 인덱스 조회(직접 API 및 public DB.Query)다. FTS 후보 비교는 1만 문서, rareterm 포함률 1%, LIMIT 10으로 인덱스 유무만 바꾼다. 벡터 쿼리는 전체 1만 노드 중 GroupA 5천 개, 16차원, K=10, ef=64다. 이 벡터 쿼리 하네스는 결과 수만 확인하므로 아래 별도 128차원 recall 값을 이 결과에 대입하면 안 된다.

| 워크로드 | 시간 중앙값 | 반복 범위 | 할당량/회 | 할당 횟수/회 |
|---|---:|---:|---:|---:|
| `PropertyEquality10K/indexed` | 0.0004 ms | 0.0004–0.0004 ms | 0.09 KiB | 4 |
| `PropertyEquality10K/query_indexed` | 0.0022 ms | 0.0022–0.0029 ms | 2.09 KiB | 31 |
| `PropertyIndexCommonValue10K/limit_1` | 0.0005 ms | 0.0005–0.0005 ms | 0.09 KiB | 4 |
| `PropertyIndexCommonValue10K/full_then_limit_1` | 0.3215 ms | 0.3018–0.4199 ms | 80.09 KiB | 4 |
| `PropertyIndexCommonValue10K/unique_limit_1` | 0.0004 ms | 0.0004–0.0005 ms | 0.09 KiB | 4 |
| `PropertyIndexCommonValue10K/unique_full_then_limit_1` | 0.0005 ms | 0.0005–0.0005 ms | 0.09 KiB | 4 |
| `QueryFTSCandidates10K/scan` | 7.2967 ms | 7.1854–7.9758 ms | 6887.11 KiB | 100092 |
| `QueryFTSCandidates10K/postings` | 0.0747 ms | 0.0736–0.0805 ms | 60.94 KiB | 1074 |
| `QueryVectorCandidates10K/exact` | 2.2947 ms | 2.1311–2.9736 ms | 1734.31 KiB | 5083 |
| `QueryVectorCandidates10K/ann` | 0.0333 ms | 0.0323–0.0341 ms | 7.85 KiB | 72 |
| `QueryMultiHopSlots` | 0.0500 ms | 0.0473–0.0522 ms | 81.28 KiB | 616 |
| `QueryMultiHopSlots100K` | 64.1306 ms | 64.0716–65.3585 ms | 94837.36 KiB | 699805 |
| `QueryOrderLimitTopK/top_k_10/10000` | 4.7870 ms | 4.7303–5.1193 ms | 2353.93 KiB | 10059 |
| `QueryOrderLimitTopK/full_sort_all_rows_reference/10000` | 9.6535 ms | 9.1269–9.8125 ms | 7733.06 KiB | 30069 |
| `QueryOrderLimitTopK/top_k_10/100000` | 49.5271 ms | 46.6453–80.3892 ms | 27172.79 KiB | 100070 |
| `QueryOrderLimitTopK/full_sort_all_rows_reference/100000` | 99.3109 ms | 93.3893–102.3372 ms | 86067.29 KiB | 300101 |

Top-K와 전체 정렬은 반환량도 다르다(10개 대 전체 노드). 따라서 차이는 정렬 알고리즘뿐 아니라 결과 생성·할당 비용을 포함한다. 다중 홉은 NEXT 체인의 두 홉 결과를 전부 반환한다(100 노드→98행, 10만 노드→99,998행).

## 새 문법의 실행 비용

1만/10만 Item 노드. value=0..N−1, bucket=i%100, name="Sample". count 및 sum/avg는 1행, group_100은 100행, WITH 집계 후 상위 10개는 10행, collect는 N개 원소를 담은 1행이다. nested_functions는 `sum(size(toLower(n.name)))`를 실행한다. 워밍업에서 행 수 및 기대 집계 값도 검증했다.

| 워크로드 | 시간 중앙값 | 반복 범위 | 할당량/회 | 할당 횟수/회 |
|---|---:|---:|---:|---:|
| `QueryLanguage/nodes_10000/count` | 2.5239 ms | 2.2166–2.6832 ms | 4148.88 KiB | 10050 |
| `QueryLanguage/nodes_10000/sum_avg` | 5.4576 ms | 2.7802–6.0069 ms | 4461.66 KiB | 20057 |
| `QueryLanguage/nodes_10000/group_100` | 4.4887 ms | 4.4015–4.7048 ms | 5533.31 KiB | 110667 |
| `QueryLanguage/nodes_10000/with_group_top10` | 4.8637 ms | 4.8382–4.8692 ms | 6297.46 KiB | 120595 |
| `QueryLanguage/nodes_10000/collect` | 2.4519 ms | 2.4496–2.4620 ms | 5271.95 KiB | 20075 |
| `QueryLanguage/nodes_10000/nested_functions` | 2.8836 ms | 2.8816–2.9123 ms | 5008.43 KiB | 60055 |
| `QueryLanguage/nodes_100000/count` | 18.2893 ms | 18.2467–23.3044 ms | 48864.12 KiB | 100074 |
| `QueryLanguage/nodes_100000/sum_avg` | 26.6253 ms | 26.4881–29.3138 ms | 51989.58 KiB | 200083 |
| `QueryLanguage/nodes_100000/group_100` | 48.3974 ms | 47.7399–56.3439 ms | 62201.80 KiB | 1100692 |
| `QueryLanguage/nodes_100000/with_group_top10` | 50.6679 ms | 50.3329–50.8483 ms | 69997.18 KiB | 1200620 |
| `QueryLanguage/nodes_100000/collect` | 27.1148 ms | 26.7712–31.0089 ms | 62271.76 KiB | 200109 |
| `QueryLanguage/nodes_100000/nested_functions` | 31.3884 ms | 31.3577–32.2140 ms | 57458.22 KiB | 600080 |

## 직접 전문 검색: 문서 수와 어휘 크기

아래 scaling 하네스는 대부분 "common token", 한 문서만 "common rare token"이다. 어휘가 3개인 의도적으로 단순한 데이터다. rare는 1건, common은 전체 문서가 매칭하고 기본 Limit 10을 반환한다. multi_rare는 "rare absent"이며 이 엔진의 점수 의미에 따른 검색이다. fuzzy_rare는 "rarf", 편집거리 1이다. 희귀어/fuzzy 숫자를 큰 실제 어휘의 성능으로 일반화하면 안 된다.

| 워크로드 | 시간 중앙값 | 반복 범위 | 할당량/회 | 할당 횟수/회 |
|---|---:|---:|---:|---:|
| `FTSSearchScaling/records_1000/rare` | 0.0007 ms | 0.0006–0.0010 ms | 0.48 KiB | 11 |
| `FTSSearchScaling/records_1000/common` | 0.0326 ms | 0.0321–0.0552 ms | 0.57 KiB | 11 |
| `FTSSearchScaling/records_1000/multi_rare` | 0.0008 ms | 0.0008–0.0008 ms | 0.64 KiB | 16 |
| `FTSSearchScaling/records_1000/fuzzy_rare` | 0.0027 ms | 0.0027–0.0037 ms | 1.09 KiB | 24 |
| `FTSSearchScaling/records_10000/rare` | 0.0006 ms | 0.0006–0.0007 ms | 0.48 KiB | 11 |
| `FTSSearchScaling/records_10000/common` | 0.2950 ms | 0.2873–0.3161 ms | 0.57 KiB | 11 |
| `FTSSearchScaling/records_10000/multi_rare` | 0.0010 ms | 0.0008–0.0012 ms | 0.64 KiB | 16 |
| `FTSSearchScaling/records_10000/fuzzy_rare` | 0.0016 ms | 0.0015–0.0017 ms | 1.09 KiB | 24 |
| `FTSSearchScaling/records_100000/rare` | 0.0006 ms | 0.0006–0.0006 ms | 0.48 KiB | 11 |
| `FTSSearchScaling/records_100000/common` | 4.6723 ms | 4.4735–5.7520 ms | 0.58 KiB | 11 |
| `FTSSearchScaling/records_100000/multi_rare` | 0.0008 ms | 0.0008–0.0008 ms | 0.64 KiB | 16 |
| `FTSSearchScaling/records_100000/fuzzy_rare` | 0.0016 ms | 0.0015–0.0022 ms | 1.09 KiB | 24 |

별도의 vocabulary-pruning 측정은 서로 다른 긴 토큰 1만/10만 개와 한 글자 질의 "x", 편집거리 1, 결과 0개다. 큰 어휘를 스캔하되 길이 차이로 빠르게 제외하는 비용이며, 같은 길이의 근접 단어를 다수 비교하는 최악 조건은 아니다.

| 워크로드 | 시간 중앙값 | 반복 범위 | 할당량/회 | 할당 횟수/회 |
|---|---:|---:|---:|---:|
| `FTSFuzzyVocabularyPruning/tokens_10000` | 1.3402 ms | 1.3017–1.4172 ms | 0.37 KiB | 9 |
| `FTSFuzzyVocabularyPruning/tokens_100000` | 39.7840 ms | 34.7424–49.1599 ms | 0.61 KiB | 11 |

## 128차원 벡터: 동일 데이터 exact/ANN

seed=42, 정규화된 군집 벡터, L2, K=10. ANN M=16/M0=32, construction ef=200, search ef=64. 100개 질의를 생성하고 10개로 워밍업한다. Recall은 첫 10개 질의의 exact top-10과 비교(총 100개 이웃)하므로 전체 분포의 품질 보장은 아니다.

| 벡터 수 | exact ns/op 중앙값 | ANN 100질의 평균 중앙값 | ANN p99 중앙값 | recall@10 범위 | 구축 시간 중앙값 |
|---|---:|---:|---:|---:|---:|
| 1K | 0.0566 ms | 0.0756 ms | 0.5055 ms | 100.0–100.0% | 0.57 s |
| 10K | 0.6821 ms | 0.1391 ms | 0.3924 ms | 99.0–99.0% | 8.69 s |
| 100K | 6.3698 ms | 0.3963 ms | 1.0825 ms | 100.0–100.0% | 157.58 s |


exact는 순환 질의의 Go benchmark 평균, ANN은 고정 100개 질의의 별도 샘플 평균이다. 같은 데이터지만 샘플 수·타이밍 방식은 다르므로 엄밀한 paired latency 비교는 아니다. 전체 반복값은 CSV/원본을 참고한다.

## 재현 및 한계

이 디렉터리는 v0.8.0 당시 측정의 역사적 스냅샷이다. 아래 명령은 보관된 원시 로그를 다시 집계하며 현재 checkout을 측정하지 않는다. 환경·문법 로그·성능 로그의 SHA-256이 보관 manifest와 다르면 출력 파일을 변경하기 전에 거부한다. 새 측정은 별도 디렉터리와 해당 후보의 환경·하네스 설명으로 기록해야 한다. 현재 원본 비교용 실행 명령은 [별도 하네스](../upstream-2026-09-22/run.sh)를 참고한다.

```sh
python3 docs/benchmarks/v0.8.0-2026-09-22/summarize.py
```

[중앙값·범위 CSV](summary.csv), [쿼리 원본](query.txt), [FTS 원본](fts.txt), [exact 벡터 원본](vector-exact.txt), [ANN 벡터 원본](vector-ann.txt), [큰 어휘 원본](fuzzy-vocabulary.txt), [환경](environment.txt), [문법 테스트](grammar-tests.jsonl), [추가 하네스](../../../internal/engine/query_language_benchmark_test.go).

합성 데이터·단일 장비·warm read 결과이며 CPU 격리나 주파수 고정은 하지 않았다. 읽기/쓰기 혼합, 다중 사용자, 한국어 실제 문서, 콜드 오픈, 대형 임베딩(768/1536차원), 100만 건 이상은 이번 측정 범위에 포함하지 않는다. 변동이 큰 항목은 원본의 범위를 함께 봐야 한다. 제품 성능 변경은 하지 않았고, 이 보고서의 엔진·환경·문법·계측 설명은 보관된 측정 시점에 한정된다.
