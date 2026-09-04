---
source: https://chatgpt.com/c/6a98de20-b734-83e9-b746-2a9c80c0b235
reviewed_main: b81152b9b74295cc435e5c9ed4f76900f7985d1f
captured_at: 2026-09-04T08:47:11+09:00
---
# `latticedb-go` 메인 브랜치 코드 리뷰

## 1. 검토 기준

검토 시점의 `main` HEAD는 **`b81152b9b74295cc435e5c9ed4f76900f7985d1f`**, 커밋 메시지는 `query: scope bounded filters by pattern (#102)`입니다. 이 SHA의 `test`와 `benchmark` GitHub Actions 실행은 모두 성공했습니다.

README나 설계 문서는 아키텍처 판단의 근거로 사용하지 않았습니다. 루트 공개 API, `embedding`, `internal/search`, `internal/store`, `internal/engine`, `internal/exporter`, 플랫폼별 파일을 포함한 **프로덕션 런타임 구현 파일 전체를 파일 단위로 읽었습니다**. 테스트·벤치마크·conformance 코드는 모든 줄을 동일한 밀도로 해설하기보다, 구현이 실제로 보장하는 성질과 검증 사각지대를 중심으로 대조했습니다.

성능 수치는 별도의 로컬 재측정값이 아니라, 위 SHA의 성공한 CI 벤치마크 로그와 산출물을 검토한 결과입니다. 공용 러너 수치이므로 절대적인 SLA보다는 병목의 상대적 크기와 할당 패턴을 보는 근거로 해석해야 합니다.  

---

# 2. 총평

이 프로젝트의 본질은 일반적인 디스크 페이지 기반 그래프 데이터베이스가 아닙니다.

> **전체 상태를 메모리에 유지하면서, 불변 스냅샷과 copy-on-write로 트랜잭션을 구현하고, WAL과 체크포인트로 이를 내구성 있게 보존하는 단일 프로세스 임베디드 그래프 저장소**입니다.

그 위에 다음 기능이 결합되어 있습니다.

* 제한된 Cypher 스타일 쿼리 실행기
* 노드·엣지 속성 동등성 인덱스
* HNSW 벡터 검색
* 역색인 기반 전문 검색
* changefeed 및 사용자 스트림
* 원자적 내보내기와 백업
* 외부 임베딩 공급자 편의 계층

이 구조는 **읽기 위주, 단일 프로세스, 단일 writer, 중간 규모의 메모리 상주 그래프**에는 매우 잘 맞습니다. 반면 다음 워크로드에는 아직 적합하지 않습니다.

* 지속적인 고빈도 쓰기
* 다수 writer의 동시 처리
* 수십만 개 이상의 벡터를 사용하면서 재시작이 잦은 서비스
* supernode를 자주 삭제하는 그래프
* 복잡한 Cypher 분석 쿼리
* 데이터 전체가 메모리에 들어오지 않는 규모
* 긴 읽기 트랜잭션이나 스냅샷이 흔한 서비스
* 다중 프로세스 read replica나 shared read-only 접근

가장 성숙한 부분은 **WAL·복구·체크포인트·파일 게시 안전성**입니다. 가장 큰 약점은 **파생 인덱스 수명주기**, **검색과 쿼리 플래너의 통합**, **대량 변경 경로**, **실제 RSS에 대한 통제**입니다.

정적 검토에서 정상 경로의 명백한 데이터 손상 결함은 찾지 못했습니다. 다만 이는 형식 검증이나 crash model proof를 의미하지 않습니다. 현재 가장 현실적인 위험은 데이터 손상보다는 **시작 불가, 쓰기 거부, 유지보수 기아, 꼬리 지연 폭증, GC 압력**입니다.

### 주관적 성숙도 평가

| 영역           |     평가 | 판단                         |
| ------------ | -----: | -------------------------- |
| WAL·복구·체크포인트 | 8.5/10 | 프로젝트에서 가장 강한 부분            |
| 트랜잭션·스냅샷 격리  |   8/10 | 단순하고 일관된 COW 설계            |
| 직접 CRUD·점 조회 |   8/10 | 규모에 거의 독립적인 변경 비용          |
| 직접 벡터 검색     | 7.5/10 | warm 상태는 우수, 수명주기는 취약      |
| 직접 FTS       |   7/10 | 희소 용어는 매우 빠름               |
| 쿼리 플래너       |   5/10 | 규칙 기반이고 물리 연산자 범위가 좁음      |
| 쿼리 언어 커버리지   | 4.5/10 | 의도적으로 제한된 Cypher 부분집합      |
| 메모리 예측 가능성   |   5/10 | 논리 예산은 있으나 실제 heap/RSS와 차이 |
| 쓰기 확장성       | 5.5/10 | 단일 writer와 유지보수 경로가 한계     |
| 운영 가능성       |   6/10 | 진단·메트릭·온라인 유지보수가 부족        |
| 테스트 문화       | 8.5/10 | 장애·호환성·회귀 테스트가 상당히 좋음      |

---

# 3. 설계의 핵심

## 3.1 전체 데이터베이스가 하나의 불변 세대다

`GraphState`는 노드, 엣지, FTS 레코드, 양방향 adjacency, 라벨 및 타입 postings, 속성 인덱스, 벡터 인덱스, 벡터 tombstone, 스트림을 한 세대에 묶습니다. 쓰기 트랜잭션은 이를 얕게 fork하고, 실제 변경이 발생한 페이지·샤드·postings만 복제합니다.

이 선택은 좋습니다. 한 레코드의 변경 비용이 전체 DB 크기에 비례하지 않습니다. 실제 CI에서도 단일 레코드 직접 커밋은 1천 노드와 10만 노드 상태에서 모두 약 0.23~0.24ms로 거의 같았습니다. 즉 기본 쓰기 비용은 대체로 데이터베이스 총크기가 아니라 **변경 집합 크기**에 비례합니다.

## 3.2 reader는 그래프 포인터를 고정하고 writer는 새 세대를 만든다

읽기 트랜잭션은 현재 `GraphState` 포인터를 잡고 락 없이 불변 상태를 읽습니다. 쓰기 트랜잭션은 전역 `writeMu`를 획득한 뒤 COW 세대를 생성하고, WAL 기록이 성공한 후 짧은 `db.mu` 구간에서 새 그래프 포인터와 commit ID를 게시합니다.

복잡한 MVCC 버전 체인이나 row lock 없이도 snapshot isolation과 read-your-writes를 제공한다는 점에서 매우 적절한 임베디드 설계입니다.

## 3.3 canonical state와 derived state를 구분한다

노드·엣지·FTS 원문·스트림·인덱스 정의 등은 canonical persisted state에 포함됩니다. 라벨 postings, 엣지 타입 postings, adjacency, FTS token postings, 속성 postings, HNSW는 원본으로부터 재구성 가능한 파생 상태로 취급됩니다. 특히 HNSW는 의도적으로 지속하지 않습니다.

이 구분은 손상 복구와 형식 호환성에는 유리하지만, 현재는 **모든 Open 비용과 유지보수 비용을 파생 인덱스 재구축으로 지불**하고 있습니다.

## 3.4 쿼리 계층은 작은 파서와 iterator 실행기의 결합이다

쿼리 구현은 별도 파서 라이브러리 없이 문자열 스캔, 바인딩 검증, 작은 logical plan, iterator 체인, 결과 렌더링을 한 파일에서 처리합니다. 단순 읽기에서는 iterator를 유지하고 `LIMIT`를 조기에 적용하지만, 정렬·중복 제거·집계·변경 쿼리·벡터·FTS가 들어가면 중간 row를 물질화합니다.

---

# 4. 가장 우선해서 해결해야 할 문제

아래의 P0/P1/P2는 보안 심각도가 아니라 **제품화 관점의 수정 우선순위**입니다.

## P0-1. HNSW를 사용할 때 `Open`이 전체 벡터 인덱스를 동기 재구축한다

HNSW는 저장되지 않으며, `VectorIndexHNSWSynchronous`로 열면 `OpenContext`가 반환되기 전에 `rebuildVectorIndexBudget`을 실행합니다. 수동 `RebuildVectorIndexContext`도 전체 writer lock을 잡은 상태에서 재구축을 수행합니다.

최신 CI의 clustered 128D 벤치마크에서는 다음 수치가 나왔습니다.

* 1K 인덱스 빌드: 약 0.92초
* 10K 인덱스 빌드: 약 16초
* 100K 인덱스 빌드: 약 **239.7초**
* 100K 검색: 평균 약 0.50ms, p99 약 0.74ms, recall@10 99%

이 벤치마크가 Open 전체를 직접 측정한 것은 아니지만, Open이 같은 동기 재구축 함수를 호출하므로 cold-start 위험의 크기를 보여줍니다. 검색 자체는 빠른데 인덱스를 사용할 준비가 되기까지 수 분이 걸릴 수 있습니다. 같은 SHA의 `ColdOpen` 약 132ms 수치는 HNSW 동기 빌드를 포함하지 않은 별도 fixture이므로 혼동하면 안 됩니다.

더 심각한 점은 수동 rebuild가 `writeMu`를 빌드 전 과정 동안 유지한다는 것입니다. 100K 벡터에서 위와 비슷한 시간이 걸리면 그동안 모든 쓰기는 기다리는 것이 아니라 `ErrWriteTxActive`로 실패할 수 있습니다.

### 권고 설계

HNSW를 다음과 같은 독립 세대로 지속해야 합니다.

* 키: database ID, canonical commit ID, 인덱스 형식 버전, 차원, 거리 함수, 대상 property
* 내용: 벡터 ID 배열, 연속 벡터 블록, 레벨, 이웃 offset 배열
* 검증: checksum과 canonical commit ID
* 게시: 임시 파일 생성 후 원자적 rename
* Open: 유효한 세대가 있으면 즉시 로드
* 세대가 없거나 오래된 경우: exact 검색으로 먼저 서비스하고 백그라운드 구축
* 구축 중 발생한 벡터 변경: commit ID 기반 delta로 catch-up
* 최종 swap: 짧은 writer lock 또는 CAS

핵심은 **재구축을 없애는 것보다 서비스 시작과 canonical write availability에서 분리하는 것**입니다.

---

## P0-2. 백그라운드 체크포인트가 지속적인 쓰기에서 기아 상태에 빠질 수 있다

백그라운드 체크포인트는 불변 세대를 캡처하고 writer lock 밖에서 파일을 준비합니다. 이는 좋은 설계입니다. 하지만 게시 직전에 `db.graph == generation.graph`와 commit ID가 그대로인지 확인하고, 그 사이 커밋 하나라도 발생했다면 준비한 전체 체크포인트를 버리고 처음부터 다시 시도합니다.

따라서 체크포인트 직렬화 시간보다 커밋 간격이 짧은 지속적 쓰기에서는 다음 루프가 가능합니다.

1. commit C의 그래프를 캡처한다.
2. 전체 snapshot을 디스크에 쓴다.
3. 그 사이 C+1이 커밋된다.
4. prepared snapshot을 폐기한다.
5. C+1 기준으로 다시 전체 snapshot을 쓴다.
6. 다시 새 커밋 때문에 폐기한다.

결과는 단순 지연이 아니라 다음과 같습니다.

* WAL이 임계값을 넘어서 계속 증가
* 매번 전체 snapshot을 재인코딩하는 CPU·디스크 쓰기 증폭
* 재시작 복구 시간 증가
* 임시 파일 생성과 삭제 반복
* 결국 명시적 `Checkpoint`나 쓰기 중단이 있어야만 진전할 가능성

### 권고 설계

정석적인 해결은 **WAL rotation을 통한 fuzzy checkpoint**입니다.

1. 짧은 writer lock에서 commit C의 그래프를 고정하고 현재 WAL을 rotate한다.
2. 새 커밋은 새 WAL segment에 기록한다.
3. lock 밖에서 C의 snapshot을 만든다.
4. snapshot C를 게시한다.
5. C 이후 WAL segment는 그대로 보존한다.
6. snapshot에 포함된 이전 WAL만 제거한다.

이렇게 하면 최신 세대와 일치할 필요가 없으며, 체크포인트가 반드시 진전합니다.

같은 기아 문제가 adjacency compactor에도 있습니다. compactor는 여러 `Step`에 걸쳐 작업하지만 그 사이 그래프 포인터나 commit ID가 바뀌면 작업을 폐기하고 다시 enqueue합니다. 고차수 노드가 있는 지속 쓰기 환경에서는 adjacency tombstone 정리가 영원히 완료되지 않을 수 있습니다. per-node adjacency version과 결과 merge 또는 짧은 writer 구간에서의 batch compaction이 필요합니다.

---

## P0-3. 쿼리 언어의 벡터·FTS 연산자가 실제 검색 인덱스를 사용하지 않는다

직접 API인 `VectorSearch`와 `FTSSearch`는 각각 HNSW와 FTS postings를 사용합니다. 반면 쿼리의 `<=>`와 `@@`는 물리적인 index scan으로 계획되지 않습니다.

실행기는 해당 WHERE 절을 만나면 앞선 MATCH iterator의 모든 row를 수집한 뒤, 각 row를 순회하면서 벡터 거리를 정확 계산하거나 텍스트를 토큰화·점수화합니다. 이후 점수 순으로 전체 row를 정렬합니다.

따라서 다음 두 API는 기능은 비슷해 보여도 성능 모델이 완전히 다릅니다.

* 직접 `VectorSearch`: 대체로 HNSW 후보만 탐색
* `MATCH ... WHERE n.embedding <=> $q ...`: MATCH 결과 전체에 exact distance
* 직접 `FTSSearch`: token postings에서 후보 조회
* `MATCH ... WHERE n.text @@ $q ...`: MATCH row를 물질화하고 row별 scoring

그래프 조인 결과가 커질수록 CPU, 중간 메모리, 첫 행 지연이 모두 커집니다. 쿼리 사용자 입장에서는 검색 API와 쿼리 언어가 같은 성능 특성을 가진다고 예상하기 쉬워 특히 위험합니다.

### 필요한 물리 연산자

* `VectorKNNScan(label, property, query, k, ef)`
* `FTSPostingScan(label, property/index, terms)`
* `NodePropertyEqualityScan`
* `AdjacencyExpand`
* `TopK`
* `StreamingAggregate`
* `DistinctTuple`
* `Limit`

필터가 있는 ANN은 보통 다음 전략이 필요합니다.

1. label/property predicate의 예상 선택도 계산
2. HNSW에서 `K × oversampling` 후보 생성
3. graph/property filter 적용
4. 정확 거리로 rerank
5. 부족하면 oversampling 확대 또는 exact fallback

검색 결과를 먼저 노드 ID stream으로 만든 다음 graph traversal과 결합해야 합니다. 현재처럼 MATCH 전체를 먼저 만들고 검색 점수를 계산하는 순서는 역전되어야 합니다.

---

## P0-4. 벡터 검색 exact fallback의 목표 결과 수가 전체 노드 수를 기준으로 계산된다

직접 벡터 검색은 결과 capacity를 다음 논리로 설정합니다.

* `min(K, 전체 노드 수)`

하지만 올바른 기준은 다음입니다.

* `min(K, 실제 벡터가 있는 live 노드 수)`

현재 HNSW가 반환한 결과 수가 이 capacity보다 작으면 ANN 결과를 모두 버리고 전체 노드를 exact scan합니다.

예를 들어 다음 상태를 생각할 수 있습니다.

* 전체 노드: 1,000,000
* 벡터가 있는 노드: 50
* 요청 K: 100

HNSW가 50개의 live vector를 모두 정확히 찾더라도 capacity는 100입니다. 결과 길이가 100이 아니므로 이미 완전한 결과를 버리고 1,000,000개 노드를 다시 훑습니다. 더 찾을 벡터는 없습니다.

CI에서도 10K의 일반 matched ANN 검색은 약 36µs인데, ANN fallback benchmark는 약 0.886ms로 약 25배 느렸습니다.

### 수정

`GraphState`에 다음 값을 O(1)로 유지해야 합니다.

* live vector count
* indexed live count
* tombstone count
* 각 named vector index별 live count

ANN 결과가 `min(K, liveVectorCount)`만큼 있으면 즉시 성공해야 합니다. fallback은 실제 index underfill, tombstone filtering, disconnected graph, stale generation 등 명확한 사유로만 발생해야 하며 사유별 메트릭도 분리해야 합니다.

---

## P0-5. 파생 HNSW 유지보수 상태가 canonical 쓰기를 거부하게 만든다

벡터 변경 후 tombstone과 mutation debt가 임계값을 초과하면 `applyVectorIndexChanges`는 `ErrVectorIndexMaintenanceRequired`를 반환합니다. 즉 그래프 자체로는 유효한 노드 속성 변경이라도 파생 인덱스의 상태 때문에 커밋이 실패합니다.

이는 강한 ANN 일관성을 유지하려는 선택이지만 운영 가용성에는 좋지 않습니다.

현재 흐름은 다음과 같이 악화될 수 있습니다.

1. 벡터 churn이 임계값을 넘는다.
2. 정상적인 쓰기가 maintenance required로 실패한다.
3. 운영자가 rebuild를 호출한다.
4. rebuild가 전역 writer lock을 장시간 점유한다.
5. 그동안 다른 쓰기는 `ErrWriteTxActive`로 실패한다.

### 권고

기본 정책은 다음이어야 합니다.

* canonical write는 성공
* HNSW 세대를 stale로 표시
* ANN 검색은 exact fallback 또는 직전 세대+delta overlay 사용
* rebuild는 백그라운드 실행
* 새 세대가 완성되면 원자적으로 swap

ANN과 canonical graph를 반드시 동기화해야 하는 사용자를 위해서만 별도의 strict mode를 제공하는 편이 낫습니다.

---

## P1-1. 고차수 노드 삭제가 CPU와 메모리의 심각한 절벽이다

100K degree 노드의 삭제 벤치마크는 대략 다음과 같습니다.

* 약 233ms
* 약 543.9MB allocated bytes/op
* 약 1.20M allocations/op

이는 평균적으로 엣지 하나당 약 5.4KB의 일시 할당과 약 12회의 할당입니다. 결과 heap이 544MB 늘어난다는 뜻은 아니지만, 한 번의 삭제가 GC에 그만큼의 allocation pressure를 전달한다는 뜻입니다.

원인은 고차수 삭제가 각 엣지마다 다음 작업을 반복하기 때문입니다.

* edge PagedMap 변경
* 양쪽 adjacency tombstone
* edge type postings 변경
* property index postings 변경
* delta map 갱신
* changefeed 항목 구성
* 여러 COW 샤드와 map 복제

chunked adjacency 자체의 append 설계는 매우 좋습니다. 100K adjacency append가 약 1.5µs, 4.5KB 할당에 머무는 반면 flat slice 방식은 약 84µs, 803KB가 필요했습니다. 문제는 구조 자체가 아니라 **대량 삭제를 단건 변경의 반복으로 구현한 경로**입니다.

### 필요한 변경

* 내부 `deleteEdgesBatch`
* self-loop 중복 제거
* 변경 edge ID를 정렬하고 샤드별로 한 번만 COW
* postings별 batch remove
* adjacency tombstone range 또는 bitmap
* WAL delta에서 개별 JSON object 대신 batch/range encoding
* changefeed의 대량 삭제 요약 또는 chunking
* writer-local arena와 재사용 가능한 scratch

이 경로는 micro-optimization이 아니라 알고리즘과 mutation representation을 바꿔야 합니다.

---

## P1-2. 쿼리 플래너가 관계형·그래프 조인 순서를 충분히 최적화하지 않는다

현재 cardinality 추정은 라벨 수, 엣지 타입 수, 속성 동등성 postings 크기를 사용합니다. 다중 equality index는 postings intersection을 수행하고, 최근 변경에서는 LIMIT가 있는 residual filter를 postings 방문 중에 적용합니다. 이 부분은 방향이 좋습니다.

그러나 제한이 큽니다.

* 연결된 경로의 순서는 사실상 쿼리 소스 순서를 유지
* 독립 component 재정렬도 `ORDER BY`가 있고, mutation·SKIP·LIMIT가 없는 좁은 경우에만 수행
* degree 통계 없음
* property 분포 histogram 없음
* label-property 상관관계 없음
* range selectivity 없음
* join algorithm 선택 없음
* traversal 방향을 비용에 따라 뒤집지 않음
* search operator를 시작점으로 선택하지 않음

이는 결정적인 자연 순서를 유지하는 데에는 유리하지만, 사용자가 broad pattern을 먼저 쓴 쿼리에서는 중간 row 폭발을 유발할 수 있습니다.

장기적으로는 다음 계층 분리가 필요합니다.

1. lexer/parser
2. semantic AST
3. logical algebra
4. cardinality estimator
5. physical operator selection
6. iterator/batch executor

현재 약 120KB 규모의 `query.go` 하나에 파싱, 바인딩, 계획, 실행, 정렬, 검색 scoring, 값 비교가 함께 있어 기능 확장 시 회귀 위험이 빠르게 커집니다.

---

## P1-3. 쿼리 row 표현의 할당량이 높다

각 query row는 바인딩 값 slice와 bound 여부 slice를 가집니다. 패턴 확장 때마다 이 상태를 복제하며, 최종 API는 각 결과 row를 `map[string]any`로 만듭니다. `ORDER BY`는 모든 row를 stable sort하고, `DISTINCT`는 결과 map이 완성된 후 다시 해시·비교합니다.

CI의 multi-hop benchmark는 약 다음과 같습니다.

* 98µs/op
* 95KB/op
* 814 allocations/op

### 개선 순서

1. 바인딩 8개 이하를 위한 inline small-row representation
2. `[]bool` 대신 bitmap
3. executor 단위 row arena
4. 결과 map은 최종 경계에서만 생성
5. `COUNT`는 row를 보관하지 않는 streaming aggregate
6. `ORDER BY ... LIMIT K`는 전체 sort 대신 top-K heap
7. `DISTINCT`는 map 생성 전 projected tuple에서 수행
8. 별도의 cursor API로 eager `[]map` 회피

기존 API는 호환성을 위해 유지하되, 고성능 사용자를 위해 transaction-scoped borrowed row 또는 column cursor를 추가하는 것이 좋습니다.

---

## P1-4. 쿼리 메모리 budget이 실제 peak scratch보다 과대 계상될 가능성이 있다

node·edge candidate ID 배열을 처리할 때 `chargeTemporary(len(ids) * 8)`을 호출하지만, 해당 pattern 처리가 끝났을 때 대칭적인 release가 보이지 않습니다. 반면 FTS 토큰 임시 메모리는 명시적으로 release합니다.

따라서 여러 pattern이나 component가 있는 쿼리는 실제로 동시에 살아 있는 scratch보다 큰 누적값으로 `MaxBytes`에 도달할 수 있습니다. 특히 candidate가 postings의 borrowed slice이거나 다음 iterator 단계에서 이미 소비된 경우에는 논리적으로 peak live memory와 맞지 않습니다.

이 부분은 다음 테스트로 확정해야 합니다.

* 동일한 peak scratch를 순차적으로 사용하는 다중 pattern
* 작은 `MaxBytes`에서 pattern 수만 증가시키는 쿼리
* indexed candidate와 full-scan candidate 비교
* FTS/vector special WHERE 전후의 budget 잔액

메모리 budget은 다음 세 종류로 분리하는 편이 좋습니다.

* 누적 work
* 현재 live scratch bytes
* 최종 결과 retained bytes

---

## P1-5. 그래프가 변경된 쓰기 트랜잭션에서는 쿼리 인덱스가 광범위하게 비활성화된다

`indexedNodeIDs`는 transaction에 graph changes가 있으면 기본적으로 인덱스 사용을 중단합니다. 이는 commit 전까지 property postings가 최신 graph state를 반영하지 않기 때문입니다.

정확성에는 안전하지만, 긴 쓰기 트랜잭션에서 초반에 변경을 수행한 뒤 후반에 쿼리하면 전체 scan으로 급격히 느려집니다.

해결책은 base index를 버리는 것이 아니라 overlay를 두는 것입니다.

* base postings
* transaction upsert IDs
* transaction delete IDs
* changed property old/new 값
* create/drop index delta

조회 시 base postings와 delta를 병합하면 read-your-writes와 인덱스 성능을 모두 유지할 수 있습니다.

---

## P1-6. 긴 reader가 오래된 COW 세대를 계속 붙잡는다

읽기 트랜잭션은 불변 graph pointer를 고정합니다. 따라서 reader가 오래 살아 있는 동안 이후 writer들이 교체한 페이지, postings, adjacency chunk, property map은 GC될 수 없습니다.

예를 들어 한 reader가 수십 분 열린 상태에서 hot node들의 property가 지속적으로 변경되면 각 커밋은 새 COW 페이지를 만들고, reader는 모든 이전 세대의 경로를 간접적으로 유지할 수 있습니다.

`MaxDatabaseSnapshotBytes`는 canonical serialized snapshot의 논리적 상한이지 다음을 제한하지 않습니다.

* Go map overhead
* interface와 포인터
* COW 구세대
* allocator fragmentation
* GC 미회수 span
* HNSW와 postings
* vector tombstone
* export가 고정한 generation

공개 타입 주석도 여러 `MaxBytes`가 RSS가 아닌 논리적 메모리 한도라고 명시합니다.

### 필요한 운영 메트릭

* active read transaction 수
* oldest reader age
* oldest pinned commit ID
* 현재 commit과 oldest pinned commit의 거리
* COW clone bytes 추정치
* vector index bytes
* postings bytes
* tombstone bytes
* stream retained bytes
* checkpoint temporary bytes
* query scratch high-water mark

선택적으로 최대 읽기 트랜잭션 수명, 경고 callback, memory-pressure 시 신규 reader 거부 기능이 필요합니다.

---

## P1-7. 속성 인덱스 생성과 삭제가 전역 writer를 장시간 점유한다

`CreateNodePropertyIndex`와 `CreateEdgePropertyIndex`는 `DB.Update` 안에서 대상 label/type 전체를 순회하며 postings를 생성합니다. 취소 가능한 Context API도 없습니다.

큰 DB에서는 다음이 발생합니다.

* 인덱스 빌드 내내 다른 writer가 실패
* COW 구조와 postings에 큰 일시 할당
* commit 직전 derived budget 계산
* 실패하면 전체 빌드 비용을 지불하고 rollback
* 장시간 작업을 호출자가 취소하기 어려움

### 온라인 빌드 방식

1. commit C의 immutable snapshot을 고정
2. writer lock 밖에서 index generation 생성
3. C 이후 mutation delta 수집
4. delta catch-up
5. 짧은 writer lock에서 definition과 generation swap
6. 실패하면 기존 graph에는 영향 없음

벡터 인덱스와 속성 인덱스는 같은 generation builder 프레임워크를 공유할 수 있습니다.

---

## P1-8. stream trim이 남아 있는 로그 크기에 비례한다

스트림은 불변 chunk와 skip link를 사용합니다. tail read와 sequence 탐색은 효율적이지만, 앞부분을 trim할 때 남은 chunk를 새 연결 구조로 다시 구성합니다.

automatic changefeed가 보존 용량을 넘을 때 이 trim이 commit 경로에 들어가면, 오랫동안 안정적이던 write latency에 주기적인 큰 spike가 나타날 수 있습니다.

또한 append는 tail chunk가 찰 때까지 tail record slice를 매번 복사합니다. 현재 벤치마크에서도 publish당 약 1.7~2.8KB, 9회의 할당이 발생합니다.

### 개선

* 물리적 재구성 대신 logical head sequence
* immutable segment 단위 저장
* 소비 범위 밖 segment를 백그라운드 reclaim
* transaction 안에서는 tail을 copy-on-first-write 후 mutable하게 사용하고 commit 때 freeze
* stream별 notify channel

현재 DB에는 사실상 하나의 `streamNotify`가 있어 어느 스트림의 commit이든 모든 대기 reader를 깨울 가능성이 있습니다. consumer가 많아지면 unrelated stream까지 깨우는 thundering herd가 생깁니다. stream별 또는 shard별 notification이 낫습니다.

---

## P1-9. property postings의 `LIMIT`가 posting 전체를 순회한다

postings가 map 또는 sharded map 기반이라 ID 순서가 없습니다. `LookupLimit`은 posting 전체를 방문하면서 가장 작은 ID K개를 정렬 삽입합니다.

최신 벤치마크에서는 10K 데이터에서 다음 차이가 있었습니다.

* unique value, LIMIT 1: 약 414ns
* common value, LIMIT 1: 약 117µs
* common value 전체 조회 후 LIMIT 1: 약 1.11ms

최근 bounded residual filtering으로 전체 물질화보다는 많이 개선됐지만, common postings에서는 `LIMIT 1`이어도 posting 전체를 훑습니다.

### 대안

* 정렬된 immutable posting chunks
* small sorted vector → roaring-style bitmap 전환
* chunk별 min/max ID
* K가 작을 때 min-ID prefix cache
* 큰 K에는 bounded max-heap
* 순차 ID에 특화된 compressed delta encoding

현재처럼 64개 이하를 map으로 두다가 sharded representation으로 승격하는 구조는 쓰기에는 편하지만, 한번 large로 승격한 posting은 다시 작아져도 downgrade되지 않습니다. churn 이후 메모리와 iteration 비용이 high-water 상태에 머물 수 있습니다.

---

## P1-10. 전체 메모리 상주 구조에서 벡터와 텍스트가 중복된다

HNSW의 `VectorIndexNode`는 벡터 자체를 보관합니다. 같은 벡터가 노드 property map에도 존재하므로 벡터 payload가 적어도 두 번 저장됩니다. update/delete된 벡터는 tombstone에도 복제됩니다.

128차원 float32 벡터 100K개만 보더라도 원본 payload는 약 51.2MB입니다. HNSW 노드에 동일 벡터를 다시 보관하면 payload만 추가 51.2MB이고, 여기에 다음이 더해집니다.

* slice headers
* PagedMap pages
* HNSW neighbor slices
* 레벨별 slice
* tombstone vector
* node property map 및 interface
* GC metadata

FTS도 별도 원문, token slice, token string, postings를 보관합니다. 같은 텍스트를 node property에도 넣으면 다시 중복됩니다.

장기적으로는 property map 안에 대형 vector/text를 직접 넣는 대신 다음 구조가 더 적합합니다.

* typed value store
* vector arena 또는 column store
* node property는 handle만 보유
* HNSW는 같은 vector block을 참조
* interned labels, edge types, property keys, FTS tokens
* generation 단위 immutable contiguous arrays

현재 `map[string]any`는 사용성에는 좋지만 데이터베이스 내부 representation으로는 pointer와 GC 비용이 큽니다.

---

## P2-1. JSON 기반 snapshot과 WAL은 안전하고 읽기 쉽지만 CPU·할당 비용이 크다

지속 형식은 typed JSON value, envelope, checksum, database ID, commit ID를 사용합니다. WAL 프레임은 길이와 CRC를 검증하고, 불완전한 마지막 프레임과 완전하지만 손상된 프레임을 구분합니다. 연속 commit ID와 database ID도 검증합니다. 이 부분은 매우 좋습니다.

그러나 복구 시에는 다음 비용을 지불합니다.

* JSON tokenization과 reflection
* `persistedValue` 중간 객체
* map과 slice 재할당
* 노드·엣지 정렬
* 모든 파생 인덱스 재구축
* FTS 재토큰화
* property postings 재해시
* HNSW 선택 시 추가 전체 빌드

CI의 일반 `ColdOpen` fixture는 약 다음과 같았습니다.

* 약 132ms
* 약 117.4MB allocated bytes
* 약 512.7K allocations

명시적 checkpoint도 약 62.7ms, 15.8MB, 100K allocations였습니다. 이 수치는 retained heap이 아니라 한 작업에서 발생한 총 할당량입니다.

### 진화 방향

한 번에 완전히 교체하기보다 다음 순서가 현실적입니다.

1. 현재 JSON envelope·버전·checksum은 유지
2. payload를 versioned binary record stream으로 교체
3. string dictionary와 property-key ID 도입
4. 노드·엣지·스트림 segment 분리
5. 지속 가능한 파생 index generation 추가
6. recovery가 전체 map을 중간 객체로 만들지 않고 직접 최종 구조에 decode

사람이 읽기 쉬운 JSON의 디버깅 장점은 export 형식에 남기고, 내부 지속 형식은 CPU와 크기를 우선하는 편이 낫습니다.

---

## P2-2. 공개 API의 nil·closed 상태 처리가 일관되지 않다

`Close`, `Serialize`, `BeginSnapshot`, 일부 compatibility API는 `db == nil || db.inner == nil`을 확인합니다. 반면 `Checkpoint`, `Begin`, `View`, `Update`, `Query`, 검색·캐시·인덱스 메서드 상당수는 바로 `db.inner`를 역참조합니다.

따라서 zero-value DB나 nil receiver에서 메서드에 따라 다음 중 하나가 발생합니다.

* `ErrDatabaseClosed`
* no-op
* panic

저장소 정확성 문제는 아니지만 공개 API 계약으로는 좋지 않습니다. 모든 메서드가 공통 `requireInner` 또는 equivalent guard를 사용해야 합니다.

`Tx`는 비교적 일관되게 inactive 상태를 검사합니다. DB 계층도 같은 수준으로 정리해야 합니다.

---

## P2-3. 옵션 표면이 실제 엔진 기능보다 넓다

공개 `OpenOptions`에는 `CacheSizeMB`, `PageSize`, WAL enable/disable, adjacency cache, 두 개의 vector enable alias 등이 있습니다. 그러나 엔진은 다음처럼 동작합니다.

* `CacheSizeMB`: 0 또는 100만 허용하며 실제 DB cache 크기로 사용하지 않음

* `PageSize`: 0 또는 4096만 허용하며 데이터 구조 page 크기를 변경하지 않음

* WAL: 항상 활성화

* adjacency cache: 미지원

* `EnableVector`와 `EnableVectors`: alias

* 메모리 limit: 실제 RSS가 아니라 논리적 snapshot/build 크기

호환성 계약을 위한 필드라는 의도는 이해되지만, 허용되면서 아무 효과가 없는 값은 특히 위험합니다. 호출자가 `CacheSizeMB=100`을 설정하고 실제 heap이 100MB 안으로 제한된다고 오해할 수 있습니다.

권고는 다음 중 하나입니다.

* 효과 없는 nonzero 설정은 모두 `ErrUnsupportedOption`
* `Capabilities()`로 지원 옵션을 명시
* 호환성 타입과 native Go 타입 분리
* deprecated alias 정리

---

## P2-4. read-only Open도 독점 파일 lock을 사용한다

`OpenContext`는 `ReadOnly` 여부와 무관하게 lock을 끄지 않았다면 동일한 path lock을 획득합니다. 따라서 여러 프로세스가 같은 DB를 read-only로 동시에 여는 구성이 제한됩니다.

Unix와 Windows 구현은 경로 canonicalization, symlink·hardlink·layout 충돌을 상당히 방어적으로 처리합니다. 이는 큰 장점입니다. 그러나 JS·Plan 9·WASI portable 구현의 lock은 프로세스 내부 범위이므로, OS 수준 다중 프로세스 배타성을 보장한다고 간주하면 안 됩니다.

향후에는 다음이 필요합니다.

* writer: exclusive lock
* read-only: shared lock
* persistent format migration/checkpoint publication: 별도 maintenance lock
* portable 플랫폼: 명시적인 single-instance capability 상태
* `DisableLock`은 `UnsafeDisableLock`처럼 위험을 이름에 드러내기

---

## P2-5. 내보내기는 안전하지만 큰 DB에서 완전한 streaming은 아니다

exporter는 deterministic ID 정렬, 임시 파일 작성, sync, 원자적 게시, 디렉터리 sync, 동시 writer 직렬화를 수행합니다. 정확성과 재현성은 좋습니다.

다만 DB 자체가 이미 전체 메모리 상주 구조이며, export는 한 graph generation을 고정한 상태에서 노드·엣지 ID 배열을 만들고 정렬합니다. 따라서 추가 메모리는 적어도 O(N+E) ID이고, 첫 바이트 이전에 정렬 비용이 발생할 수 있습니다. `Serialize()`는 전체 결과를 `[]byte`로 반환하므로 더 큰 peak memory가 생깁니다.

`PagedMap` 내부 radix 구조는 bucket·page·occupied bit를 순서대로 순회할 수 있습니다. sparse high root key만 정렬하면 전체 ID 배열을 만들지 않고 deterministic ordered iteration을 구현할 수 있습니다.

CSV generation 방식은 게시 안전성은 높지만, 이전 generation 회수가 자동 reader lease와 강하게 결합되어 있지 않아 명시적 정리가 없으면 디스크에 누적될 수 있습니다.

---

# 5. 성능 수치가 보여주는 설계의 장단점

다음은 동일 SHA의 최신 CI 결과에서 대표값을 정리한 것입니다.

| 경로                         |                        대표 측정치 | 해석                              |
| -------------------------- | ----------------------------: | ------------------------------- |
| 노드 점 조회                    |        약 453ns, 560B, 8 alloc | 빠르지만 안전한 복사 비용 존재               |
| 단순 쿼리                      |       약 1.24µs, 1KB, 16 alloc | parser cache 이후 기본 overhead는 낮음 |
| 10K property index 직접 조회   |                       약 376ns | 우수                              |
| 10K property index 쿼리      |                      약 3.17µs | 쿼리 추상화 overhead 약 8배            |
| 단일 직접 커밋, 1K 노드            |                       약 238µs | WAL sync 비용 중심                  |
| 단일 직접 커밋, 100K 노드          |                       약 233µs | DB 총크기에 거의 독립적                  |
| 100K vector ANN, clustered |     평균 약 0.501ms, p99 0.743ms | warm 검색은 좋음                     |
| 100K HNSW build            |                      약 239.7초 | cold start 및 rebuild가 치명적       |
| 10K ANN fallback           |                     약 0.886ms | 정상 ANN 약 36µs보다 크게 느림           |
| 100K FTS rare term         |                     약 0.533µs | postings의 장점이 잘 드러남             |
| 100K FTS common term       |                      약 8.25ms | match cardinality에 선형           |
| checkpoint                 |  약 62.7ms, 15.8MB, 100K alloc | JSON·정렬 비용                      |
| 일반 cold open               |  약 132ms, 117.4MB, 513K alloc | 전체 decode·파생 index rebuild 비용   |
| 100K degree 노드 삭제          | 약 233ms, 543.9MB, 1.20M alloc | 최우선 bulk mutation 병목            |
| multi-hop query            |       약 98µs, 95KB, 814 alloc | row 복제가 병목                      |
| stream bulk read 100K      |              약 2.65ms, 4.02MB | 결과 자체 크기에 비례                    |
| unique postings 10K 생성     | 약 11.2ms, 16.6MB, 67.8K alloc | 고카디널리티 문자열 인덱스 비용               |

이 결과에서 가장 중요한 것은 평균 성능이 아니라 **분포가 갈리는 조건**입니다.

* 작은 변경: 매우 좋음
* 선택도 높은 index lookup: 매우 좋음
* warm ANN: 좋음
* 희소 FTS: 매우 좋음
* common posting, fallback, supernode, rebuild, cold open: 급격한 절벽



---

# 6. CPU 관점의 상세 리뷰

## 잘된 부분

### 변경 크기에 비례하는 COW

`PagedMap`은 순차 uint64 ID에 특화된 radix page이고, 한 변경에 root·bucket·page만 복제합니다. 일반 sharded map보다 ForkSet 할당이 작고, 전체 iteration은 allocation-free입니다.

### 벡터 거리 계산

거리 루프는 네 개 accumulator로 unroll되어 있고, 비교 전용 squared distance 함수가 별도로 있습니다. HNSW 내부는 제곱 거리를 사용해 불필요한 sqrt를 피합니다. context cancellation도 일정 간격으로 검사합니다.

### FTS 희소 후보 경로

exact term은 postings에서 후보를 줄이고 top-K heap을 사용합니다. 결과를 전부 정렬하지 않는 방향이 좋습니다.

## 개선할 부분

### exact vector scan은 모든 후보에 sqrt를 계산한다

정확 검색 경로는 각 vector마다 `VectorDistance`를 호출해 sqrt까지 계산한 뒤 top-K heap에 넣습니다. 순위 비교에는 squared distance만 필요하므로 다음처럼 바꾸는 것이 낫습니다.

* 모든 후보: squared distance
* top-K 확정 후: 최종 K개만 sqrt

쿼리의 vector scoring에서도 정렬 전에는 제곱 거리를 유지할 수 있습니다.

### composite property index key가 매번 SHA-256을 계산한다

`[]byte`, vector, list, map 속성은 canonical hash를 만들기 위해 SHA-256을 사용합니다. map은 key를 정렬하고 재귀적으로 encoding합니다. 안전하고 결정적이지만 자주 변경되는 복합 속성에서는 CPU와 할당이 큽니다. 현재 composite key benchmark도 약 580ns, 344B, 19 allocations 수준입니다.

복합 객체 전체 equality index가 실제 사용되는 빈도가 낮다면 기본 지원을 줄이거나, normalized value에 미리 canonical hash를 캐시하는 편이 낫습니다.

### fuzzy FTS의 어휘 전체 탐색

fuzzy 검색은 token vocabulary를 순회하면서 Levenshtein 거리를 계산할 수 있습니다. vocabulary가 작을 때는 빠르지만 고카디널리티 corpus에서 `어휘 수 × term 수 × 문자열 길이²`에 가까워집니다. Levenshtein 구현도 rune slice 두 개와 DP row 두 개를 할당합니다.

대안은 다음과 같습니다.

* trigram inverted index
* BK-tree
* Levenshtein automaton
* maxDistance에 맞춘 banded DP
* DP buffer pooling
* 길이 차이·prefix 기반 사전 pruning

### JSON recovery와 export

전체 state를 JSON으로 decode·encode하고 파생 인덱스를 다시 만드는 경로는 CPU 캐시 관점에서도 좋지 않습니다. 많은 작은 map과 pointer 객체를 생성해 GC와 allocator가 hot path가 됩니다.

---

# 7. 메모리 관점의 상세 리뷰

## PagedMap의 장점과 한계

순차 ID가 밀집된 경우 64개 slot이 하나의 value page에 들어가며 구조적 locality가 좋습니다. 빈 page와 root는 삭제 시 제거됩니다.

반면 매우 희소한 높은 ID 하나는 별도 root, bucket, page를 요구할 수 있습니다. 현재 ID는 내부적으로 순차 할당되므로 정상 데이터에서는 큰 문제가 아니지만, deserialize·recovery·호환성 경로에서 희소 ID가 허용될 경우 구조 overhead가 커질 수 있습니다.

또한 `smallActive` 최적화는 소수 page일 때만 적용되고 한번 overflow 상태가 되면 축소 후에도 작은 경로로 복귀하지 않습니다. correctness 문제는 아니지만 churn 이후 iteration 비용이 현재 cardinality가 아니라 과거 high-water의 구조에 영향을 받을 수 있습니다.

## adjacency의 singleton 비용

chunked immutable adjacency는 append 확장성을 크게 개선했습니다. 그러나 한 엣지만 가진 노드마다 별도 `EdgeList`와 chunk를 만들기 때문에 벤치마크상 약 152 raw bytes/list가 필요합니다.

대부분의 실제 그래프가 낮은 degree를 가진다면 다음 representation이 효과적입니다.

* 0개: nil
* 1개: inline edge ID
* 2~4개: inline small array
* 그 이상: chunk chain

이 변경은 대규모 sparse graph의 메모리를 크게 줄일 가능성이 있습니다.

## 문자열과 interface의 GC 비용

노드마다 다음 객체가 흩어져 있습니다.

* `*NodeRecord`
* label string slice
* `map[string]any`
* map buckets
* interface headers
* string headers와 backing bytes
* nested slice/map

그리고 같은 label, property key, edge type, FTS token이 여러 위치에서 반복됩니다.

가장 큰 장기 개선은 micro-allocation pooling보다 **interning과 tagged value representation**입니다.

* label ID
* edge type ID
* property key ID
* token ID
* tagged scalar/list/map value
* 큰 blob/vector는 별도 immutable arena

표준 라이브러리만 사용하는 현재 철학을 유지하면서도 구현할 수 있습니다.

## 논리 budget은 실제 메모리 차단기가 아니다

`SnapshotBytes`, `DerivedIndexLogicalBytes`, `VectorIndexBuildMaxLogicalBytes`, query `MaxBytes`는 유용한 DoS 방어선입니다. 하지만 실제 resident memory를 제한하지 않습니다.

특히 다음 상황에서는 논리 snapshot이 그대로여도 RSS가 증가합니다.

* 오래된 reader가 이전 세대를 pin
* HNSW vector 중복
* vector tombstone 증가
* 큰 query 결과 map
* export ID 정렬
* checkpoint encode buffer
* allocator fragmentation

따라서 “512MiB snapshot limit이면 프로세스가 512MiB 이내”라는 운영 가정은 성립하지 않습니다.

---

# 8. 레이턴시와 동시성

## 읽기 동시성은 좋다

reader는 immutable graph를 읽고 writer는 새 세대를 만듭니다. WAL I/O 중에도 기존 reader가 현재 세대를 계속 사용할 수 있습니다. background checkpoint도 준비 단계는 writer lock 밖에서 수행합니다.

이는 전체 설계의 가장 좋은 부분 중 하나입니다.

## writer는 하나이며 대기열이 없다

두 번째 writer는 공정하게 대기하지 않고 `TryLock` 실패로 `ErrWriteTxActive`를 반환합니다. 이는 embedded API에서는 예측 가능한 선택이지만 burst write 처리에는 불리합니다.

기존 nonblocking 의미를 유지하면서 별도 API를 추가하는 것이 좋습니다.

* `BeginWriteContext`: queue에서 대기
* queue depth·wait duration metric
* 선택적 group commit
* 최대 대기 writer 수
* priority 또는 maintenance writer 구분

WAL fsync가 커밋당 수행되는 구조에서는 group commit이 쓰기 throughput을 크게 올릴 수 있습니다. 단, durability latency 계약이 달라지므로 opt-in이어야 합니다.

## 명시적 checkpoint와 Serialize는 무거운 writer 작업이다

background checkpoint는 준비를 밖에서 하지만, 명시적 checkpoint와 Serialize는 writer 상태와 active transaction을 강하게 제한합니다. `Serialize()`는 전체 DB를 `[]byte`로 만들어야 하므로 큰 DB에서 긴 writer 정지와 메모리 spike가 생깁니다. `SerializeTo(io.Writer)`와 cancel 가능한 `CheckpointContext`가 필요합니다.

---

# 9. 지속성·복구 평가

이 영역은 높은 평가를 받을 만합니다.

## 잘된 점

* frame 길이와 checksum 검증

* database ID 검증

* commit ID 연속성 검증

* 완전한 손상 frame과 불완전 tail 구분

* ID reservation으로 crash 후 ID 재사용 방지

* commit outcome이 불명확하면 DB를 recovery-required 상태로 전환

* checkpoint temp 파일 작성 후 sync와 원자적 게시

* 디렉터리 sync

* state/WAL/ID reservation 간 DB identity 검증

* 다양한 장애 주입 hook

* read-only와 writer의 temp cleanup 차등

* 파생 인덱스를 canonical state에서 재구축해 영구 손상 전파 방지

특히 WAL 쓰기 실패 후 “커밋이 안 됐다”고 단정하지 않고 `ErrCommitOutcomeUnknown`과 recovery-required 상태를 사용하는 것은 올바른 데이터베이스 설계입니다.

## 남은 위험

### 지속 포맷의 의미 검증

CRC가 맞아도 의미적으로 잘못된 다음 상태를 충분히 거부해야 합니다.

* 존재하지 않는 endpoint를 가진 edge
* 중복 ID
* next ID보다 큰 canonical ID
* FTS record와 node 불일치
* index definition의 비정상 scope
* stream sequence 단조성 위반
* offset이 retained range와 비정상적으로 충돌
* vector dimensions 불일치

코드에 상당한 semantic validation이 있지만, 지속 형식 버전이 늘어날수록 이 검증을 별도 validator로 분리하는 것이 좋습니다.

### crash test와 실제 전원 손실은 다르다

장애 주입과 파일 단계 테스트가 매우 좋지만 실제 파일시스템의 writeback, rename, directory persistence, Windows antivirus/file-sharing semantics, network filesystem은 별도 문제입니다. ext4/xfs/APFS/NTFS의 실제 process-kill 테스트와 fsync fault harness가 있으면 더 강해집니다.

---

# 10. 검색 계층

## 10.1 벡터 검색

### 장점

* 검색 중 할당이 매우 낮음

* scratch pooling

* deterministic level 생성

* squared distance 사용

* tombstone과 mutation debt 계측

* work·memory budget

* exact fallback으로 결과 정확성 보전

* context cancellation

* tie-break를 NodeID로 고정해 결정적 결과 제공

### 한계

현재 벡터 모델은 사실상 다음과 같습니다.

* 데이터베이스 전역 dimensions
* L2 distance
* 노드당 선택되는 하나의 vector
* direct API에서 대상 property 지정 불가
* label·property filter 없는 전역 검색

`FirstVectorProperty`는 여러 `[]float32` property가 있으면 key가 사전순으로 가장 앞선 vector를 선택합니다. 즉 노드에 `image_embedding`과 `text_embedding`이 함께 있으면 direct vector API의 의미가 property 이름 정렬에 의해 결정될 수 있습니다.

제품 수준 모델은 다음이어야 합니다.

* named vector index
* `(label, property, dimensions, metric)`
* L2, cosine, dot product
* index별 build/search parameters
* filtered ANN
* multi-vector node 지원
* index readiness와 stale generation 상태
* vector field를 직접 지정하는 API

## 10.2 FTS

### 장점

* Unicode letter/digit 기반 일관된 tokenization
* lowercasing
* exact postings
* fuzzy 옵션
* top-K heap
* 희소 term의 탁월한 성능
* 별도 FTS record로 property schema와 독립

### 한계

ranking은 사실상 term frequency 합계입니다.

없는 기능은 다음과 같습니다.

* IDF
* document length normalization
* BM25
* token position
* phrase query
* proximity
* field weight
* stop word
* stemming
* Unicode normalization 정책
* 언어별 tokenizer
* prefix index

한국어의 경우 공백이나 기호로 구분되지 않은 어절 전체가 하나의 token이 됩니다. 형태소 검색, 조사 분리, 복합어 검색을 기대할 수 없습니다.

또한 FTS 레코드는 노드 property와 별도로 갱신됩니다. property 기반 `@@`와 직접 `IndexText`/`FTSSearch`가 서로 다른 검색 corpus 또는 semantics가 될 수 있습니다. 장기적으로는 named FTS index가 `(label, property)`를 자동 추적해야 합니다.

---

# 11. 쿼리 언어 커버리지

현재 구현은 “Cypher 호환”이라기보다 **Cypher 문법을 차용한 제한적 graph pattern DSL**로 보는 것이 정확합니다.

## 지원되는 주요 범위

| 분야    | 지원 범위                                                               |
| ----- | ------------------------------------------------------------------- |
| 시작 절  | `MATCH`, `CREATE`, `UNWIND`                                         |
| 패턴    | 고정 길이 노드·관계 패턴, 방향 관계, 여러 comma component                           |
| 필터    | AND, OR, NOT, 동등·부등·대소 비교, IN, 문자열 prefix/suffix/contains, NULL     |
| 특수 조건 | `id()`, vector `<=>`, FTS `@@`                                      |
| 변경    | 단일 노드 CREATE, bound node 간 edge CREATE, SET, REMOVE                 |
| 삭제    | DELETE, DETACH DELETE                                               |
| 결과    | binding, property, ID projection                                    |
| 집계    | `count(*)`, `count(binding)` 중심                                     |
| 후처리   | DISTINCT, ORDER BY, SKIP, LIMIT                                     |
| 값     | parameter, map value, backtick identifier                           |
| 인덱스   | label/type, 단일 property equality, 다중 equality postings intersection |

vector와 FTS 조건은 OR나 NOT 아래에서 사용할 수 없도록 제한됩니다.

## 지원되지 않거나 확인되지 않은 주요 범위

* `OPTIONAL MATCH`
* `MERGE`
* `WITH`
* `UNION`, `UNION ALL`
* `CALL`
* subquery
* pattern `EXISTS`
* variable-length relationship `*`
* shortest path와 quantified path
* path value와 named path
* 관계 타입 대안
* `sum`, `avg`, `min`, `max`, `collect`
* grouping aggregate
* `CASE`
* 일반 산술식
* 풍부한 scalar function
* list comprehension
* list slicing/indexing
* map projection
* 복수 단계 projection pipeline
* 복잡한 CREATE pattern
* `ON CREATE`, `ON MATCH`
* uniqueness·not-null constraint
* composite·range index
* `EXPLAIN`, `PROFILE`, planner hint
* streaming result cursor
* named vector·FTS index
* filtered ANN

키워드 인식은 기본적으로 대문자 문법을 전제로 합니다. whitespace normalization은 하지만 SQL 계열처럼 keyword를 case-insensitive하게 lexicalize하지 않습니다.

## 쿼리 커버리지 우선순위

기능을 무작정 넓히기 전에 다음 순서가 낫습니다.

1. `EXPLAIN`
2. 물리 search scan
3. streaming aggregate
4. `OPTIONAL MATCH`
5. `WITH`
6. 여러 aggregate와 grouping
7. variable-length traversal에 대한 명확한 resource budget
8. `MERGE`와 constraint
9. cursor API
10. subquery와 UNION

현재 구조에서 `MERGE`, variable-length path, grouping을 바로 추가하면 `query.go`가 더욱 monolithic해지고 중간 row 물질화가 폭발할 가능성이 큽니다. 먼저 실행기 계층을 분리해야 합니다.

---

# 12. 값 모델과 속성 인덱스

## 값 정규화는 방어적이다

입력 값은 다음을 검증합니다.

* UTF-8
* 정수 범위
* NaN과 infinity
* vector 유한성
* 최대 깊이
* 최대 element 수
* 최대 byte 수
* slice/map cycle
* 문자열 key map 여부
* reflection으로 전달된 일반 slice/map

그리고 사용자 객체를 내부 state가 참조하지 않도록 깊은 복사를 수행합니다. 이는 embedded DB에서 매우 중요합니다. 호출자가 commit 후 원본 map이나 slice를 변경해 DB 상태를 몰래 바꾸는 문제를 막습니다.

## 비용

매 변경과 반환에서 다음 비용이 있습니다.

* reflection
* map 복제
* nested 값 복제
* vector 복제
* byte slice 복제
* property key 반복
* `reflect.DeepEqual`

안전성과 성능의 trade-off는 합리적이지만, 고성능 사용자를 위한 transaction-scoped borrowed read API나 typed property API가 있으면 좋습니다.

## 인덱스 collision 처리

문자열 postings는 FNV 계열 hash 뒤에 실제 string map을 두므로 hash collision을 안전하게 분리합니다. composite property는 SHA-256 digest를 key로 사용합니다. 암호학적 충돌 위험은 현실적으로 매우 작지만, 직접 index API가 digest postings 결과를 반환할 때 실제 property equality를 최종 재검증하는 방식을 일관되게 유지하는 것이 좋습니다.

---

# 13. 트랜잭션 시맨틱

## 좋은 부분

* write transaction 단일 owner 계약
* read transaction snapshot
* managed transaction에서 직접 commit·rollback 금지
* commit 성공·실패 후 transaction 비활성화
* WAL 성공 전 graph 미게시
* write conflict와 recovery-required 분리
* query mutation의 statement-level atomicity
* graph 변경과 automatic changefeed의 원자성
* property와 vector index 변경이 commit에 결합
* context cancellation 지원

특히 명시적 write transaction 안에서 변경 쿼리를 실행할 때 해당 statement용 COW branch를 한 번 더 만들고, statement가 성공한 경우에만 transaction state로 병합합니다. 앞선 transaction 변경은 유지하면서 실패한 statement만 되돌릴 수 있습니다.

## 개선할 부분

* writer fairness 없음
* blocking BeginWrite 없음
* group commit 없음
* DDL과 vector rebuild가 writer를 장시간 점유
* 오래된 reader 추적 없음
* transaction별 allocation 및 work metric 없음
* 한 트랜잭션의 최대 변경 entity 수 제한이 간접적
* 매우 큰 changefeed record의 분할 정책 부족

---

# 14. 공개 오류와 진단

공개 호환 오류 타입은 code, phase, source location, diagnostic 정보를 표현할 수 있게 설계되어 있습니다. 그러나 내부 query engine은 주로 parse와 execution 두 단계만 구분하고, 정확한 token 위치·semantic phase·plan phase를 폭넓게 채우지 않습니다.

즉 API 표면이 약속하는 진단 능력보다 실제 구현이 제공하는 정보가 좁습니다.

parser를 token 기반으로 바꾸면 다음이 가능해집니다.

* byte offset
* line/column
* offending token
* expected token set
* semantic error와 parse error 분리
* unsupported feature와 malformed query 분리
* plan-time resource rejection
* stable diagnostic code

운영에서는 문자열 오류보다 machine-readable code가 중요합니다.

---

# 15. 임베딩 계층

`embedding` 패키지는 core DB와 잘 분리되어 있고, 외부 의존성 없이 hash 기반 provider와 HTTP provider를 제공합니다.

다만 production embedding client로는 다음이 부족합니다.

* batch embedding
* retries와 exponential backoff
* retry-after 처리
* circuit breaker
* provider별 rate limit
* 응답 dimension 검증
* model·dimensions cache
* observability hook
* 전체 response read를 피하는 streaming decode
* non-2xx body drain을 통한 connection reuse
* 공급자 오류의 구조화

hash embedding은 token이 없거나 모두 필터링되는 입력에서 zero vector가 나올 수 있습니다. zero vector의 cosine normalization이나 의미가 명확하지 않으므로 명시적으로 오류를 반환하거나 별도 sentinel embedding을 사용하는 편이 낫습니다.

외부 HTTP 호출을 write transaction 안에서 수행하지 않도록 API 사용 패턴도 분명해야 합니다. 단일 writer를 잡은 채 외부 모델 응답을 기다리면 전체 쓰기 가용성이 외부 서비스 레이턴시에 종속됩니다.

---

# 16. 테스트와 벤치마크 평가

## 강점

테스트 체계는 이 규모의 프로젝트치고 상당히 진지합니다.

* Linux, macOS, Windows

* race detector

* 반복 concurrency 테스트

* path locking과 platform별 구현

* WAL 부분 쓰기·손상·sync 실패

* checkpoint 게시 단계 장애

* export crash semantics

* 별도 conformance 모듈

* property index intersection

* vector recall

* 고차수 삭제

* stream scaling

* 100K 규모 벤치마크

* allocation regression gate

* 이전 main 결과와 비교

conformance가 별도 모듈로 분리된 점은 좋습니다. 구현 내부 테스트가 아니라 외부 계약으로 지속 형식과 crash behavior를 보는 효과가 있습니다.

## 중요한 누락

### HNSW-enabled cold open

현재 핵심 벤치마크에는 HNSW build 자체는 있지만, 실제 persisted graph를 HNSW sync mode로 Open하는 end-to-end 측정이 별도로 필요합니다.

측정 항목은 다음이어야 합니다.

* canonical decode
* derived postings build
* HNSW build
* peak heap
* retained heap
* GC pause
* time to first exact query
* time to ANN ready

### query-level vector·FTS

직접 검색 API 벤치마크는 충분하지만 쿼리 `<=>`, `@@`가 큰 MATCH 결과와 결합되는 경우가 빠져 있습니다. 현재 가장 큰 검색 성능 격차를 CI가 보호하지 못합니다.

### 지속 쓰기 중 checkpoint 진전성

다음 테스트가 필요합니다.

* snapshot prepare 시간보다 빠른 commit loop
* WAL 크기가 일정 상한 안에 유지되는지
* discarded checkpoint 횟수
* temporary write amplification
* 최종 checkpoint progress 보장

### 긴 reader의 RSS

* reader 하나를 고정
* 같은 hot set을 수십만 번 업데이트
* HeapInuse·HeapAlloc·GC 횟수·old generation 추정
* reader 종료 후 회수 여부

### stream trim p99

평균 publish benchmark만으로는 retention boundary에서 발생하는 trim spike를 볼 수 없습니다.

### 온라인 DDL

대형 label에서 property index 생성 중 writer availability, 메모리, cancellation을 측정해야 합니다.

### fuzz 및 정적 분석

workflow에서 지속적으로 수행되는 parser fuzz, recovery decoder fuzz, value normalization fuzz, `go vet`·staticcheck 계열 작업이 명확히 보이지 않습니다. 특히 handwritten parser와 손상 입력 decoder는 fuzz 효율이 높은 영역입니다.

### 실제 filesystem crash matrix

fault injection은 강하지만 process kill과 filesystem별 persistence 실험을 보완해야 합니다.

---

# 17. 추가로 확인된 세부 사항

## query cache

128개 고정 FIFO cache는 bookkeeping이 작고 단순합니다. 다만 raw query string을 key로 사용하므로 의미상 같은 whitespace 변형이 별도 entry가 될 수 있고, ad-hoc query가 많은 환경에서는 빠르게 thrash합니다. normalization한 canonical query를 cache key로 쓰고, hit rate가 실제로 낮을 때만 LRU나 TinyLFU를 고려하는 것이 좋습니다.

## 자연 결과 순서

ORDER BY가 없을 때도 ID와 binding 기반 결정적 순서를 상당히 보존하려는 의도가 보입니다. 테스트 재현성과 API 안정성에는 좋지만 optimizer의 join reorder 자유도를 제한합니다. “결정적 기본 순서”가 호환성 요구인지, 단순 구현 부산물인지 명시적으로 결정해야 합니다.

## 혼합 타입 정렬

서로 직접 비교할 수 없는 값이 문자열 표현 기반 total order로 떨어지는 경로는 비표준적이고 할당을 유발할 수 있습니다. 타입 rank를 명시적으로 정의하는 편이 낫습니다.

## 초기 생성 경로

새 DB 생성 시 초기 state checkpoint를 만든 뒤 WAL append 준비 여부에 따라 state+WAL checkpoint를 다시 만들 가능성이 보입니다. correctness 문제는 아니지만 최초 생성 I/O를 한 번의 원자적 initialization으로 합칠 수 있습니다.

## `Deserialize`

이름에서 순수한 메모리 DB를 연상하기 쉽지만 실제 구현은 임시 경로와 파일을 사용하고 Close에서 제거하는 구조입니다. 큰 payload에서 filesystem I/O와 임시 디스크 용량에 영향을 받습니다. true in-memory backend와 명확히 구분하는 것이 좋습니다.

## CSV generation 회수

원자적 게시 구조는 좋지만 실패나 reader 생존과 관계없이 오래된 generation을 자동으로 안전하게 회수하는 lifecycle 관리가 필요합니다.

---

# 18. 권장 개선 로드맵

## 단계 0: 즉시 수정할 항목

이 단계는 아키텍처를 바꾸지 않고도 고칠 수 있습니다.

1. vector result capacity를 live vector count 기준으로 변경
2. fallback 사유별 metric 추가
3. 모든 공개 DB 메서드의 nil/closed guard 통일
4. query temporary budget의 scoped release 검증 및 수정
5. query-level vector·FTS 벤치마크 추가
6. HNSW-enabled Open 벤치마크 추가
7. 지속 write 중 background checkpoint progress 테스트
8. 오래된 reader age와 pinned commit metric 추가
9. common posting `LookupLimit`에서 bounded max-heap 사용
10. exact vector scan에서 sqrt를 최종 K개에만 수행
11. `Create*PropertyIndexContext` 추가
12. per-stream notification 도입

## 단계 1: 운영 가용성 확보

1. WAL rotation 기반 fuzzy checkpoint
2. adjacency compaction의 per-list generation 또는 merge
3. HNSW background builder와 atomic generation swap
4. HNSW maintenance 때문에 canonical write를 거부하지 않는 기본 정책
5. property index online build
6. batch edge delete와 batch postings mutation
7. logical stream head와 segment reclaim
8. blocking·context-aware writer queue
9. writer wait, checkpoint discard, rebuild duration, pinned reader metric

## 단계 2: 쿼리 실행기 개선

1. parser·semantic·logical·physical·executor 분리
2. `VectorKNNScan`
3. `FTSPostingScan`
4. cardinality와 degree 통계
5. 연결 pattern의 방향 및 시작점 선택
6. top-K operator
7. streaming aggregate
8. pre-materialization DISTINCT
9. inline small row와 row arena
10. cursor 결과 API
11. `EXPLAIN`과 실제 cardinality profile

## 단계 3: 저장 representation 개선

1. versioned binary payload
2. string dictionary
3. tagged internal value
4. vector column store
5. 지속 가능한 named vector·FTS index
6. 정렬된 또는 bitmap postings
7. inline small adjacency
8. ordered PagedMap iterator
9. segmented snapshot
10. export의 bounded-memory streaming

## 단계 4: 기능 범위 확장

기반 실행기가 개선된 뒤에 다음을 추가하는 편이 안전합니다.

1. named vector·FTS index
2. cosine·dot product
3. filtered ANN
4. `OPTIONAL MATCH`
5. `WITH`
6. grouping aggregate
7. `MERGE`와 constraint
8. bounded variable-length traversal
9. subquery와 UNION
10. planner hints와 PROFILE

---

# 19. 최종 판단

이 코드는 “기능을 많이 붙인 작은 DB”라기보다, **내구성과 crash semantics를 먼저 확실히 만든 뒤 그래프·검색·스트림 기능을 얹은 구현**입니다. 저장 및 복구 계층에는 명확한 설계 규율이 있고, 불변 세대와 COW 선택도 임베디드 환경에 잘 맞습니다. 단일 레코드 커밋이 1K와 100K DB에서 거의 같은 비용을 보이는 것은 이 설계가 실제로 작동한다는 좋은 증거입니다.

다만 현재 구조는 비대칭적입니다.

* 저장·복구: production에 가까움
* 직접 점 조회와 선택도 높은 인덱스: production에 가까움
* warm vector 검색: 성능은 좋음
* 벡터 인덱스 수명주기: production 운영에 부족
* 쿼리 검색 통합: 초기 단계
* 복잡한 쿼리 플래너: 제한적
* supernode·긴 스트림·지속 쓰기 유지보수: 성능 절벽 존재
* 실제 메모리 관리: 논리 예산 중심
* 운영 메트릭과 온라인 maintenance: 부족

**전체를 다시 작성할 이유는 없습니다.** 유지해야 할 핵심은 다음입니다.

* immutable `GraphState`
* COW page·shard
* WAL-before-publish
* database identity와 commit continuity 검증
* background checkpoint preparation
* 파생 상태와 canonical 상태의 분리
* resource budget
* deterministic 결과와 지속 형식
* conformance 중심 테스트

가장 큰 효과를 내는 투자는 거리 함수의 수 나노초를 줄이는 일이 아닙니다. 다음 세 가지입니다.

1. **세대가 바뀌어도 진전하는 checkpoint와 index maintenance**
2. **검색 인덱스를 쿼리의 첫 번째 물리 연산자로 사용하는 planner**
3. **대량 변경을 단건 mutation 반복이 아니라 batch representation으로 처리하는 것**

이 세 가지가 해결되면 현재의 좋은 저장 기반을 그대로 유지하면서, CPU·메모리·꼬리 지연·재시작 시간·쿼리 커버리지 모두에서 한 단계 높은 임베디드 그래프 엔진으로 발전할 수 있습니다.

