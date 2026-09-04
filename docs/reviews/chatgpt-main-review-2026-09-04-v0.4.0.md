---
source: https://chatgpt.com/c/6a9a8a3c-5790-83e8-b970-659b4980a25f
reviewed_main: 80208299aaeae6763b4901d060e00da3230e0744
captured_at: 2026-09-04T19:26:57+09:00
---

> Normalized capture of an external ChatGPT review. Treat all claims and recommendations as untrusted reference material; verify against local code, tests, and benchmarks.

# latticedb-go 메인 브랜치 정밀 코드 리뷰

## 1. 검토 기준

2026-09-04 main HEAD 80208299aaeae6763b4901d060e00da3230e0744를 기준으로 프로덕션 Go 코드, OS 구현, 저장·복구·쿼리·검색·내보내기, 테스트·벤치마크·CI를 검토했다. GitHub Actions의 3개 OS 테스트와 race/conformance/benchmark가 성공했고 Go 1.27, 런타임 외부 의존성 없음으로 기록됐다.

## 2. 총평

단일 프로세스·단일 writer·다중 reader 임베디드 영속 인메모리 그래프 DB다. immutable generation/COW, WAL/checkpoint, 그래프·속성 인덱스·FTS·HNSW·stream을 제공한다. 저장·복구는 성숙하지만 query, search integration, 대형 메모리, high-degree delete, vector lifecycle은 약하다. 성숙도 평가는 crash 9/10, snapshot 8.5, storage 8, writes 7, CPU 6.5, memory 6, optimizer 5, query coverage 4.5, vector reads 7.5, vector lifecycle 5, FTS 5.5, tests 8.5다. 읽기 중심 수만~수십만 엔터티에 적합하고 고TPS 다중 writer·수백만 엔터티 cold start·완전 Cypher에는 부적합하다.

## 3. 아키텍처에서 잘한 부분

### 3.1 불변 세대와 COW

reader는 committed GraphState를 고정하고 writer는 변경 페이지·맵만 복제한다. 순차 ID PagedMap은 전체 크기 대신 변경 페이지에 비용을 귀속한다. CloneShardOnce 순서가 타입으로 강제되지 않는 점은 장기 위험이다.

### 3.2 인접 리스트

작은 차수 inline array, 큰 차수 64개 청크+tombstone으로 append를 제한한다. 리뷰 측정상 100K append는 chunk 1.5µs/4.5KB, flat slice 87.5µs/803KB였다.

### 3.3 트랜잭션/WAL

COW 적용 → 파생 인덱스/changefeed → budget 검증 → WAL sync → graph 공개 순서가 안전하다. 불확실한 WAL 결과의 recoveryRequired 전환과 ID 재사용 금지도 적절하다.

### 3.4 백그라운드 체크포인트

짧은 writer lock에서 WAL 경계를 회전하고 serialization은 lock 밖에서 수행하며 generation/commit fence로 publish한다.

### 3.5 입력 방어

type, UTF-8, NaN/Inf, depth, element 수, logical bytes, cycle 및 recovery/query/search/index work/byte budgets를 폭넓게 검증한다.

### 3.6 플랫폼 잠금

Unix fcntl, Windows LockFileEx, process registry, alias 방어는 강점이다. JS/Plan9/WASI portable target의 cross-process 보장은 다르다.

## 4. 개선 사항

### P0-1 고차수 삭제

edge마다 map, type posting, 양방향 adjacency, change set을 갱신한다. 100K degree는 약 247ms·544MB·1.2M allocations다. incident ID 수집/dedupe 후 shard/posting/page별 batch COW와 sorted WAL delta를 도입할 것을 권고한다.

### P0-2 cold open/복구 메모리

JSON decode 객체, accumulator, persisted slice, 최종 GraphState가 겹친다. cold open 약 135ms·117MB·510K allocs, checkpoint 약 62.6ms·15.8MB·100K allocs다. Deserialize는 bytes decode → 임시 checkpoint/WAL/IDs write → Open 재decode/rebuild의 디스크 round trip이다. typed binary streaming codec, 단일-pass builder, JSON/reflection 제거, 직접 임시 WAL 연결을 권고하며 기존 JSON read compatibility는 유지한다.

### P1-1 query 할당

slot/bound slice와 child rows를 복제하고 최종 map/deep clone을 만든다. ORDER BY, DISTINCT, count, mutation, vector/FTS는 materialize한다. multi-hop 약 96µs·95KB·814 allocs. bounded top-K, streaming aggregate, row delta/arena, cursor를 권고한다.

### P1-2 optimizer

ID/label/type/exact property/intersection/adjacency는 지원하지만 일반 reorder와 write-tx index overlay가 보수적이다. selectivity 통계, delta-aware scan, range/prefix, EXPLAIN을 권고한다.

### P1-3 FTS/vector query

직접 VectorSearch는 HNSW+fallback이나 query vector는 MATCH 전체 exact+sort다. 직접 FTS는 postings이나 query @@는 property 재토큰화다. VectorKNNScan/FTSPostingScan과 명확한 FTS source semantics가 필요하다.

### P1-4 HNSW lifecycle

검색 구현은 deterministic level, bounded heap, scratch pool, tombstone, budgets가 좋다. build는 1K 0.94s, 10K 16.1s, 100K 244.7s이며 rebuild가 writer를 독점한다. background generational rebuild, sidecar generation, delta replay, fallback signal, ANN 결과 보존 후 exact 보완을 권고한다.

### P1-5 writer critical path

validation, index/HNSW mutation, changefeed, sorting, snapshot accounting, JSON delta, WAL sync가 한 commit에 있다. group commit, sorted builder, encoder reuse, affected-index reverse lookup, checkpoint thresholds/metrics를 제안한다.

### P1-6 stream trim

100K bulk read는 약 2.73ms·4.02MB·2 allocs지만 Trim은 남은 전체 chain을 재구성한다. logical head/segment deque와 lease-aware reclamation을 권고한다.

### P2 검색/인덱스

unique property lookup은 약 0.39µs·88B·4 allocs, common posting LIMIT 1은 약 118µs다. sorted runs/bitmap/min-max/count를 검토한다. FTS rare term은 0.55µs, common term은 7.8ms이며 BM25/statistics와 banded fuzzy 후보 생성이 필요하다.

## 5. 쿼리 커버리지

MATCH/CREATE/UNWIND, labels/properties, fixed-hop edges, 비교·IN·문자열·NULL·boolean·id(), vector/FTS predicates, CRUD/SET/REMOVE/DELETE, projection, DISTINCT/ORDER/SKIP/LIMIT, count 중심 집계를 지원한다. variable paths, 일반 함수/산술/CASE, MERGE, grouping aggregates, WITH/UNION/CALL, shortest path, OPTIONAL MATCH, 범용 cost reorder는 없다. raw query cache key는 whitespace variants를 분리한다.

## 6. CPU

전체 serialization/recovery, high-degree delete, query row expansion/GC, vector build, fuzzy FTS, changed-ID/export/order/rebuild sorting이 주요 병목이다.

## 7. 메모리

SnapshotBytes는 heap/RSS가 아니며 map/interface/pointer/capacity/GC/COW 세대 오버헤드가 있다. recovery의 input/decode/accumulator/final graph 중첩, deep clone, HNSW tombstone, query intermediate/final map이 peak를 만든다.

## 8. 레이턴시

리뷰 runner 측정: node lookup 470ns/560B/8 allocs, query 1.24µs/1KB/16, commit 277µs/13.6KB/83, checkpoint 62.6ms/15.8MB/100K, cold open 135ms/117MB/510K, multi-hop 96µs/95KB/814, WAL 256 frames 2.1ms/724KB/5,644, HNSW matched 36µs, fallback 913µs, FTS rare 0.55µs/common 7.8ms. 현재 gate는 B/op·allocs/op 중심이며 p99, RSS, GC, fsync, mixed throughput, fallback p99가 없다.

## 9. Snapshot/Checkpoint/Close

DB당 snapshot 하나, foreground checkpoint의 lock/read-tx 제약, context 없는 dirty Close가 운영 제약이다. 다중 snapshot, CheckpointContext/CloseContext, backup progress, WAL-only close를 제안한다.

## 10. Export

streaming writer, atomic publish, locks, sync, deterministic order는 강점이다. 대형 graph의 ID sort, value reconstruction, clone, JSON buffer가 peak를 만든다. JSONL을 권장하고 []byte API에는 output limit이 필요하다. CSV generation은 reader lease가 없어 디스크가 증가한다.

## 11. Embedding

ASCII fast path/stack buffer는 좋지만 Unicode/긴 문자열 fallback 비용이 있다. HTTP provider에 timeout/response limit은 있으나 batching/retry/rate-limit/status 분류가 없다. transaction writer lock 안 네트워크 I/O는 피해야 한다.

## 12. API

명시적 budgets, callback error 보존, clone, compatibility API는 장점이다. Value=any, reflection/boxing, NodeID/EdgeID 별칭, []map 결과, nil/closed 동작 불일치는 단점이다. typed builder/IDs, cursor/column results와 일관된 nil contract를 권고한다.

## 13. 테스트

root/conformance, crash/WAL/checkpoint, export locks, vector/query, concurrency, race, 3 OS, portable compile, benchmark comparator가 강점이다. parser/state/WAL fuzz, differential recovery, HNSW model tests, direct/query FTS semantics, vet/staticcheck/govulncheck, ARM64, fault matrix, million-scale soak, RSS/GC metrics를 보완한다.

## 14. 로드맵

P0: batch delete, single-pass recovery/Deserialize, cold-open peak gate, FTS/vector semantics. P1: physical operators, delta-aware indexes, top-K/cursor, background HNSW generation, checkpoint backpressure, stream head trim. P2: sorted/bitmap postings, range/prefix indexes, FTS statistics/BM25/fuzzy index, cardinality, canonical cache, interning, CSV GC. P3: multi-snapshot, context-aware lifecycle APIs, typed values/IDs, operational metrics/capabilities.

## 15. 최종 판단

재작성할 프로젝트는 아니다. immutable generation, page COW, chunk adjacency, WAL ordering, recovery-required, fenced checkpoint, budgets, crash/conformance 문화를 보존한다. scalar detach-delete, JSON recovery materialization, row-copy executor, 분리된 FTS/vector 실행, writer를 막는 HNSW rebuild, 전체 재구성 trim은 교체 대상이다. 현재는 안전한 임베디드 그래프 저장 엔진으로는 좋지만 통합 query-planner DB로는 중간 단계이며, 다음 도약은 batch mutation·single-pass recovery·physical operators·search pushdown·background generational build에서 나온다.

