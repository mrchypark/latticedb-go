# Go와 Zig 전체 점검 — 2026-10-13

이 문서는 수정 전 기준점의 점검 기록이다. 이후 정확성 수정과 공개 디스크 API 재측정은 [후속 보고서](../benchmarks/disk-vector-2026-10-13/REPORT.md)에 기록한다.

Go는 이미 디스크 페이지 저장 엔진이다. Zig의 주요 기능 범주는 대부분 갖췄지만, 검색 기본 의미·백업 동시성·공간 회수·배포 도구가 다르다. 현재 성능 자료만으로 두 **디스크 DB 전체**의 우열을 판정할 수 없다. 이 평가는 API·파일 형식 상호호환성을 요구하지 않는다.

## 기준과 원격 동기화

| 대상 | 확인한 버전 |
|---|---|
| Go 로컬 HEAD / origin/main / v0.11.3 | `21d788c9cc1c399f63bab99a958aff0d64ff4058` |
| Zig 최신 main | `f8e778359a54c4a79d7a49210bf75aa152f47820` |
| Zig 최신 릴리스 v0.15.0 | `9800159e22e200f2b6888c7c6be1810adb695506` |
| 기존 Go 기능 평가의 Zig 기준 | `827891e2c6fd55d13aa8f8284a7c7043f68b60fd` |

`git fetch origin` 후 HEAD와 origin/main의 ahead/behind는 0/0이었다. 시작 시 로컬 수정이 없었으므로 merge/reset/push가 필요하지 않았다. 제품 소스는 수정하지 않았다. 이 보고서만 새로 추가했다. Yeoul preflight 검색은 관련 결과가 없었다. 두 저장소의 검색 인덱스가 없어 exact-search와 소스 읽기로 확인했다.

Zig는 별도 임시 복제본에서 검사했다. 기존 기능 기준 이후 최신 main까지 39개 파일이 바뀌었다. 이 구간의 변경은 주로 바인딩 배포·Electron·CLI·플랫폼 파일/페이지 처리이며, `src/query`, `src/graph`, `src/vector`, `src/fts`의 기능 변경은 없었다. 릴리스 v0.15.0 이후에는 FTS/planner 변경도 있으므로 릴리스와 main을 같은 것으로 취급하지 않았다.

## 저장 계층: 구현 완료와 비교 측정 누락을 구분

공개 [`db.go:98`](../../db.go#L98)는 `PageStorage: path != ":memory:"`를 설정한다. [`internal/engine/db.go:543`](../../internal/engine/db.go#L543)는 이 옵션을 `openPageDB`로 연결한다. 디스크 경로는 bbolt에 graph record, adjacency, property postings, stream, HNSW, FTS postings를 저장한다. `:memory:`와 `Deserialize`는 별도 메모리 경로다.

따라서 **저장 계층을 새로 만드는 작업은 필요하지 않다.** 남은 측정 문제는 Zig 비교 벤치마크가 이 경로를 사용하지 않는다는 점이다.

- [`vector_comparison_benchmark_test.go:15`](../../internal/engine/vector_comparison_benchmark_test.go#L15)는 저장/buffer-pool 계층을 제외한다고 명시한다.
- 같은 파일의 `zigHarnessIndexedDB`는 메모리 graph에 인덱스를 만들고 `&DB{graph: ...}`를 반환한다. 공개 `Open`, page store, 디스크 commit/reopen을 거치지 않는다.
- 반면 루트 [`db_benchmark_test.go:12`](../../db_benchmark_test.go#L12)의 `benchmarkDB`는 임시 디스크 경로를 공개 `Open`으로 연다. **CI 전체가 메모리 전용인 것은 아니다.** Zig와 직접 나란히 표시하는 100K 벡터 표의 비교 범위가 문제다.
- [기존 256 MiB / 1.42 GB 검증](../disk-storage.md)은 디스크 페이징의 근거다. 다만 새 page HNSW/FTS 구현 이전 실행이며, 대규모 검색 인덱스의 현재 RSS·성능까지 보증하지 않는다. 개별 트랜잭션, metadata/catalog, 반환 결과는 여전히 메모리를 사용한다.

## 사용자 관점의 차이

| 영역 | Go 현재 | Zig 최신 main | 평가 |
|---|---|---|---|
| 설치·배포 | cgo 없는 Go 라이브러리, bbolt 의존 | Zig 엔진, C ABI, CLI, Python/TS/Go(cgo)/Java 바인딩 | Go 앱 통합은 Go가 단순하다. 다언어·대화형 운영 도구는 Zig가 넓다. |
| 저장 | bbolt B+tree, 페이지별 읽기, 기본 디렉터리 DB | 자체 B+tree, buffer pool, WAL, 주 DB 파일과 WAL sidecar | 둘 다 실제 디스크 엔진이며 내부 구조는 크게 다르다. |
| 그래프·쿼리 | 다중 라벨, 방향/가변 경로, equality index, MERGE/WITH/UNWIND, 집계 | 같은 주요 기능 범주 | 전체 Cypher 동일성은 없다. Go 문법의 제한은 계약으로 관리한다. |
| 벡터 API | exact 및 page HNSW, M/ef, namespace | HNSW, M/ef | 기본 기능 존재 여부보다 update 후 품질과 동일 조건 측정이 남는다. |
| 쿼리 벡터 검색 | `<=>`는 기본 exact, `ApproximateVector`로 ANN 선택 | 벡터 query operator가 HNSW search 호출 | 동일 쿼리 문자열의 지연·정확도 비교는 검색 모드를 먼저 맞춰야 한다. |
| 전문 검색 | 기본 Frequency. 직접 검색 API에서 BM25/English Porter 선택 | BM25 기반 FTS, stemming/fuzzy 지원 | Go의 `@@`는 기존 의미를 유지한다. BM25 지원과 query 기본 순위는 별개다. |
| FTS 범위 | global 수동 corpus와 이름 있는 node/edge scoped index 제공 | 종류·scope·property에 선언한 index를 직접 검색 | 호출자가 검색 범위를 명시해야 한다. 서로 다른 API 설계다. |
| 온라인 백업 | 고정 generation snapshot 중 writer 진행 가능. mmap 성장 시 보수적으로 쓰기 거절 가능 | 활성 transaction이 있으면 backup 거절, 복사 중 배타적 사용 요구 | Go가 동시 백업에 다른 운영 계약을 제공한다. |
| 연속 백업 | commit별 durable outbox와 archive, 기록된 복구점 복원 | `replicateTo` 호출마다 WAL shipping, 로그 절단 뒤 새 full generation | 양쪽 모두 백업이다. 분산 replica가 아니다. 복구점의 기록 단위가 다르다. |
| 공간 축소 | 살아 있는 페이지를 새 파일로 재작성 | 파일 끝의 연속된 free page만 절단, live page 이동 없음 | 내부 단편화 회수 범위와 I/O 비용이 다르다. |
| 자원 제한 | context, work/논리 bytes, build/recovery 예산 | buffer pool 등 설정, 같은 Go식 per-request 계약은 없음 | Go는 명시적 제한이 강점이나 누락된 내부 loop 검사가 남아 있다. |
| streams/changefeed | durable stream, offset, trim, 알림 | 같은 주요 기능 범주 | 기능 이름의 유사성이 동시성·복원 의미 동일성을 뜻하지 않는다. |

Go 문법은 [engine_conformance.md](../engine_conformance.md), 검색 기본값은 [README](../../README.md), Zig의 백업·공간 회수는 [고정 소스](https://github.com/jeffhajewski/latticedb/blob/f8e778359a54c4a79d7a49210bf75aa152f47820/src/storage/database.zig#L1303)를 근거로 했다. Go는 검색 predicate의 OR/NOT 조합, OPTIONAL MATCH/UNION, 계산 projection의 ORDER BY 등에 제한이 있다. 이를 요청되지 않은 호환성 결함으로 분류하지 않는다.

기존 [feature-goals.md](../feature-goals.md)의 26개 목표 행은 모두 달성으로 기록되어 있다. 이는 **기능 범주가 존재한다는 문서상 판정**이며, 무결점·성능 동등성 또는 전체 문법 일치를 뜻하지 않는다.

## 현재 결함·위험 확인

| 항목 | 이번 확인 결과 | 영향 / 증거 수준 |
|---|---|---|
| [#236 fuzzy Frequency](https://github.com/mrchypark/latticedb-go/issues/236) | 공개 API에서 memory/disk 모두 재현 | ID1=`book books`, ID2=`book book`, query=`book`, distance=1이면 둘 다 score=2로 ID1이 먼저 나온다. 기존 max-frequency 계약은 ID2=2, ID1=1이다. 기본 순위·Limit1 결과가 바뀐다. |
| [#235 HNSW update](https://github.com/mrchypark/latticedb-go/issues/235) | 현재 코드에서 loop 결함 확인 | `reconnectPageVectorNode`가 `meta.MaxLevel`에서 시작하면서 동시에 `level <= node.Level`을 요구한다. 낮은 level 노드는 재연결을 0회 수행한다. vector update/revival 후 ANN 품질 위험. recall 저하량은 측정하지 않았다. |
| [#230 export lock](https://github.com/mrchypark/latticedb-go/issues/230) | 현재 코드에서 충돌 경로 확인 | output `X`의 lock은 `X.lock`. 다른 export가 `X.lock`을 결과 파일로 rename하면 lock inode가 교체된다. 동시 export 직렬화가 깨질 수 있다. 이번 통합 실행 재현은 하지 않았다. |
| [#231 archive frame 한도](https://github.com/mrchypark/latticedb-go/issues/231) | payload와 전체 frame 한도 불일치 확인 | payload는 1 GiB까지 허용하지만 archive는 68-byte header를 포함한 frame을 1 GiB로 제한한다. page commit도 outbox 저장/native commit 이후 `publishPageSegment`에서 거절할 수 있다. 1 GiB 실제 transaction은 실행하지 않았다. |
| [#237 HNSW 예산](https://github.com/mrchypark/latticedb-go/issues/237) | open/commit 계산 경로 차이 확인 | commit은 active namespace 전체 persistent 비용을 합산한다. open의 기존 valid index 검증은 build persistent 비용을 같은 방식으로 누적하지 않는다. 경계 fixture는 미실행. |
| [#238 FTS 작업량](https://github.com/mrchypark/latticedb-go/issues/238) | 현재 indexed scorer 내부 loop 확인 | query-term 빈도 누적과 DF matrix 순회에 cell별 work/cancellation 검사가 없다. 논리 예산보다 많은 CPU 작업을 허용할 수 있다. 실제 지연·취소 시간은 미측정. |
| [#247 backup 재해시](https://github.com/mrchypark/latticedb-go/issues/247) | 현재 코드에서 I/O 비용 확인 | `writeArchiveHead`가 이전 head 파일 전체를 다시 hash한다. 이전 head가 큰 full base이면 다음 작은 commit도 큰 읽기를 동반한다. 모든 commit이 언제나 full base를 읽는다는 뜻은 아니다. 무결성 정책 tradeoff이며 corruption 결함으로 단정하지 않는다. |
| [#248 sparse helper panic](https://github.com/mrchypark/latticedb-go/issues/248) | 임시 Go overlay에서 내부 helper 재현 | 65개 ID를 넣고 모두 삭제한 뒤 add하면 index[-1] panic. 기존 1-ID public index 회귀 검사는 이 경계를 검증하지 않는다. 공개 API 도달성은 여전히 미확정. |
| [#233 stream capacity](https://github.com/mrchypark/latticedb-go/issues/233) | 현재 구현은 남은 구간과 MaxBytes로 초기 capacity 제한 | 예전 이슈 설명을 그대로 현재 결함으로 집계하면 안 된다. 다른 스트림 메모리 경계까지 모두 해결됐다는 뜻은 아니다. |

주요 소스: `internal/engine/page_vector.go:873`, `page_fts_index.go:718`, `internal/exporter/exportlock.go:39`, `internal/store/page_commit.go:41`, `internal/engine/page_backup.go:217`, `backup_archive.go:336`, `internal/store/property_index.go:619`, `page_stream.go:216`.

열린 이슈는 umbrella #224 포함 12개였으며, 나머지 #245/#246은 벤치마크 보고서 출력 도구에 관한 것이다. 이슈 상태만으로 미해결 결함 수나 심각도를 산정하지 않았다. 이슈 변경·종료와 제품 수정은 수행하지 않았다.

## 성능 근거와 한계

[현재 SHA의 CI benchmark](https://github.com/mrchypark/latticedb-go/actions/runs/37216255638) artifact를 읽었다. 100K/128D clustered HNSW 표는 Go 평균 0.395 ms, Zig 1.033 ms, recall@10은 양쪽 99%다. Go build 165.524초, Zig 1202.717초다. **Go는 resident index이고 Zig는 저장 계층을 포함하며, Zig 값은 이전 artifact 재사용이다. 이 수치로 디스크 Go가 더 빠르다고 결론 내리지 않는다.**

[SciFact 기존 실측](../benchmarks/fts-scifact-2026-10-04/REPORT.md)은 5,183개 문서·300개 질의의 공통 BM25 조건에서 top-10 순서가 모두 일치했다. nDCG@10은 양쪽 0.6607이다. Go 원문 Frequency 0.0959, BM25 0.6617, BM25+Porter 0.6889였다. 이는 기본 검색 선택이 중요하다는 근거다. 과거 리비전, 영어 과학 문서 한 dataset이며 최신 main의 재측정은 아니다. Go public DB와 Zig FTS component의 cache/API 경계도 다르다.

이번에는 Go 공개 디스크 API의 `BenchmarkReadRequests`를 1회, 100ms로 실행했다. 1,000개 노드 fixture에서 node lookup 1.689µs, query 2.719µs, vector search 1.680ms, FTS 0.353ms, write commit 5.853ms였다. Apple M3/macOS arm64, 동시 시스템 부하 통제 없는 **실행 smoke 측정**이다. zero-vector fixture이며 비교용 ANN 품질 자료가 아니다. Zig와 속도 배수로 비교하지 않는다.

필요한 최소 후속 비교는 두 public DB API를 통한 동일 입력·검색 모드·M/ef/K·transaction batch·durability 설정의 디스크 실행이다. 새 DB 생성, commit, close/reopen 검증 후 build/search/update/read의 비용을 분리한다. 동일한 exact ground truth로 recall을 계산하고, 새 실행에서 양쪽을 측정한다. warm/cold 상태와 cache/RSS/file size를 명시한다. 기존 메모리 microbenchmark는 별도 표로 유지할 수 있다.

## 이번 검증

실행 환경: macOS arm64, Apple M3, Go 1.27.1, Zig 0.16.0. 제품 소스 변경 없이 실행했다.

| 검사 | 결과 |
|---|---|
| Go `go test ./...` | 통과, 9 packages |
| Go `go test -race ./...` | 통과, 9 packages |
| Go `go vet ./...` | 통과 |
| conformance module `go test -race ./...` | 통과 |
| vector reader/rebuild/cancellation race ×20 | 통과 |
| exporter subprocess/lock-registry race ×20 | 통과 |
| parser + state/WAL/nested-value/binary-payload fuzz | 5개 대상 각각 5초, 1 worker, 통과 |
| Linux/Windows/Solaris/AIX/Plan9/js-wasm/WASI test binary 교차 컴파일 | 통과. 실행 검증과 구분 |
| Python FTS / Zig config / archived provenance | 8+1+1 tests 통과 |
| Zig `zig build test integration-test crash-test -j2 --summary all` | exit 0, 21/21 steps 성공, 1089 pass / 5 skip |
| Go 공개 디스크 benchmark smoke | 통과 |
| #236 / #248 별도 probe | 알려진 문제 재현. 제품 회귀 테스트의 통과를 뜻하지 않음 |

Zig의 5개 skip은 transaction 테스트의 미래 MVCC/read-committed/concurrent-writer/savepoint stub이다. 다른 transaction-aware read/search 테스트는 실행되므로, skip만으로 Zig의 트랜잭션 전체가 미구현이라고 해석하지 않는다.

원격에서도 같은 Go SHA의 [test workflow](https://github.com/mrchypark/latticedb-go/actions/runs/37216255640)는 Linux/macOS/Windows 세 job 성공, benchmark workflow도 성공이었다. 이번 로컬 실행과 기존 원격 결과를 구분했다.

## 후속 우선순위

1. export lock #230과 commit 후 archive 실패 #231의 공개 경로를 고정 재현하고 해결한다. 기본 저장소 손상이 입증된 것과 export/backup 계약 위험은 구분한다.
2. 사용자 결과에 직접 영향을 주는 fuzzy 순위 #236, HNSW update #235를 고정 기대값 테스트와 함께 해결한다. 예산 계약 #237/#238, backup head 재해시 #247도 정리한다. #248은 public reachability를 분리해서 조사한다.
3. 저장 엔진 재구현 대신 **Zig 비교 표를 public disk backend 기준으로 보완**한다. 현재 성능 우위 표현에는 비교 범위를 함께 표시한다. 동일한 M/ef뿐 아니라 실측 recall도 맞춘다. 새 프로세스 reopen은 OS cold cache와 다르다.
4. 실제 검색 데이터와 제한 메모리 조건에서 page HNSW/FTS를 측정한다. CLI·다언어 바인딩은 제품 수요가 있을 때만 추가한다.

[Pro 독립 검토](https://chatgpt.com/g/g-p-69ecdc42175c819186cf485b225c0e46-codex-request/c/6ace026a-3228-83e8-a519-b023fb6be945)는 제공된 근거 요약을 검토했다. 저장 계층 재설계 대신 정확성·예산 계약을 먼저 확인하고, public persistent API로 동시점 비교를 수행하라는 판단을 반영했다. Pro는 소스 전수 검토나 실행 검증을 하지 않았다. 답변의 잠정 P0 분류는 공개 도달성·기본 저장소 손상 증거가 부족하여 채택하지 않았다. 전달 당시의 27개 목표 표기는 실제 26개로, #233 미해결 가정은 현재 소스의 capacity 제한으로 정정했다. 최종 판단은 위 로컬 근거를 우선한다.

전체 점검은 저장·쿼리·검색·복구·내보내기·배포/검증 경로를 대상으로 한 구조 검토, 알려진 이슈 재확인, 실행 검증이다. 모든 소스 줄의 독립 전수 보안 감사는 아니다. 신규 동일 조건 대규모 벤치마크, 물리 전원 차단, 장시간 soak, Zig 언어별 바인딩 실행은 하지 않았다.
