# LatticeDB Go 전체 프로젝트 검토 — 2026-09-26

검토 기준: `bf00f8a711d8615988e008c9d167f9a3a6edd00f` (`v0.9.0`). `origin/main`을 fetch했고 로컬 HEAD와 같았다. 시작 시 작업 트리는 깨끗했다. 최초 검토 시 구현은 수정하지 않았으며 이 보고서만 추가했다. 이후 사용자가 기능 목표 달성과 결함 수정을 요청했으므로, 아래 결함·수치는 수정 전 기준이고 현재 달성 상태는 [기능 목표표](../feature-goals.md)를 따른다.

## 판단과 프로젝트 목표

**목표는 “외부 런타임과 cgo 없이 Go 애플리케이션 안에서, 메모리에 수용 가능한 그래프를 트랜잭션으로 안전하게 저장하고 제한된 Cypher·전문 검색·벡터 검색으로 조회하는 임베디드 데이터베이스”로 명확히 하는 것이 적절하다.**

현재 구현은 초기 프로토타입 수준을 넘어섰다. 단일 writer와 immutable generation/COW, WAL 동기화 후 공개, 체크포인트, 복구, 인덱스, 온라인 스냅샷, 내보내기, stream/changefeed가 실제로 연결되어 있다. 유지할 기반이 충분하며 전면 재작성할 이유는 발견하지 못했다. 다만 아래 재현된 결함 때문에 “테스트가 통과하므로 자원 제한과 손상 검출까지 안전하다”는 결론은 내릴 수 없다.

현재 비목표는 전체 openCypher/Neo4j 호환, RAM보다 큰 데이터를 위한 디스크 페이징, 동시 다중 writer 실행, 분산 DB, 프로세스 RSS의 엄격한 상한이다. 사용자 확인에 따라 Zig는 기능 목표이며 호환성 목표가 아니다. 따라서 `MERGE`, 산술식, 집계 `DISTINCT`, 가변 길이 경로는 결함과 구분되는 기능 미달 항목으로 다루고 이번 후속 구현에 포함한다.

## 확인된 문제

### R1 · P1: WAL 길이 헤더 손상이 커밋 손실로 조용히 처리된다

위치: `internal/store/recovery.go:2222`, `:2230`, `:3848`.

WAL CRC는 payload에만 적용된다. 복구는 검증되지 않은 헤더의 payload 길이를 읽고, 남은 파일 크기보다 크면 정상적인 미완성 꼬리로 간주하여 이전 커밋을 오류 없이 반환한다. 따라서 실제로 완전하게 기록된 프레임의 길이 헤더 한 비트가 바뀌어도 이미 커밋된 데이터가 누락될 수 있다.

독립 검토와 통합 검토에서 각각 재현했다. 정상 체크포인트/기본 WAL(commit 0) 뒤 노드 생성 delta(commit 1)를 기록했고, 손상 전에는 commit 1과 노드 존재를 확인했다. 마지막 프레임 길이만 **68 → 69**로 바꾼 뒤:

```text
corruption silently accepted: length=68->69 commit=0 node=false
```

손상 오류를 기대하는 임시 회귀 검사는 실패했다. 이는 **저장 파일 손상 조건**의 문제이며, 정상 파일에 대한 일반적인 강제 종료가 항상 데이터를 잃는다는 뜻은 아니다.

수정 방향: 새 WAL 버전에서 길이를 포함한 헤더 무결성을 먼저 검증할 수 있도록 framing을 보강한다. 기존 버전의 읽기 정책을 명시하고, 실제 잘린 꼬리 허용과 완전한 프레임의 헤더 손상 거부를 함께 검증한다. 단순히 모든 EOF를 오류로 바꾸면 기존 torn-tail 복구 계약을 깨뜨린다. 기존 포맷은 이 두 상황을 확실히 구분할 정보가 없다.

### R2 · P2: 복합 값 정렬이 내부 비교 작업량과 임시 메모리를 계상하지 않는다

위치: `internal/engine/query_sort.go:20`, `internal/engine/query.go:4085`, `:4106`, `:4173`, `:662`.

정렬은 행 비교 호출마다 work 1을 계산하지만, 실제 비교 함수는 벡터 전체를 `slices.Compare`, 목록을 재귀 비교하며 내부에 budget/context 검사가 없다. 맵 비교에서는 양쪽 키 슬라이스를 새로 만들고 정렬하지만 임시 메모리도 계상하지 않는다. Top-K 경로도 같은 비교 함수를 사용한다. 기존 equality 비교에는 청크 단위 검사가 있어 정렬 경로와 차이가 난다.

공개 API로 길이 1,048,576인 벡터 두 개를 만들고 마지막 원소만 다르게 했다. `MaxWork: 33000`, `MaxBytes: 64 << 20`으로 다음 두 쿼리가 모두 성공했다.

```text
UNWIND $values AS value RETURN value ORDER BY value: rows=2 err=<nil>
UNWIND $values AS value RETURN value ORDER BY value LIMIT 1: rows=1 err=<nil>
```

정규화 후 수행되는 약 100만 원소의 사전식 비교가 예산에 반영되지 않는다. 큰 단일 비교 중에는 취소를 관찰하지 못한다. 맵 임시 메모리 누락은 코드로 확인했으며 별도 RSS/OOM 또는 취소 지연 수치는 측정하지 않았다.

수정 방향: 공유 정렬 값 비교 경로에 예산/취소를 전달하여 문자열·bytes·벡터를 청크 단위로, 목록/맵을 요소 단위로 확인하고 맵 키 버퍼를 계상한다. 전체 정렬과 Top-K를 함께 회귀 검증한다. 단순 scalar 정렬까지 새로운 실행 프레임워크로 바꿀 필요는 없다.

### R3 · P2: 배포·저장 형식·검색 기능 문서가 현재 코드와 다르다

| 위치 | 현재 설명 | 확인된 실제 상태 / 영향 |
|---|---|---|
| `README.md:10` | 설치 버전 `v0.6.0` | 검토 HEAD는 `v0.9.0`. 같은 README가 설명하는 WITH·확장 집계·list literal 등의 기능을 설치 버전에서 기대하게 한다. `git show v0.6.0:internal/engine/query.go`와 현재 코드를 대조했다. |
| `README.md:123` | state v4 / WAL v3 | 실제 상수는 state v5 / WAL v4 (`internal/store/recovery.go:109`). 업그레이드/다운그레이드 판단에 영향을 준다. `docs/binary-storage.md`는 현재 형식과 일치한다. |
| `README.md:135` | direct vector search는 property selector가 없는 global index | 같은 문서 104행과 실제 API는 명시적 namespace를 지원한다. nil namespace의 legacy 경계로 한정해야 한다. 다중 벡터 속성 설명도 vector-enabled 노드당 하나라는 값 계약과 맞춰야 한다. |
| `docs/vector-namespaces.md:52` | query comparison은 exact | 기본값은 exact지만 eligible query에는 `ApproximateVector` opt-in이 존재한다. |
| `doc.go:1` | 앞으로 엔진을 구현할 bootstrap 프로젝트 | 현재 패키지 상태를 잘못 설명한다. 이 항목 자체는 P3 문서 품질 문제다. |

수정 방향: 한 번에 현 버전의 설치·파일 형식·기본 동작·opt-in 경계를 정리한다. 과거 벤치마크 보고서는 당시 버전의 기록이므로 현재 결과로 덮어쓰지 않는다.

### R4 · P3: 빈 Tx의 공개 메서드 동작이 일관되지 않다

위치: `db.go:697`와 이후 주요 Tx wrapper; 비교 대상 `db.go:672`, `compat.go:28`.

```go
var tx latticedb.Tx
tx.Commit()                                // ErrInactiveTx
tx.CreateNode(latticedb.CreateNodeOptions{}) // nil pointer panic
```

공개 API로 재현했다. `DeleteNode`, `GetProperty` 등에도 같은 무검사 역참조 패턴이 있다. 정상 `Begin`으로 얻은 Tx의 일반 경로 결함은 아니며, zero/nil Tx가 모든 메서드에서 지원된다는 명시적 계약도 확인하지 못했으므로 API 방어성/일관성 문제로 낮게 분류한다. nil/zero DB와 일부 compatibility Tx 메서드는 이미 검사한다. 지원 정책을 명시하거나 기존 `ErrInactiveTx` 처리로 통일하는 작은 변경이 적절하다.

## 잘 구현된 부분과 현재 경계

| 영역 | 확인한 강점 | 남는 경계 |
|---|---|---|
| 저장·트랜잭션 | WAL sync 이후 graph publication, 불확실한 커밋의 recovery-required fencing, 단조 ID, statement rollback | R1의 헤더 손상 검출 |
| 세대·수명 | COW, read/snapshot/export lease, 세대 보유량 통계와 선택적 admission 제한 | canonical bytes는 RSS가 아니며 기본 lease 제한은 무제한 |
| 검색 | property index, FTS 후보 경로와 기준 스캔 비교 테스트, exact 기본값, namespace/HNSW/cache | ANN 후보 사용은 좁은 query shape에 한정. namespace/FTSProperties는 재오픈 시 다시 제공 |
| 운영 API | context 지원, group commit, stream별 알림, 온라인 스냅샷, CSV reader lease와 pruning | Batch peer 실패는 그룹 rollback, Close의 최종 teardown은 비취소 등 명시된 운영 계약 |
| 검증 | crash/recovery, 병렬성, 자원 제한, fuzz 대상, 3 OS CI 정의, 벤치마크 provenance 검사 | 실제 전원 장애와 대규모 혼합 부하는 이번 실행 범위 밖 |

2026-09-04 리뷰의 바이너리 저장·검색 후보·백그라운드 벡터 재구축·다중 스냅샷·CSV lease·canonical query cache 제안은 이미 코드에 반영되어 있다. 이를 미구현 항목으로 재사용하면 안 된다.

## 부족한 검증과 개선 우선순위

### Zig 기능 목표와 Go 계약을 분리해야 한다

최초 명세는 두 엔진의 관찰 가능한 동등성을 요구했지만, 사용자가 목표를 기능 달성으로 명확히 했다. 이 동등성 요구는 잘못된 목표 설명이므로 수정한다. Go conformance suite는 Go의 공개 동작 계약을 검증하는 용도로 유지하고, Zig 원본은 기능 목록의 근거로 사용한다. 동일 값·타입·순서·오류나 동일 파일 배치를 강제하는 양방향 호환 harness는 완료 조건이 아니다.

### 작은 집계 결과에도 중간 행 비용이 크다

현재 후보에서 기존 `BenchmarkQueryLanguage`를 실행했다. Apple M3 / darwin arm64 / Go 1.27.1 / GOMAXPROCS=2, 100K 노드, warm query, `-benchtime=1x -count=3`. 중앙값:

| 쿼리 | 반환 행 | 시간 | 누적 할당량/호출 | 할당 횟수/호출 |
|---|---:|---:|---:|---:|
| `count(*)` | 1 | 10.10 ms | 47.718 MiB | 100,070 |
| `WITH` 그룹 집계 + 상위 10개 | 10 | 48.47 ms | 68.357 MiB | 1,200,620 |

`internal/engine/query.go:1765`에서 집계 전에 행을 수집하며, `aggregateWithRows`는 그룹 키를 행마다 구성한다. 일반 projection streaming과 일부 ORDER BY Top-K는 이미 존재한다. 우선 개선 대상은 단순 count 및 집계의 중간 행 materialization/그룹 키 할당이다. 통째로 query executor를 재작성할 근거는 없다.

이는 3회의 작은 표본이며 SLA나 처리량 수치가 아니다. B/op는 누적 할당량으로 peak RSS가 아니고, 64 MiB 논리 예산과 직접 비교하여 예산 위반이라고 판정하면 안 된다.

### 운영 가능한 크기를 수치로 정할 필요가 있다

512 MiB canonical snapshot 기본 제한은 실제 프로세스 메모리나 권장 데이터 규모가 아니다. 장기 reader/snapshot, 지속 쓰기, checkpoint, HNSW rebuild가 겹치는 상황의 peak RSS·GC·writer 지연·lease 해제 후 회수를 함께 측정해야 한다. 기존 통계와 벤치마크를 확장하면 되며 새 관측 플랫폼을 도입할 필요는 없다.

실제 문서/한국어 검색 품질, 768/1536차원 벡터, 더 큰 데이터 및 혼합 부하의 결과는 이번 검토로 확정하지 않았다. 과거 보고서의 synthetic recall과 warm latency를 일반 서비스 성능으로 확대 해석하지 않는다.

실행 순서는 **R1 손상 검출 → R2 자원 제한 → 문서 정합성 → count/집계 메모리 개선 및 혼합 부하 측정 → 사용자 요청으로 확정된 미달 기능 구현**이 적절하다. R4는 작은 API 정리로 병행할 수 있다.

## 수행한 검증과 한계

저장소에는 production Go 파일 82개/31,855행과 test Go 파일 170개/36,845행이 있다. 공개 API, query/parser/functions/aggregates, store/recovery/codec, indexes/search, stream, exporter/leases, embedding, conformance, 문서와 CI를 영역별로 검토했다. 모든 파일의 모든 분기·플랫폼을 실행하거나 결함 부재를 증명한 것은 아니다. 독립 검토 결과의 주요 결론과 재현은 통합 단계에서 다시 확인했다.

통과:

- `go test ./...`
- `go test -race ./...`
- `conformance/go`에서 `go test -race ./...`
- `go vet ./...`
- `python3 scripts/test_zig_benchmark_config.py`
- `python3 scripts/test_archived_benchmark.py`
- `FuzzParseQuery`, `FuzzDeserializeGraphState`, `FuzzLoadLatestWALFrames`, `FuzzNestedValueRoundTrip`: 각각 5초, 1 worker
- 위 100K count/WITH의 기존 benchmark와 기대 결과 검사

추가 검증에서는 R1의 corruption-rejection 임시 테스트가 예상대로 실패했고, R2의 예산 부족 정렬이 성공하는 현상과 R4 panic을 각각 재현했다. 임시 소스는 모두 제거했다. 기존 테스트 통과와 신규 결함 재현 실패는 모순이 아니라 현재 회귀 커버리지의 빈틈이다.

이번 실제 실행 환경은 macOS arm64다. Linux/Windows 실행, 전원 차단, 전체 장시간 benchmark CI, Zig 양방향 의미론 비교 및 대규모 soak는 수행하지 않았다.

독립 Pro 의견은 목표·비목표와 검토 우선순위에 한정해 받았다. 전체 소스 검토나 새 결함의 검증을 Pro에게 귀속하지 않는다.


## 후속 구현 결과

사용자의 “Zig는 호환성이 아닌 기능 달성 목표” 지시에 따라 목표를 정정하고 R1–R4를 수정했다. 산술식/일반 RETURN, 집계 DISTINCT, MERGE, 가변 길이 경로, BM25/English Porter, HNSW M 설정, 커밋별 백업과 기록된 시점 복구를 추가했다. 구체적인 API·달성 근거·검증 결과는 [기능 목표표](../feature-goals.md)에 통합했다.

연속 백업은 전체 체크포인트를 동기적으로 보관한다. 증분 전송 효율이나 실제 과거 트랜잭션 시각의 재구성을 주장하지 않는다. 백업 이력이 소스와 맞지 않으면 새 아카이브를 요구하고, WAL 기록 이후 아카이브 실패는 outcome-unknown으로 처리하여 로컬 복구 근거를 보존한다.

추가 독립 Pro 자문은 WAL/MERGE 경계 조건과 백업 안전성의 설계 검토에 사용했고, 제안은 로컬 소스와 회귀 검사로 확인했다. 이후 남은 문법 검토와 백업 검사는 사용자 지정 GPT-6 Luna 서브에이전트를 사용하고 부모 작업에서 다시 검증했다. 자문 자체를 소스 전체의 검증 증명으로 취급하지 않는다.
