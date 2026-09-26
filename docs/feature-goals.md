# 기능 목표와 달성 기준

## 프로젝트 목표

LatticeDB Go는 외부 DB 서버나 cgo 없이 Go 애플리케이션에서 그래프·벡터·텍스트 데이터를 함께 저장하고 조회하는 임베디드 데이터베이스다. 트랜잭션, 내구성, 제한 가능한 조회 비용, 증분 백업과 복구, 디스크 페이징을 통한 RAM보다 큰 데이터 처리를 목표로 한다. 공개 `Open`은 page backend를 사용하지만 전체 페이징 완료 게이트는 아직 통과하지 않았다.

2026-09-26에 정한 구현 우선순위는 **증분 백업·재개 → 디스크 페이징 → 검색 확장**이다. 디스크 페이징은 이제 비목표가 아니라 필수 기능 목표다. 메모리 한도 상향, 전체 디코딩 후 mmap 적용, 속성만 디스크로 이동하는 구현은 이 목표의 완료로 인정하지 않는다.

**Zig LatticeDB는 기능 목표의 기준이며 호환성 목표가 아니다.** 같은 사용자 작업을 달성하면 Go의 API, 파일 배치, 검색 점수, 실행 계획이 달라도 기능을 달성한 것으로 본다. Zig의 버그나 구현 제약을 복제하지 않는다. 기존 Go 사용자가 의존하는 동작은 [Go 엔진 계약](engine_conformance.md)으로 관리한다.

기준 기능 목록은 [Zig README의 고정 리비전 `827891e`](https://github.com/jeffhajewski/latticedb/blob/827891e2c6fd55d13aa8f8284a7c7043f68b60fd/README.md#features)과 해당 리비전의 쿼리 parser/expression/aggregate 구현이다. 로컬 비교의 기준점은 Go `v0.9.0` (`bf00f8a`)이다. 이 문서는 작업 트리의 기능을 평가하며, 아직 태그로 배포되지 않은 기능을 릴리스에 포함됐다고 뜻하지 않는다.

## 기능별 판정

| 기능 목표 | v0.9.0 기준 | 이번 작업 / 확인 근거 |
|---|---|---|
| 노드·방향 관계·다중 라벨·중첩 속성 | 달성 | 공개 API 및 `conformance/go`의 값·ID·라벨 회귀 검사 |
| 노드·관계 속성 equality 인덱스 | 달성 | page postings의 생성·갱신·삭제·재오픈·롤백 검사 및 256 MiB 제한에서 10만 노드 인덱스 구축·복원 검증 |
| 고정 길이 그래프 탐색 | 달성 | outgoing/incoming/undirected와 여러 경로 조합 |
| 가변 길이 경로 | 미달 | `query_variable_path_test.go`: 범위·방향·0홉·cycle·자원 제한 |
| 단일 writer 트랜잭션, commit/rollback, crash recovery | 달성 | bbolt 원자 commit·재오픈, 미커밋 변경을 남긴 프로세스 종료, snapshot 보존 및 쓰기 거절 후 재시도 검사 |
| 기본 조회·필터·정렬·페이지 처리·파라미터 | 달성 | 기존 grammar/conformance 검사; 깊은 ORDER BY 비교 예산 결함 수정 |
| WITH·UNWIND | 달성 | 새 표현식·MERGE·경로와 조합하는 회귀 검사 추가 |
| 산술식과 일반/독립 RETURN | 미달 | `query_arithmetic_test.go`: 우선순위·괄호·오버플로·0 나눗셈·원자성 |
| count/sum/avg/min/max/collect | 달성 | 기존 Go의 NULL·타입 처리 규칙 유지 |
| 집계 내부 DISTINCT | 미달 | `query_aggregate_distinct_test.go`: 그룹별 중복 제거·중첩 값·메모리 예산 |
| MERGE 및 ON CREATE/ON MATCH SET | 미달 | `query_merge_test.go`: 전체 패턴·중복 입력·조건부 수정·후속 오류 롤백·재오픈 |
| HNSW 근사 벡터 검색과 ef 설정 | 메모리 엔진에서 달성 | 기존 exact/ANN 계약은 유지; page backend의 HNSW 저장·재오픈은 미구현이며 exact 경로는 페이지를 스캔 |
| HNSW M 설정 | 메모리 엔진에서 달성 | `OpenOptions.VectorM`과 namespace/cache/build 예산 및 공개 API 검사; page backend에 HNSW 인덱스는 아직 없음 |
| hash/Ollama/OpenAI 호환 임베딩 | 달성 | `embedding` 패키지; 외부 HTTP 클라이언트는 선택 사항 |
| 벡터 노드 일괄 삽입 | 달성 | `Tx.BatchInsertVectors` |
| 전문 검색과 fuzzy Levenshtein 검색 | 달성 | direct FTS, property-scoped `@@`, 인덱스/스캔 비교 검사 |
| BM25 순위와 English stemming | 미달 | `FTSSearchOptions.Scoring` / `Analyzer`; 순위·어간·fuzzy·재오픈·예산 검사. 기존 frequency 점수는 기본값으로 유지 |
| DB를 닫지 않는 온라인 백업 | 달성 | `BeginSnapshot` / `Snapshot.Backup`, writer와 동시 사용 검사 |
| 연속 백업 및 시점 복구 | 달성, 기록된 복구점 기준 | 기준 체크포인트와 커밋별 증분, durable outbox 재개, source history 검증 및 보존. 백업 비활성화 중의 미기록 커밋은 복원하지 않으며 중단 구간 선택을 명시적으로 거절 |
| bytes 직렬화·역직렬화·메모리 DB | 달성 | `Serialize` / `Deserialize` / `:memory:` |
| durable stream·consumer offset·trim·changefeed | 달성 | 스트림·복구·트림·알림 회귀 검사 |
| 삭제 후 공간 재사용 | 달성, 파일 축소는 미구현 | bbolt freelist가 해제된 페이지를 이후 쓰기에 재사용한다. page backend의 `Checkpoint`는 sync하며 파일을 축소하지 않는다. |
| 서버 없이 열기·단일 writer 로컬 운영 | 달성 | native file lock, context-aware writer acquisition, 복구 후 reopen |
| 디스크 페이징으로 RAM보다 큰 DB 처리 | 달성, 명시한 범위 검증 | 공개 `Open`의 기본 엔진. 256 MiB에서 1.42 GB DB의 생성·인덱스·재오픈·수정·삭제·백업·migration·증분 복원 통과. 개별 트랜잭션과 metadata/catalog는 RAM에 들어가야 함. [검증 범위](disk-storage.md) |

기존 쿼리 확장과 안전성 수정은 구현했다. page backend가 공개 기본 저장 경로이며, 아래 제한 메모리 시나리오를 통과했다. HNSW 페이지화와 검색 확장은 후속 작업이다.

새 쿼리 기능의 공개 API 조합 검사는 루트의 `TestQueryFeatureGoalsTogether`가 담당한다. 개별 기능이 동작하는 것과 함께, WITH 경계·중첩 관계 반환·조건부 수정·집계가 연결되는지도 검증한다.

## 명시적 의미와 경계

- 전체 openCypher/Neo4j 문법, OPTIONAL MATCH, UNION, CALL은 이번 기준의 필수 목표로 추가하지 않는다. Zig도 전체 Cypher를 제공하지 않는다. 지원 문법은 [정식 grammar](../internal/engine/testdata/query_grammar.ebnf)로 고정한다.
- MERGE는 하나의 연결된 고정 길이 패턴에 적용한다. 전체 매치가 없으면 기존에 바인딩된 개체만 재사용하고 나머지를 만든다. NULL identity 속성을 거부하며, 문장 전체는 실패 시 롤백된다.
- 가변 길이 관계 변수는 관계 목록이다. 한 번의 가변 길이 확장 안에서는 같은 관계를 재사용하지 않지만 노드 재방문은 허용한다. 여러 MATCH 패턴 사이의 관계 중복 여부는 기존 Go 매칭 계약을 유지한다.
- 검색 방식 선택과 정확한 점수는 Go 계약으로 명시한다. BM25나 stemming 추가를 이유로 기존 frequency 검색 결과를 몰래 바꾸지 않는다.
- 연속 아카이브는 기준 체크포인트 이후 성공한 커밋의 WAL 변경분을 보관한다. 동일 상태에서 재개하면 기존 복구점을 유지하고, 백업 중단 중 소스가 앞서간 경우 독립 기준점을 추가한다. 중단 구간의 누락 커밋과 실제 과거 트랜잭션 시각을 재구성하지 않는다. WAL에 영속적인 이력 해시와 커밋 시각을 추가하기 전에는 미기록 구간의 역사적 catch-up을 달성했다고 간주하지 않는다.
- Zig의 C ABI와 다른 언어 바인딩, 동일한 바이너리 파일, 동일한 내부 페이지 배치는 Go 라이브러리의 기능 목표에 포함하지 않는다.
- RAM보다 큰 데이터의 디스크 페이징은 증분 백업 다음에 달성할 필수 목표다. 레코드뿐 아니라 인덱스, 복구, 체크포인트와 스냅샷도 전체 DB를 메모리에 적재하지 않아야 한다. 다중 writer 동시 실행과 분산 복제는 여전히 비목표다. 논리적 메모리 예산은 RSS 수치가 아니며, 페이징 완료 판정에는 제한된 메모리 환경의 실측이 필요하다.

## 이전 검토 결함의 처리

| 검토 항목 | 처리 |
|---|---|
| R1: WAL 길이 헤더 손상으로 마지막 커밋이 조용히 누락됨 | v5 헤더 CRC를 길이 사용 전에 검사; v2/v3/v4 읽기·안전한 재기록 지원. legacy 포맷의 정보 부족과 실제 파일 절단의 검출 한계를 명시 |
| R2: ORDER BY 깊은 비교가 work/context/임시 bytes를 우회함 | 전체 정렬·Top-K·WITH·집계 정렬에서 예산을 전파; 맵 키 버퍼를 예약하고 오류를 반환 |
| R3: 설치 버전·파일 형식·namespace/ANN 문서 불일치 | README·패키지 설명·저장/벡터 문서 정정, 미배포 기능과 v0.9.0 구분 |
| R4: nil/zero Tx의 panic | 공유 검사를 통해 구조화된 ErrInactiveTx 반환; Rollback은 idempotent 유지 |

성능 수치와 과거 벤치마크 문서는 당시 리비전의 기록으로 유지한다. 기능 달성은 과거 벤치마크 수치가 그대로 유지된다는 뜻이 아니다.


## 이전 후보의 검증

증분 백업·페이징 작업 이전 후보(`5cdcf2b`)는 2026-09-26, macOS arm64 / Go 1.27.1에서 다음 검사를 통과했다. 아래 결과가 이후 변경분까지 검증했다는 뜻은 아니다.

- 루트 `go test ./...`, `go test -race ./...`, `go vet ./...`
- `conformance/go`의 `go test -race ./...`
- 벡터 reader/rebuild/cancellation 및 CSV subprocess publication/lock registry의 race 반복 검사 각각 20회
- 파서·state decode·WAL recovery·nested value round-trip 퍼징 각각 5초, 1 worker. 파서 seed에 새 문법을 추가했다.
- 백업의 공개 API/내부 회귀 검사와 race 검사: 커밋 0, 시각 하한, 재오픈과 롤백 ID 예약, 손상된 시각/내용, no-overwrite, archive-only restore 후 쓰기와 재오픈, WAL 이후 백업 실패의 fencing/복구
- Linux/Windows/Solaris/AIX/Plan 9/js-wasm/WASI 교차 컴파일
- 기존 벤치마크 provenance Python 검사 2개와 `git diff --check`

기존 회귀 검사 중 WAL payload 검사기는 v5의 68바이트 헤더를 사용하도록 갱신했다. 정렬 비교 비용을 새로 부과하면서 두 optimizer 검사의 예산을 실제 측정한 484/807 work에 맞춰 500/850으로 조정했고, 비최적 source-order 경로가 낮은 예산에서 실패한다는 대조 검사는 유지했다.

Linux/Windows에서의 실제 실행, 물리 전원 차단, 장시간 soak와 신규 기능의 대규모 성능 측정은 수행하지 않았다. 기능 검증과 과거 성능 수치의 재보증을 구분한다. 이 결과는 미배포 작업 트리에 해당하며, 이후 page backend 변경을 검증하지 않는다.

## 증분 백업 작업 검증 (page backend 이전 후보)

2026-09-26, macOS arm64 / Go 1.27.1에서 증분 백업 변경분에 대해 루트 `go test ./...`, `go test -race ./...`, `go vet ./...`, conformance 모듈의 `go test -race ./...`를 통과했다. CI에 지정된 벡터 동시성 및 CSV publication race 검사 각각 20회와 Linux/Windows/Solaris/AIX/Plan 9/js-wasm/WASI 교차 컴파일도 통과했다.

공개 API 회귀 검사에서 1MiB 문서의 숫자 속성 하나와 스트림 이벤트 하나를 변경할 때 추가 아카이브 크기가 기존 1,049,142바이트에서 296바이트로 줄었다. 백업 비활성화 중 체크포인트를 수행한 뒤 같은 아카이브를 재개하고 이전·새 복구점을 모두 복원했다. 중단 구간 시각의 선택 거부, 복원 대상 잠금, 최종 publication 중 취소, 손상 및 WAL 이후 실패의 recovery fencing도 검사했다.

체크포인트의 스트림 순차 기록은 기존 바이너리 표현과 일치한다. v5 순차 스캐너는 레코드별 예산, checksum, truncation, callback 오류와 취소 검사를 통과했으며 백업 기준점 검증에 사용한다.

## Page backend 제한 메모리 검증

2026-09-27, swap 없는 256 MiB cgroup에서 1,418,768,384바이트 DB로 `TestPageDBMemoryCheck`가 298.27초에 통과했다. 작은 노드 100,000개, 관계 99,999개, 큰 속성 노드 1,024개를 사용했다. 속성 인덱스 생성·재오픈·무작위 조회·수정·삭제·snapshot backup·대용량 체크포인트 migration·증분 archive 복원과 복원된 인덱스/삭제 결과까지 확인했다.

전체 일반/race 검사와 conformance 검사를 통과했다. 개별 트랜잭션·delta frame·애플리케이션 metadata 및 schema/stream catalog의 메모리 한계, snapshot 유지 중 보수적인 쓰기 거절 등은 [저장 엔진 계약](disk-storage.md)에 명시한다.
