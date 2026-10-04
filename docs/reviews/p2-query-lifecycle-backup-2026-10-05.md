# P2 쿼리·종료·백업 수정 검증

기준은 v0.11.2, `e6a5ae03eda1111393a42825ac076324f5a3bd88`이다. 구현 브랜치는 `feature/p2-query-lifecycle-backup-fixes`이다.

요청한 순서의 #232까지 다음 8개 항목을 구현했다.

| 순서 | 이슈 | 변경과 회귀 조건 |
|---|---|---|
| 1 | #240 | WITH DISTINCT의 clone·key 작업 메모리를 중복 제거 및 절 종료 시 반납한다. 성공·예산 초과·취소 후 호출자의 bytes와 sourceBytes를 유지한다. |
| 2 | #242 | RETURN/WITH/count의 true·false·null을 리터럴로 해석한다. backtick binding은 유지한다. 문법 허용·거부 사례와 공개 conformance를 확인한 뒤 감사 digest를 갱신했다. |
| 3 | #241 | abs(MinInt64)를 overflow 오류로 거부한다. CREATE/SET 실패 시 statement 변경을 rollback한다. native int 입력도 검사하며 wasm 테스트를 컴파일했다. |
| 4 | #243 | 쿼리 맵에서 node·edge와 중첩 값을 보존한다. 맵 필드를 반환할 때도 공개 엔티티로 변환한다. DB·Tx·캐시된 계획과 디스크·메모리 백엔드를 검사했다. 별도 공개 Prepare API는 없다. 저장 property의 엔티티 거부를 유지한다. |
| 5 | #244 | 현재 binary state/WAL fixture와 유효한 대조군을 사용한다. 의도한 semantic 오류를 확인하며 임시 overlay에서 검증을 제거하는 대조 실험을 수행했다. |
| 6 | #234 | memory Close에서 graph·cache·maintenance·rebuild 참조를 분리한다. 활성 Tx는 Close를 막고, 취소 worker와 늦은 parse는 닫힌 DB에 결과를 게시하지 못한다. 반복 Close를 유지한다. |
| 7 | #239 | 긴 문자열 함수·검색·비교·min/max에 작업량과 취소 검사를 적용한다. KMP 작업 메모리를 예약·반납한다. replace 출력 조각들은 과금 잔여량을 공유한다. ASCII·byte 동작을 유지한다. |
| 8 | #232 | archive 열기·검증·base 직렬화·CRC·임시 WAL 준비에 context를 전달한다. I/O 청크를 64KiB 이하로 제한하고 취소 시 임시 파일을 정리한다. 개수 계산에도 context를 전달한다. live state 게시 및 source commit 이후 완료는 취소로 중단하지 않는다. |

MaxBytes는 살아 있는 논리적 소유권의 예산이다. RSS 상한을 뜻하지 않는다. Close는 DB가 가진 참조를 제거하며, 취소된 worker가 가진 private graph는 worker 종료까지 살아 있을 수 있다.

## 독립 Pro 검토

[검토 대화](https://chatgpt.com/g/g-p-69ecdc42175c819186cf485b225c0e46-codex-request/c/6ac268b3-bac4-83ee-8533-07833afe3b78)는 종료·문자열·백업 경계를 대상으로 했다. 첫 후보의 맵 필드 변환, private WAL 준비 취소, 반복 치환 과금 지적을 모두 수정했다. 재검토는 이 세 지적의 해소와 live 완료 경계 유지를 확인했으며 추가 수정 요구가 없었다. Pro는 소스와 테스트를 읽었으며 Go 테스트를 실행하지 않았다.

재검토 ZIP SHA256: `bce5f937932477142a756fa339c30437f44fa68b5de35966407cab1c5911bc2a`.
재검토 patch SHA256: `604706f46ea255b2b38d6676447b5f34b2fe0eb2a1646e09719ecaa90ba583f8`.
재검토 이후 공개 DB/Tx 회귀 테스트와 개수 계산의 context 전달을 추가했다. 이 추가분은 로컬 검증 대상이며 Pro 승인 범위로 확대하지 않는다.

## 검증

- 전체 normal 테스트와 parser audit 통과.
- 최종 후보의 전체 race 테스트 통과. 실행 환경은 macOS/arm64이며 원격 CI 결과를 뜻하지 않는다.
- conformance normal/race, Python FTS 도구 8개 테스트 통과.
- Solaris/amd64, AIX/ppc64, Plan9/amd64, js/wasm, wasip1/wasm 교차 컴파일과 engine js/wasm 테스트 컴파일 통과.
- 10K·100K 쿼리 언어 벤치마크의 nested functions와 WITH grouping 실행 성공. 이전 버전과의 성능 비교나 성능 보장은 아니다.
- 복구 mutation 대조 실험에서 snapshot history, delta history, duplicate operation, incident edge, high-water, labels, edge type, property depth 검증 제거를 테스트가 검출했다. orphan FTS는 중복 검증 중 하나만 제거하면 다른 경로가 계속 거부했고, 관련 검증을 함께 제거하면 테스트가 실패했다. 작업 트리의 production guard는 변경하지 않았다.

## PR 준비 점검

| 위험 | 리뷰 확인점 | 로컬 근거 | 처리 | 상태 |
|---|---|---|---|---|
| 쿼리 소유권 | 내부 엔티티 노출 및 DISTINCT 예산 누적 | 공개 DB/Tx, 중첩 Node/Edge, 결과 변경 독립성, 성공·실패·취소 ledger 검사 | 공개 반환 변환 및 작업 메모리 반납 | 수정·검증 완료 |
| 종료 경쟁 | late parse·rebuild의 참조 재게시 | 중단된 rebuild, inactive Tx 유지, Close 재호출, 전체 race | 닫힘 상태 검사와 heavy 참조 분리 | 수정·검증 완료 |
| 백업 취소 | 임시 준비 취소와 live 완료 경계 혼동 | 두 WAL 복사 취소, state rename 이후 private/live 대조, 복구·재시도·cleanup | staging에 context 전달, live 완료는 Background 유지 | 수정·검증 완료 |
| 이식성·fixture | 32비트 정수와 무효 형식으로 우연히 성공하는 오류 검사 | wasm 컴파일, binary 유효 대조군, semantic mutation 실험 | native int overflow 및 현재 형식 fixture | 수정·검증 완료 |

머지 준비에는 PR 최신 후보의 3개 OS test workflow와 100K benchmark workflow 성공, 충돌 부재, 미해결 리뷰 스레드 부재를 확인한다. 로컬 통과 기록으로 원격 게이트를 대체하지 않는다. 최종 원격 결과는 PR에 기록한다.
