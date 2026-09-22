import collections, csv, hashlib, json, pathlib, re, statistics
root = pathlib.Path(__file__).parent
files = ['query.txt','fts.txt','vector-exact.txt','vector-ann.txt','fuzzy-vocabulary.txt']
# This generator describes one archived measurement, not the current checkout.
# Reject changed metadata or logs before touching either generated output.
manifest = json.loads((root/'inputs.sha256.json').read_text())
inputs = files + ['environment.txt', 'grammar-tests.jsonl']
if set(manifest) != set(inputs):
    raise SystemExit('Archived benchmark input manifest is incomplete')
for name in inputs:
    if hashlib.sha256((root/name).read_bytes()).hexdigest() != manifest[name]:
        raise SystemExit(f'Archived benchmark input changed: {name}; refusing to overwrite historical report')
rows = []
for file in files:
    source = (root/file).read_text()
    assert '\nPASS\n' in source and '\nFAIL' not in source, file
    grouped = collections.defaultdict(list)
    for line in source.splitlines():
        if not line.startswith('Benchmark'): continue
        parts = line.split()
        assert len(parts) >= 4, line
        metrics = {'iterations':float(parts[1])}
        for i in range(2,len(parts),2): metrics[parts[i+1]]=float(parts[i])
        grouped[re.sub(r'-\d+$','',parts[0])].append(metrics)
    for name, runs in grouped.items():
        assert len(runs)==3,(name,len(runs))
        row = {'file':file,'benchmark':name,'runs':len(runs)}
        for metric in runs[0]:
            values = [r[metric] for r in runs]
            row[metric] = statistics.median(values)
            row[metric+'_min'] = min(values)
            row[metric+'_max'] = max(values)
        rows.append(row)
keys=['file','benchmark','runs']+sorted(set().union(*(r.keys() for r in rows))-{'file','benchmark','runs'})
with (root/'summary.csv').open('w') as f:
    w=csv.DictWriter(f,keys);w.writeheader();w.writerows(rows)
by_name={r['benchmark']:r for r in rows}
def table(names, metric='ns/op'):
    lines=['| 워크로드 | 시간 중앙값 | 반복 범위 | 할당량/회 | 할당 횟수/회 |','|---|---:|---:|---:|---:|']
    for name in names:
        r=by_name[name]
        lines.append(f"| `{name.removeprefix('Benchmark')}` | {r[metric]/1e6:.4f} ms | {r[metric+'_min']/1e6:.4f}–{r[metric+'_max']/1e6:.4f} ms | {r['B/op']/1024:.2f} KiB | {r['allocs/op']:.0f} |")
    return '\n'.join(lines)
counts=collections.Counter()
for line in (root/'grammar-tests.jsonl').read_text().splitlines():
    event=json.loads(line)
    assert event.get('Action')!='fail',event
    t=event.get('Test','')
    if event.get('Action')=='pass' and t:
        counts['pass']+=1
        if t.startswith('TestQueryGrammarMatrix/'): counts[t.split('/')[1]]+=1
report='''# LatticeDB Go v0.8.0 문법 범위 및 검색 벤치마크

측정일: 2026-09-22. 엔진 기준: `e5faec33e9a8ca2c07f05ba32fc3192bec64235c` (`v0.8.0`). Apple M3 / 24 GiB / macOS arm64 / Go 1.27.1 / GOMAXPROCS=2. 단일 클라이언트가 순차 실행하며, 동시 요청 처리량 측정은 아니다.

## 측정 방법

- 대부분 `-benchtime=200ms -count=3`; 표는 세 실행의 중앙값과 최소–최대다. 작은 표본의 기술 통계이며 신뢰구간은 아니다.
- DB/데이터/인덱스 준비는 검색 타이머 밖, 쿼리 계획 캐시는 워밍업한다. 캐시가 차가운 첫 요청, 디스크에서 읽는 비용, 영속 쓰기 비용은 측정하지 않았다.
- 기본 QueryOptions를 사용했다. 직접 FTS 확장 벤치마크는 기존 하네스대로 MaxWork를 최대로 올린다. B/op는 누적 Go 할당량이며 최대 상주 메모리나 논리 MaxBytes가 아니다.
- 엔진 구현은 수정하지 않았다. 새 WITH/집계/함수 및 동일 데이터의 exact 벡터 측정 하네스를 추가했다. 기존 top-K/다중 홉 하네스의 `defer db.Close()`는 `b.Cleanup`으로 바꿨다. Go의 StopTimer 이후 cleanup이 실행되므로 종료/체크포인트 비용이 검색 결과에 섞이지 않는다. 수정 전 query 결과는 `query-initial-excluded.txt`에 보관하고 아래 집계에서 제외했다.
- ANN은 기존 하네스의 `-benchtime=1x -count=3`. 표의 시간은 단일 ns/op가 아니라 별도 100개 질의의 평균(mean-ns) 중앙값이다. p99도 100개 표본 기반이며 서비스 SLA 수치가 아니다. ANN은 호출 옵션 이름이며 자동 exact fallback 횟수는 이번 하네스에서 별도로 수집하지 않았다. ANN의 B/op는 단일 반복 측정이다.
- 비교는 같은 버전 내 접근 방식 비교다. Zig 원본 또는 Ladybug를 실행한 결과가 아니며, 제품 간 성능 우위를 의미하지 않는다.

## 문법 범위

'''+f"문법 매트릭스: 지원 사례 **{counts['accept']}개**, 의도적 거절 사례 **{counts['reject']}개** 모두 통과. 선택한 문법·함수·집계·WITH 테스트의 pass 이벤트는 총 **{counts['pass']}개**(상위 테스트 포함)다. 문법 계약과 문서, 파서 digest도 일치한다. 이는 자체 회귀 사례의 통과율이며 전체 openCypher 지원률이 아니다.\n\n"+'''| 영역 | 지원 | 주요 제한 |
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

'''
report+=table([r['benchmark'] for r in rows if r['file']=='query.txt' and 'QueryLanguage/' not in r['benchmark']])
report+='''

Top-K와 전체 정렬은 반환량도 다르다(10개 대 전체 노드). 따라서 차이는 정렬 알고리즘뿐 아니라 결과 생성·할당 비용을 포함한다. 다중 홉은 NEXT 체인의 두 홉 결과를 전부 반환한다(100 노드→98행, 10만 노드→99,998행).

## 새 문법의 실행 비용

1만/10만 Item 노드. value=0..N−1, bucket=i%100, name="Sample". count 및 sum/avg는 1행, group_100은 100행, WITH 집계 후 상위 10개는 10행, collect는 N개 원소를 담은 1행이다. nested_functions는 `sum(size(toLower(n.name)))`를 실행한다. 워밍업에서 행 수 및 기대 집계 값도 검증했다.

'''
report+=table([r['benchmark'] for r in rows if 'QueryLanguage/' in r['benchmark']])
report+='''

## 직접 전문 검색: 문서 수와 어휘 크기

아래 scaling 하네스는 대부분 "common token", 한 문서만 "common rare token"이다. 어휘가 3개인 의도적으로 단순한 데이터다. rare는 1건, common은 전체 문서가 매칭하고 기본 Limit 10을 반환한다. multi_rare는 "rare absent"이며 이 엔진의 점수 의미에 따른 검색이다. fuzzy_rare는 "rarf", 편집거리 1이다. 희귀어/fuzzy 숫자를 큰 실제 어휘의 성능으로 일반화하면 안 된다.

'''
report+=table([r['benchmark'] for r in rows if r['file']=='fts.txt'])
report+='''

별도의 vocabulary-pruning 측정은 서로 다른 긴 토큰 1만/10만 개와 한 글자 질의 "x", 편집거리 1, 결과 0개다. 큰 어휘를 스캔하되 길이 차이로 빠르게 제외하는 비용이며, 같은 길이의 근접 단어를 다수 비교하는 최악 조건은 아니다.

'''
report+=table([r['benchmark'] for r in rows if r['file']=='fuzzy-vocabulary.txt'])
report+='''

## 128차원 벡터: 동일 데이터 exact/ANN

seed=42, 정규화된 군집 벡터, L2, K=10. ANN M=16/M0=32, construction ef=200, search ef=64. 100개 질의를 생성하고 10개로 워밍업한다. Recall은 첫 10개 질의의 exact top-10과 비교(총 100개 이웃)하므로 전체 분포의 품질 보장은 아니다.

| 벡터 수 | exact ns/op 중앙값 | ANN 100질의 평균 중앙값 | ANN p99 중앙값 | recall@10 범위 | 구축 시간 중앙값 |
|---|---:|---:|---:|---:|---:|
'''
for scale in ['1K','10K','100K']:
    a=by_name['BenchmarkVectorSearchClustered128D/'+scale];e=by_name['BenchmarkVectorSearchClusteredExact128D/'+scale]
    report+=f"| {scale} | {e['ns/op']/1e6:.4f} ms | {a['mean-ns']/1e6:.4f} ms | {a['p99-ns']/1e6:.4f} ms | {a['recall@10_min']:.1f}–{a['recall@10_max']:.1f}% | {a['index-build-ms']/1000:.2f} s |\n"
report+='''

exact는 순환 질의의 Go benchmark 평균, ANN은 고정 100개 질의의 별도 샘플 평균이다. 같은 데이터지만 샘플 수·타이밍 방식은 다르므로 엄밀한 paired latency 비교는 아니다. 전체 반복값은 CSV/원본을 참고한다.

## 재현 및 한계

이 디렉터리는 v0.8.0 당시 측정의 역사적 스냅샷이다. 아래 명령은 보관된 원시 로그를 다시 집계하며 현재 checkout을 측정하지 않는다. 환경·문법 로그·성능 로그의 SHA-256이 보관 manifest와 다르면 출력 파일을 변경하기 전에 거부한다. 새 측정은 별도 디렉터리와 해당 후보의 환경·하네스 설명으로 기록해야 한다. 현재 원본 비교용 실행 명령은 [별도 하네스](../upstream-2026-09-22/run.sh)를 참고한다.

```sh
python3 docs/benchmarks/v0.8.0-2026-09-22/summarize.py
```

[중앙값·범위 CSV](summary.csv), [쿼리 원본](query.txt), [FTS 원본](fts.txt), [exact 벡터 원본](vector-exact.txt), [ANN 벡터 원본](vector-ann.txt), [큰 어휘 원본](fuzzy-vocabulary.txt), [환경](environment.txt), [문법 테스트](grammar-tests.jsonl), [추가 하네스](../../../internal/engine/query_language_benchmark_test.go).

합성 데이터·단일 장비·warm read 결과이며 CPU 격리나 주파수 고정은 하지 않았다. 읽기/쓰기 혼합, 다중 사용자, 한국어 실제 문서, 콜드 오픈, 대형 임베딩(768/1536차원), 100만 건 이상은 이번 측정 범위에 포함하지 않는다. 변동이 큰 항목은 원본의 범위를 함께 봐야 한다. 제품 성능 변경은 하지 않았고, 이 보고서의 엔진·환경·문법·계측 설명은 보관된 측정 시점에 한정된다.
'''
fts_scan=by_name['BenchmarkQueryFTSCandidates10K/scan']['ns/op']
fts_index=by_name['BenchmarkQueryFTSCandidates10K/postings']['ns/op']
with_row=by_name['BenchmarkQueryLanguage/nodes_100000/with_group_top10']
fuzzy_row=by_name['BenchmarkFTSFuzzyVocabularyPruning/tokens_100000']
exact_100k=by_name['BenchmarkVectorSearchClusteredExact128D/100K']
ann_100k=by_name['BenchmarkVectorSearchClustered128D/100K']
summary=f"""## 핵심 결과

- 문법은 WITH·그룹 집계 6종·내장 함수 20종을 지원하지만 전체 openCypher 구현은 아니다. 자체 매트릭스 {counts['accept']+counts['reject']}개 사례를 모두 통과했다.
- 1만 문서의 동일 FTS 질의에서 속성 인덱스로 {fts_scan/1e6:.3f} ms → {fts_index/1e6:.3f} ms, 약 {fts_scan/fts_index:.1f}배 낮은 지연을 측정했다. 반환 10행 조건이다.
- 10만 노드 WITH 그룹 집계 후 상위 10행은 {with_row['ns/op']/1e6:.2f} ms이나, 회당 {with_row['B/op']/1024/1024:.2f} MiB·{with_row['allocs/op']:,.0f}회 할당했다. 전체 스캔/집계의 할당량은 후속 최적화 후보다.
- 큰 어휘의 fuzzy 검색은 길이 차이로 쉽게 제외되는 조건에서도 10만 토큰에 {fuzzy_row['ns/op']/1e6:.2f} ms다. 작은 어휘의 마이크로초 결과만으로 실서비스 지연을 판단하면 안 된다.
- 10만 128차원 벡터: exact 약 {exact_100k['ns/op']/1e6:.2f} ms, ANN 옵션의 100질의 평균 중앙값 약 {ann_100k['mean-ns']/1e6:.3f} ms, 표본 recall@10 {ann_100k['recall@10']:.1f}%. 인덱스 구축은 약 {ann_100k['index-build-ms']/1000:.0f}초이며 정확도/지연의 표본 한계는 아래에 명시했다.

"""
report=report.replace('## 측정 방법',summary+'## 측정 방법',1)
(root/'REPORT.md').write_text(report)
print('wrote',len(rows),'benchmark summaries; grammar',dict(counts))
