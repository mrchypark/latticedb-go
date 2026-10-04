"""Prepare pinned BEIR SciFact inputs and score complete top-10 result files.

No dependencies. Download the archive separately. Dataset text is not vendored.
"""
import argparse
import csv
import hashlib
import io
import json
import math
from pathlib import Path
import re
import statistics
import zipfile

SHA256 = '536e14446a0ba56ed1398ab1055f39fe852686ecad24a6306c80c490fa8e0165'
URL = 'https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/scifact.zip'


def tokens(text):
    return [t for t in re.findall(r'[a-z0-9]+', text.lower()) if len(t) <= 64]


def query_tokens(text):
    return list(dict.fromkeys(t for t in tokens(text) if t not in {'and', 'or', 'not'}))


def prepare(archive, output):
    data = Path(archive).read_bytes()
    if hashlib.sha256(data).hexdigest() != SHA256:
        raise ValueError('SciFact archive SHA256 mismatch')
    out = Path(output)
    out.mkdir(parents=True, exist_ok=False)
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        def records(name):
            return [json.loads(line) for line in z.read('scifact/' + name).decode().splitlines()]
        docs = sorted(records('corpus.jsonl'), key=lambda r: r['_id'])
        all_queries = {r['_id']: r for r in records('queries.jsonl')}
        qrels = list(csv.DictReader(io.StringIO(z.read('scifact/qrels/test.tsv').decode()), delimiter='\t'))
    ids = {r['_id']: str(i + 1) for i, r in enumerate(docs)}
    query_ids = sorted({r['query-id'] for r in qrels})
    queries = [all_queries[q] for q in query_ids]
    if len(ids) != len(docs) or len(docs) != 5183 or len(queries) != 300:
        raise ValueError('Unexpected corpus/query counts or duplicate document IDs')
    qids = {r['_id']: str(i + 1) for i, r in enumerate(queries)}
    relevance = {qids[q]: {} for q in query_ids}
    for row in qrels:
        relevance[qids[row['query-id']]][ids[row['corpus-id']]] = int(row['score'])
    truncated = []
    empty = []
    for kind, rows in [('corpus', docs), ('queries', queries)]:
        raw, common = [], []
        for i, row in enumerate(rows):
            text = (row.get('title', '') + ' ' + row['text']).strip()
            ts = tokens(text)
            if kind == 'queries':
                ts = query_tokens(text)
                if len(ts) > 32:
                    truncated.append({'id': str(i + 1), 'original_id': row['_id'], 'terms': len(ts)})
                ts = ts[:32]
            if not ts:
                empty.append((kind, row['_id']))
            raw.append({'id': str(i + 1), 'text': text})
            common.append({'id': str(i + 1), 'text': ' '.join(ts)})
        for variant, prepared in [('raw', raw), ('common', common)]:
            (out / f'{kind}-{variant}.jsonl').write_text(''.join(json.dumps(r, ensure_ascii=False) + '\n' for r in prepared))
        (out / f'{kind}-common.tsv').write_text(''.join(r['id'] + '\t' + r['text'] + '\n' for r in common))
    if empty:
        raise ValueError(f'Empty normalized inputs: {empty}')
    (out / 'qrels.json').write_text(json.dumps(relevance, indent=2) + '\n')
    manifest = {'source': URL, 'archive_sha256': SHA256, 'documents': len(docs), 'queries': len(queries),
                'document_ids': {v: k for k, v in ids.items()}, 'query_ids': {v: k for k, v in qids.items()},
                'truncated_queries': truncated, 'normalization': 'ASCII [a-z0-9]+, lowercase, token length1..64; queries remove reserved and/or/not then unique first32; no other stopwords or stemming',
                'files': {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(out.iterdir())}}
    (out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps({k: manifest[k] for k in ['documents', 'queries', 'truncated_queries', 'archive_sha256']}, indent=2))


def metrics(ranking, relevance):
    if len(ranking) > 10 or len(set(ranking)) != len(ranking):
        raise ValueError('Duplicate document or more than10 hits')
    positive = {d: s for d, s in relevance.items() if s > 0}
    if not positive:
        raise ValueError('Query has no positive judgments')
    gains = [(2 ** positive.get(d, 0) - 1) / math.log2(i + 2) for i, d in enumerate(ranking)]
    ideal = sum((2 ** s - 1) / math.log2(i + 2) for i, s in enumerate(sorted(positive.values(), reverse=True)[:10]))
    hits = [i + 1 for i, d in enumerate(ranking) if d in positive]
    return {'ndcg10': sum(gains) / ideal, 'recall10': len(hits) / len(positive), 'mrr10': 1 / hits[0] if hits else 0}


def read_result(path):
    if path.suffix == '.json':
        return json.loads(path.read_text())
    rows = {}
    for line in path.read_text().splitlines():
        parts = line.split('\t')
        if parts[0] not in ('hit', 'time'):
            continue
        key = (int(parts[1]), parts[2])
        row = rows.setdefault(key, {'repeat': key[0], 'query_id': key[1], 'results': []})
        if parts[0] == 'time':
            if 'elapsed_ns' in row:
                raise ValueError('Duplicate timing')
            row['elapsed_ns'] = int(parts[3])
        else:
            if int(parts[3]) != len(row['results']) + 1:
                raise ValueError('Nonsequential rank')
            row['results'].append({'id': parts[4], 'score': float(parts[5])})
    return {'runs': list(rows.values())}


def evaluate(qrels, manifest, results):
    relevance = json.loads(Path(qrels).read_text())
    docs = set(json.loads(Path(manifest).read_text())['document_ids'])
    summaries, rankings = {}, {}
    for filename in results:
        path = Path(filename)
        result = read_result(path)
        runs = result['runs']
        by_repeat = {}
        for row in runs:
            rep, qid = row['repeat'], row['query_id']
            group = by_repeat.setdefault(rep, {})
            if qid in group or qid not in relevance:
                raise ValueError(f'{path}: duplicate or unknown query {qid}')
            ids = [str(h['id']) for h in row['results']]
            if not set(ids) <= docs or any(not math.isfinite(h['score']) for h in row['results']):
                raise ValueError('Unknown document or nonfinite score')
            elapsed = row.get('elapsed_ns')
            # Zero is valid when the query completes within one clock tick.
            if type(elapsed) is not int or elapsed < 0:
                raise ValueError('Missing or invalid latency: require nonnegative integer nanoseconds')
            group[qid] = (ids, metrics(ids, relevance[qid]), elapsed / 1e6)
        if set(by_repeat) != {0, 1, 2}:
            raise ValueError(f'{path}: require exactly repeats0,1,2')
        for group in by_repeat.values():
            if set(group) != set(relevance):
                raise ValueError(f'{path}: incomplete query coverage')
        first = by_repeat[0]
        if any(group[q][0] != first[q][0] for group in by_repeat.values() for q in first):
            raise ValueError(f'{path}: rankings differ between repeats')
        rankings[path.stem] = {q: v[0] for q, v in first.items()}
        latency = sorted(v[2] for group in by_repeat.values() for v in group.values())
        summaries[path.stem] = {
            'queries': len(first), 'samples': len(latency),
            **{m: statistics.mean(v[1][m] for v in first.values()) for m in ['ndcg10', 'recall10', 'mrr10']},
            'median_ms': statistics.median(latency), 'p95_ms': latency[math.ceil(.95 * len(latency)) - 1],
            'round_median_ms': [statistics.median(v[2] for v in by_repeat[r].values()) for r in range(3)],
            'round_p95_ms': [sorted(v[2] for v in by_repeat[r].values())[math.ceil(.95 * len(first)) - 1] for r in range(3)],
            'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
    comparisons = {}
    names = list(rankings)
    for i, left in enumerate(names):
        for right in names[i + 1:]:
            pairs = [(rankings[left][q], rankings[right][q]) for q in relevance]
            comparisons[f'{left} vs {right}'] = {
                'identical_rankings': sum(a == b for a, b in pairs),
                'mean_overlap10': statistics.mean(len(set(a) & set(b)) / 10 for a, b in pairs)}
    return {'results': summaries, 'comparisons': comparisons}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    p = sub.add_parser('prepare'); p.add_argument('archive'); p.add_argument('output')
    p = sub.add_parser('evaluate'); p.add_argument('qrels'); p.add_argument('manifest'); p.add_argument('results', nargs='+')
    args = parser.parse_args()
    if args.command == 'prepare':
        prepare(args.archive, args.output)
    else:
        print(json.dumps(evaluate(args.qrels, args.manifest, args.results), indent=2))
