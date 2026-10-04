import math
import json
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import zipfile
from fts_corpus import metrics, tokens, prepare, evaluate, query_tokens


class MetricsTests(unittest.TestCase):
    def test_rank_discount_recall_and_empty_result(self):
        rel = {'a': 2, 'b': 1}
        self.assertEqual(metrics(['a', 'b'], rel), {'ndcg10': 1, 'recall10': 1, 'mrr10': 1})
        result = metrics(['x', 'b'], rel)
        self.assertAlmostEqual(result['ndcg10'], (1 / math.log2(3)) / (3 + 1 / math.log2(3)))
        self.assertEqual(result['recall10'], .5)
        self.assertEqual(result['mrr10'], .5)
        self.assertEqual(metrics([], rel), {'ndcg10': 0, 'recall10': 0, 'mrr10': 0})

    def test_duplicate_rejected(self):
        with self.assertRaises(ValueError): metrics(['a', 'a'], {'a': 1})

    def test_common_tokens(self):
        self.assertEqual(tokens('A β B-2 ' + 'x' * 65), ['a', 'b', '2'])

    def test_complete_runs_and_missing_query(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            qrels = root / 'qrels.json'; qrels.write_text(json.dumps({'q': {'a': 1}, 'empty': {'a': 1}}))
            manifest = root / 'manifest.json'; manifest.write_text(json.dumps({'document_ids': {'a': 'original'}}))
            result = root / 'result.json'
            rows = [{'repeat': r, 'query_id': q, 'elapsed_ns': 1000, 'results': [{'id': 'a', 'score': 1}] if q == 'q' else []}
                    for r in range(3) for q in ['q', 'empty']]
            result.write_text(json.dumps({'runs': rows}))
            got = evaluate(qrels, manifest, [result])['results']['result']
            self.assertEqual(got['ndcg10'], .5)
            self.assertEqual(got['samples'], 6)
            result.write_text(json.dumps({'runs': rows[:-1]}))
            with self.assertRaisesRegex(ValueError, 'coverage'): evaluate(qrels, manifest, [result])

    def test_query_keywords_duplicates_and_limit(self):
        terms = query_tokens('AND OR NOT repeated repeated ' + ' '.join('t' + str(i) for i in range(33)))
        self.assertEqual(len(terms), 34)
        self.assertEqual(terms[:32], ['repeated'] + ['t' + str(i) for i in range(31)])
        self.assertEqual(tokens('x' * 64 + ' ' + 'y' * 65), ['x' * 64])

    def test_latency_resolution_and_invalid_values(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            qrels = root / 'qrels.json'; qrels.write_text(json.dumps({'q': {'a': 1}}))
            manifest = root / 'manifest.json'; manifest.write_text(json.dumps({'document_ids': {'a': 'original'}}))
            result = root / 'result.json'
            rows = [{'repeat': r, 'query_id': 'q', 'elapsed_ns': 0, 'results': []} for r in range(3)]
            result.write_text(json.dumps({'runs': rows}))
            got = evaluate(qrels, manifest, [result])['results']['result']
            self.assertEqual(got['median_ms'], 0)
            self.assertEqual(got['p95_ms'], 0)
            for value in [-1, None, True, '0', .5, float('nan'), float('inf')]:
                with self.subTest(value=value):
                    rows[0]['elapsed_ns'] = value
                    result.write_text(json.dumps({'runs': rows}))
                    with self.assertRaisesRegex(ValueError, 'latency'):
                        evaluate(qrels, manifest, [result])
            del rows[0]['elapsed_ns']
            result.write_text(json.dumps({'runs': rows}))
            with self.assertRaisesRegex(ValueError, 'latency'):
                evaluate(qrels, manifest, [result])

    def test_archive_pin_before_output(self):
        with tempfile.TemporaryDirectory() as d:
            archive = Path(d) / 'bad.zip'; archive.write_bytes(b'not the pinned archive')
            out = Path(d) / 'out'
            with self.assertRaisesRegex(ValueError, 'SHA256'): prepare(archive, out)
            self.assertFalse(out.exists())

    def test_prepare_unicode_without_utf8_locale(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            archive = root / 'fixture.zip'
            # Exercise the full preparation path with the required record counts.
            with zipfile.ZipFile(archive, 'w') as z:
                for name, count in [('corpus', 5183), ('queries', 300)]:
                    rows = [{'_id': str(i), 'text': 'science \u2265 \u03b2'} for i in range(count)]
                    z.writestr('scifact/' + name + '.jsonl', '\n'.join(json.dumps(r, ensure_ascii=False) for r in rows).encode('utf-8'))
                z.writestr('scifact/qrels/test.tsv', 'query-id\tcorpus-id\tscore\n' + ''.join(f'{i}\t0\t1\n' for i in range(300)))
            digest = hashlib.sha256(archive.read_bytes()).hexdigest()
            code = ('import sys, fts_corpus; '
                    'fts_corpus.SHA256 = sys.argv[3]; '
                    'fts_corpus.prepare(sys.argv[1], sys.argv[2])')
            out = root / 'data'
            completed = subprocess.run([sys.executable, '-c', code, str(archive), str(out), digest],
                cwd=Path(__file__).resolve().parent,
                env={**os.environ, 'PYTHONUTF8': '0', 'PYTHONCOERCECLOCALE': '0', 'LC_ALL': 'C'},
                capture_output=True)
            self.assertEqual(completed.returncode, 0, completed.stderr.decode('utf-8', errors='replace'))
            for name in ['corpus', 'queries']:
                rows = (out / (name + '-raw.jsonl')).read_text(encoding='utf-8').splitlines()
                self.assertEqual(json.loads(rows[0])['text'], 'science \u2265 \u03b2')
            manifest = json.loads((out / 'manifest.json').read_text(encoding='utf-8'))
            for name, expected in manifest['files'].items():
                data = (out / name).read_bytes()
                self.assertNotIn(b'\r', data, name)
                self.assertEqual(hashlib.sha256(data).hexdigest(), expected, name)


if __name__ == '__main__': unittest.main()
