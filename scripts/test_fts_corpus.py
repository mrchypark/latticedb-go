import math
import json
from pathlib import Path
import tempfile
import unittest
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


if __name__ == '__main__': unittest.main()
