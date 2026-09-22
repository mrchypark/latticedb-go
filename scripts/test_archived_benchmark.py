"""Historical reports must never relabel new measurements as the old baseline."""
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


class ArchivedBenchmarkTest(unittest.TestCase):
    def test_inputs_are_verified_before_outputs_change(self):
        source = Path(__file__).resolve().parents[1] / 'docs/benchmarks/v0.8.0-2026-09-22'
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'archive'
            shutil.copytree(source, root)
            def generate():
                return subprocess.run([sys.executable, str(root / 'summarize.py')], capture_output=True)
            result = generate()
            self.assertEqual(result.returncode, 0, result.stderr)
            outputs = {name: (root / name).read_bytes() for name in ['REPORT.md', 'summary.csv']}
            for name in ['environment.txt', 'query.txt', 'grammar-tests.jsonl']:
                with self.subTest(name=name):
                    path = root / name
                    original = path.read_bytes()
                    path.write_bytes(b'new candidate and environment\n' + original)
                    result = generate()
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn(name.encode(), result.stderr)
                    for output, data in outputs.items():
                        self.assertEqual((root / output).read_bytes(), data)
                    path.write_bytes(original)


if __name__ == '__main__':
    unittest.main()
