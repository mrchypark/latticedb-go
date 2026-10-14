"""Check that CI cannot reuse resident or historical Zig comparison results."""
from pathlib import Path
import unittest

class ZigBenchmarkConfigTest(unittest.TestCase):
    def test_public_disk_inputs_are_fresh(self):
        workflow = (Path(__file__).resolve().parents[1] / '.github/workflows/benchmark.yml').read_text(encoding='utf-8')
        self.assertNotIn('ZIG_REUSED', workflow)
        self.assertIn('-zig-fresh=true', workflow)
        self.assertIn('BenchmarkVectorSearchDisk128D', workflow)
        self.assertIn('LATTICEDB_VECTOR_FIXTURE_DIR=', workflow)
        self.assertIn('scripts/vector-disk-zig.zig', workflow)
        self.assertIn('results/fixtures/vectors-100000.bin', workflow)
        self.assertIn('vector-fixture-sha256.txt', workflow)
        self.assertIn('zig-adapter-sha256.txt', workflow)
        self.assertIn('sha256sum --check', workflow)

if __name__ == '__main__':
    unittest.main()
