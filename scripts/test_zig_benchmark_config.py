"""Run the workflow's actual reuse predicates against configuration fixtures."""
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest


class ZigBenchmarkConfigTest(unittest.TestCase):
    def test_both_reuse_gates(self):
        workflow = (Path(__file__).resolve().parents[1] / '.github/workflows/benchmark.yml').read_text()
        config = re.search(r'ZIG_BENCH_CONFIG: "([^"]+)"', workflow)[1]
        predicates = re.findall(r'if (\[\[ -f (?:"\$download_dir/zig.txt"|previous/zig.txt).*?); then', workflow, re.S)
        self.assertEqual(len(predicates), 2)
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / 'previous'
            fixture.mkdir()
            values = {'lattice-version': '0.15.0', 'zig-tag': 'v0.15.0',
                      'zig-version': '0.16.0', 'zig-archive-sha256': 'b' * 64,
                      'zig-ref': 'a' * 40, 'zig-revision': 'a' * 40,
                      'zig-sha256': 'valid-checksum', 'zig': 'valid-results'}
            for name, value in values.items():
                (fixture / (name + '.txt')).write_text(value + '\n')
            env = dict(os.environ, download_dir=str(fixture), LATTICE_VERSION='0.15.0',
                       ZIG_TAG='v0.15.0', ZIG_VERSION='0.16.0', ZIG_SHA256='b' * 64,
                       ZIG_REF='a' * 40, ZIG_BENCH_CONFIG=config)
            for marker, expected in [(None, 1), ('old-debug-config', 1), (config, 0)]:
                if marker is not None:
                    (fixture / 'zig-bench-config.txt').write_text(marker + '\n')
                for predicate in predicates:
                    # External checksum/output validators succeed so this check
                    # isolates the workflow's metadata compatibility decisions.
                    command = 'go() { return 0; }; sha256sum() { return 0; };\n' + predicate
                    result = subprocess.run(['bash', '-c', command], cwd=directory, env=env, capture_output=True)
                    self.assertEqual(result.returncode, expected, (marker, result.stderr.decode()))


if __name__ == '__main__':
    unittest.main()
