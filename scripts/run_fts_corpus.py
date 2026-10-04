"""Run three sequential, rotated rounds. Build both adapters before this command."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time
from fts_corpus import read_result


def run(go, zig, data, output):
    data, out = Path(data).resolve(), Path(output).resolve()
    out.mkdir(parents=True, exist_ok=False)
    configs = [('go-common-bm25', 'common', 'bm25'), ('zig-common-bm25', 'common', None),
               ('go-raw-frequency', 'raw', 'frequency'), ('go-raw-bm25', 'raw', 'bm25'),
               ('go-raw-porter-bm25', 'raw', 'porter-bm25')]
    merged = {name: {'runs': [], 'round_metadata': []} for name, _, _ in configs}
    order = []
    for rep in range(3):
        rotated = configs[rep:] + configs[:rep]
        for name, variant, mode in rotated:
            print(f'round={rep} configuration={name}', flush=True)
            db = out / f'{name}-{rep}.db'
            path = out / f'{name}-{rep}.json' if mode else out / f'{name}-{rep}.tsv'
            started = time.time()
            if mode:
                cmd = [go, '-corpus', str(data / f'corpus-{variant}.jsonl'), '-queries', str(data / f'queries-{variant}.jsonl'),
                       '-mode', mode, '-db', str(db), '-output', str(path), '-repeats', '1']
                subprocess.run(cmd, env={**os.environ, 'GOMAXPROCS': '2'}, check=True)
            else:
                cmd = [zig, str(data / 'corpus-common.tsv'), str(data / 'queries-common.tsv'), str(db), '1']
                with path.open('w', encoding='utf-8', newline='\n') as f:
                    subprocess.run(cmd, stdout=f, stderr=f, check=True)
            result = read_result(path)
            for row in result['runs']:
                if row['repeat'] != 0:
                    raise ValueError('Adapter returned an unexpected repeat')
                row['repeat'] = rep
                merged[name]['runs'].append(row)
            merged[name]['round_metadata'].append({k: v for k, v in result.items() if k != 'runs'})
            (out / f'{name}.json').write_text(json.dumps(merged[name], indent=2) + '\n', encoding='utf-8', newline='\n')
            order.append({'round': rep, 'configuration': name, 'started_unix': started, 'finished_unix': time.time(), 'command': cmd})
            (out / 'execution-order.json').write_text(json.dumps(order, indent=2) + '\n', encoding='utf-8', newline='\n')


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    for name in ['go', 'zig', 'data', 'output']: p.add_argument('--' + name, required=True)
    a = p.parse_args()
    run(a.go, a.zig, a.data, a.output)
