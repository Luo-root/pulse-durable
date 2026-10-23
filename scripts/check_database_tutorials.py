"""Compile the exact bilingual tutorial programs and exercise committed recovery.

Use only an isolated PostgreSQL database: the tutorial writes database-demo-001.
No schema is dropped by this script. A reused demo RunID is rejected, not reset.
"""
from pathlib import Path
import argparse
import os
import re
import subprocess
import tempfile


def invoke(args, cwd, env, expected=0):
    result = subprocess.run(args, cwd=cwd, env=env, capture_output=True, text=True, timeout=120)
    if result.returncode != expected:
        # PostgreSQL errors may contain connection details; do not print payloads.
        raise RuntimeError(f'{args[0]} exited {result.returncode}; expected {expected}')
    return result.stdout


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('backend', choices=['sqlite', 'postgresql'])
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    programs = []
    for prefix in ['guide', 'en/guide']:
        text = (repo / 'site' / prefix / (args.backend + '.md')).read_text(encoding='utf-8')
        full = [b for b in re.findall(r'```go\s*\n(.*?)```', text, re.S) if b.lstrip().startswith('package main')]
        if len(full) != 1:
            raise RuntimeError('Expected exactly one complete program per tutorial')
        programs.append(full[0])
    if programs[0] != programs[1]:
        raise RuntimeError('Bilingual tutorial programs differ')
    if args.backend == 'postgresql' and not os.environ.get('PULSE_POSTGRES_DSN'):
        raise RuntimeError('PULSE_POSTGRES_DSN must point to an isolated test database')
    with tempfile.TemporaryDirectory(prefix='durable-tutorial-') as name:
        work = Path(name)
        (work / 'main.go').write_text(programs[0], encoding='utf-8')
        (work / 'go.mod').write_text('module example.com/durable-tutorial-check\n\ngo 1.26.0\n', encoding='utf-8')
        paths = [work, repo, repo / 'sqlitestore', repo / 'postgresstore']
        # Quoted paths support Windows drives and spaces; relative '.' preserves
        # the main module's identity on case-insensitive Windows filesystems.
        uses = ['.'] + [p.as_posix() for p in paths[1:]]
        (work / 'go.work').write_text('go 1.26.0\n\nuse (\n' + '\n'.join('    "' + p + '"' for p in uses) + '\n)\n', encoding='utf-8')
        env = dict(os.environ, GOWORK=str(work / 'go.work'))
        exe = work / ('tutorial.exe' if os.name == 'nt' else 'tutorial')
        invoke(['go', 'build', '-buildvcs=false', '-o', str(exe), '.'], work, env)
        first = invoke([str(exe)], work, env, expected=75)
        if 'enter upstream: 41' not in first or 'simulate process exit: 75' not in first:
            raise RuntimeError('Initial run did not reach the committed upstream boundary')
        second = invoke([str(exe), '-resume'], work, env)
        if 'enter upstream:' in second or 'enter downstream: 41' not in second or 'result: 42 ready' not in second:
            raise RuntimeError('Resume failed to reuse the confirmed upstream result')
        third = invoke([str(exe), '-resume'], work, env)
        if 'enter upstream:' in third or 'enter downstream:' in third or 'result: 42 ready' not in third:
            raise RuntimeError('Repeated Resume reran confirmed business')
    print(args.backend + ': bilingual build and three-process recovery PASS')


if __name__ == '__main__':
    main()
