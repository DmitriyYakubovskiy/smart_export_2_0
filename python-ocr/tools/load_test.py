#!/usr/bin/env python3
"""Load test for the Python upload/OCR service (standard library only).

Run in Windows PowerShell from python-ocr after starting Docker:
  py tools/load_test.py scan.pdf --received received
Default: warm-up 60 s, then 180 s each at concurrency 1,5,10.
Use only synthetic, valid PDF/PNG files. Each request creates a real document.
The received directory must be the consumer's shared output directory.
Before running, verify one document manually and drain the existing Kafka queue.
The driver uses closed-loop concurrency, NOT a fixed requests-per-second rate.
It drains its own accepted documents between stages (timeout is a test failure).
JSON/TXT readiness is observed by polling and does not prove Kafka commit.
Check consumer logs/restarts and Kafka LAG separately. For resource monitoring:
  docker stats
  docker compose logs --since=10m api consumer
  docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group file-savers
For a stability run use --stages 1 --duration 1800 --max-requests 2000.
For recovery testing, stop/start consumer in a second terminal while running.
Do not send another run until this run's documents and previous queue are done.
Reports contain only document IDs/metrics, not document contents.
"""
import argparse
import concurrent.futures
import csv
import hashlib
import json
import math
import mimetypes
import platform
import re
import statistics
import threading
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path


def percentile(values, q):
    return sorted(values)[max(0, math.ceil(len(values) * q) - 1)] if values else None


def send(url, path, data, timeout, stage):
    boundary = uuid.uuid4().hex
    # Use an ASCII transport filename; the real input name is kept in summary.
    filename = 'load-test' + path.suffix.lower()
    body = (f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{filename}"\r\n'
            f'Content-Type: {mimetypes.guess_type(filename)[0]}\r\n\r\n').encode() + data + f'\r\n--{boundary}--\r\n'.encode()
    started = time.monotonic()
    row = dict(stage=stage, started=started, api_seconds=None, http_status='',
               document_id='', error='', processing_status='', e2e_seconds=None)
    request = urllib.request.Request(url, body, {'Content-Type': f'multipart/form-data; boundary={boundary}'})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            row['http_status'] = response.status
            payload = json.loads(response.read(65536))
        identifier = payload.get('document_id', '')
        if row['http_status'] != 202 or payload.get('status') != 'queued' or not re.fullmatch(r'[0-9a-f]{32}', identifier):
            row['error'] = 'Unexpected upload response'
        else:
            row['document_id'] = identifier
    except urllib.error.HTTPError as exc:
        row['http_status'] = exc.code
        row['error'] = 'HTTP error'
    except Exception as exc:
        row['error'] = f'{type(exc).__name__}: {exc}'
    row['api_seconds'] = time.monotonic() - started
    return row


def observe(row, received):
    if not row['document_id'] or row['e2e_seconds'] is not None:
        return
    identifier = row['document_id']
    try:
        result = json.loads((received / f'{identifier}.json').read_text(encoding='utf-8'))
        text = received / f'{identifier}.txt'
        if result.get('document_id') != identifier or not text.is_file():
            return
        row['processing_status'] = result.get('status', 'unknown')
        row['e2e_seconds'] = time.monotonic() - row['started']
    except (OSError, ValueError):
        pass


def run_stage(args, path, data, received, concurrency, duration, label):
    rows = []
    pending = set()
    started = time.monotonic()
    deadline = started + duration
    # Completion observer runs while HTTP requests are active, not just afterwards.
    stop = threading.Event()
    lock = threading.Lock()
    def observer():
        while not stop.wait(0.2):
            with lock:
                for row in rows:
                    observe(row, received)
    watcher = threading.Thread(target=observer, daemon=True)
    watcher.start()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            submitted = 0
            while pending or (time.monotonic() < deadline and submitted < args.max_requests):
                while time.monotonic() < deadline and len(pending) < concurrency and submitted < args.max_requests:
                    pending.add(pool.submit(send, args.url, path, data, args.timeout, label))
                    submitted += 1
                if not pending:
                    break
                done, pending = concurrent.futures.wait(pending, timeout=0.2, return_when=concurrent.futures.FIRST_COMPLETED)
                with lock:
                    rows.extend(future.result() for future in done)
        upload_elapsed = time.monotonic() - started
        drain_deadline = time.monotonic() + args.drain_timeout
        while time.monotonic() < drain_deadline:
            with lock:
                outstanding = any(r['document_id'] and r['e2e_seconds'] is None for r in rows)
            if not outstanding:
                break
            time.sleep(0.2)
    finally:
        stop.set()
        watcher.join()
    for row in rows:
        observe(row, received)
    elapsed = time.monotonic() - started
    accepted = [r for r in rows if r['document_id']]
    completed = [r for r in accepted if r['e2e_seconds'] is not None]
    good = [r for r in completed if r['processing_status'] == 'text_extracted']
    latencies = [r['api_seconds'] for r in rows]
    e2e = [r['e2e_seconds'] for r in completed]
    p95 = percentile(latencies, .95)
    summary = dict(stage=label, concurrency=concurrency, target_seconds=duration,
                   upload_seconds=upload_elapsed, total_seconds=elapsed,
                   request_cap_reached=len(rows) >= args.max_requests,
                   sent=len(rows), accepted=len(accepted), completed=len(completed),
                   text_extracted=len(good), unfinished=len(accepted)-len(completed),
                   api_errors=sum(bool(r['error']) for r in rows),
                   api_mean_seconds=statistics.mean(latencies) if latencies else None,
                   api_p95_seconds=p95, api_p99_seconds=percentile(latencies, .99),
                   observed_e2e_p95_seconds=percentile(e2e, .95),
                   requests_per_second=len(rows)/upload_elapsed,
                   completed_documents_per_minute=len(completed)*60/elapsed,
                   passed=bool(rows) and len(good)==len(rows) and not any(r['error'] for r in rows)
                          and p95 is not None and p95 <= args.api_p95_limit)
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    return rows, summary


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('file', type=Path)
    parser.add_argument('--received', type=Path, default=Path('received'))
    parser.add_argument('--url', default='http://localhost:8030/upload')
    parser.add_argument('--stages', default='1,5,10')
    parser.add_argument('--warmup', type=float, default=60)
    parser.add_argument('--duration', type=float, default=180)
    parser.add_argument('--timeout', type=float, default=30)
    parser.add_argument('--drain-timeout', type=float, default=300)
    parser.add_argument('--max-requests', type=int, default=200, help='Safety cap per stage; hitting it shortens the profile')
    parser.add_argument('--api-p95-limit', type=float, default=2, help='Proposed API acceptance threshold in seconds')
    parser.add_argument('--out', type=Path, default=Path('load-reports'))
    args = parser.parse_args()
    try:
        stages = [int(v) for v in args.stages.split(',')]
    except ValueError:
        parser.error('--stages must contain integers, e.g. 1,5,10')
    if not stages or any(v < 1 or v > 100 for v in stages):
        parser.error('Concurrency must be between 1 and 100')
    if min(args.duration, args.timeout, args.drain_timeout, args.api_p95_limit) <= 0 or args.warmup < 0 or args.max_requests < 1:
        parser.error('Durations/limits must be positive; warmup may be zero')
    if args.file.suffix.lower() not in {'.pdf', '.png'} or not args.file.is_file():
        parser.error('Provide a valid synthetic PDF or PNG file')
    data = args.file.read_bytes()
    if not 0 < len(data) <= 5*1024*1024:
        parser.error('Input must contain 1 byte to 5 MiB')
    if not args.received.is_dir():
        parser.error('--received must point to the existing consumer output directory')
    args.out.mkdir(parents=True, exist_ok=True)
    output = args.out / (time.strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:6])
    output.mkdir()
    summaries = []
    all_rows = []
    metadata = dict(platform=platform.platform(), python=platform.python_version(),
                    input_name=args.file.name, input_bytes=len(data), sha256=hashlib.sha256(data).hexdigest(),
                    arguments={k:str(v) if isinstance(v, Path) else v for k,v in vars(args).items()},
                    notes='Closed-loop load. E2E is observed result readiness, with polling delay. Does not measure Kafka commit, OCR accuracy, CPU or RAM. Record hardware/Docker limits separately.')
    def save():
        (output/'summary.json').write_text(json.dumps(dict(environment=metadata, stages=summaries), ensure_ascii=False, indent=2), encoding='utf-8')
        if all_rows:
            with (output/'requests.csv').open('w', newline='', encoding='utf-8-sig') as handle:
                writer=csv.DictWriter(handle, fieldnames=all_rows[0].keys())
                writer.writeheader()
                writer.writerows(all_rows)
    plan = ([('warmup',1,args.warmup)] if args.warmup else []) + [(f'stage-{i+1}', n, args.duration) for i,n in enumerate(stages)]
    for label, concurrency, duration in plan:
        rows, summary = run_stage(args, args.file, data, args.received, concurrency, duration, label)
        all_rows.extend(rows)
        summaries.append(summary)
        save()
        if summary['unfinished']:
            print('Stopped: accepted documents are unfinished. Check consumer and Kafka before another run.')
            break
    print(f'Reports: {output.resolve()}')
    return 0 if all(s['passed'] for s in summaries) else 1


if __name__ == '__main__':
    raise SystemExit(main())
