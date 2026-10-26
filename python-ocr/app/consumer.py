import json
import logging
import os
import signal
import tempfile
import uuid
from pathlib import Path

from confluent_kafka import Consumer, KafkaException

from app.ocr import extract_text

TOPIC = "uploaded-files"
BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", str(BASE_DIR / "received")))
logger = logging.getLogger(__name__)


def save_file(message, output_dir: Path) -> Path:
    document_id = (message.key() or b"").decode("ascii")
    if uuid.UUID(hex=document_id).hex != document_id:
        raise ValueError("Некорректный document_id")
    headers = dict(message.headers() or [])
    filename = Path(headers["filename"].decode("utf-8").replace("\\", "/")).name
    extension = Path(filename).suffix.lower()
    if extension not in {".png", ".pdf"}:
        raise ValueError("Недопустимое расширение")
    data = message.value()
    if not data or len(data) > 5 * 1024 * 1024:
        raise ValueError("Недопустимый размер файла")

    output_dir.mkdir(parents=True, exist_ok=True)
    # Deterministic name: re-reading the same message writes the same file.
    destination = output_dir / f"{document_id}{extension}"
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=output_dir, prefix=".upload-", delete=False) as f:
            temporary = Path(f.name)
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary, destination)
        # Persist the directory entry on the Linux filesystem used in Docker.
        if os.name == "posix":
            fd = os.open(output_dir, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return destination


def write_result(path: Path, data: str):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix='.ocr-', delete=False) as f:
            temporary = Path(f.name)
            f.write(data.encode('utf-8'))
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary, path)
        if os.name == 'posix':
            fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def process_message(consumer, message, output_dir=OUTPUT_DIR):
    destination = save_file(message, output_dir)
    result = extract_text(destination)
    text = result.pop('text')
    result['document_id'] = destination.stem
    result['characters'] = len(text.strip())
    write_result(destination.with_suffix('.txt'), text)
    write_result(destination.with_suffix('.json'), json.dumps(result, ensure_ascii=False, indent=2))
    # Acknowledge only after original, text and metadata are stored.
    consumer.commit(message=message, asynchronous=False)
    logger.info('OCR: %s, status=%s, characters=%s', destination.name,
                result['status'], result['characters'])
    return destination


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        "group.id": "file-savers",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "fetch.message.max.bytes": 6 * 1024 * 1024,
        "fetch.max.bytes": 12 * 1024 * 1024,
        "max.poll.interval.ms": 1_800_000,
    })
    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        consumer.subscribe([TOPIC])
        while running:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            process_message(consumer, message)
    except Exception:
        # Do not skip an unsaved message or commit subsequent messages past it.
        logger.exception("Обработка остановлена; сообщение не подтверждено")
        raise
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
