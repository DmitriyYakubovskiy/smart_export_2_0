import sys
from pathlib import Path
from types import SimpleNamespace
import uuid

import pytest
from fastapi.testclient import TestClient
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import main, consumer


class ProducerStub:
    def __init__(self, error=None):
        self.error = error
        self.messages = []
    def produce(self, topic, **message):
        self.messages.append((topic, message))
    def flush(self, timeout):
        self.messages[-1][1]['on_delivery'](self.error, None)
        return 0


def client(producer):
    main.app.state.producer = producer
    return TestClient(main.app)  # Lifespan intentionally disabled: no real Kafka.


def test_upload_ack_and_binary_payload():
    p = ProducerStub()
    r = client(p).post('/upload', files={'file': ('test.pdf', b'%PDF-test')})
    assert r.status_code == 202
    assert r.json()['status'] == 'queued'
    assert p.messages[0][1]['value'] == b'%PDF-test'
    assert p.messages[0][1]['key'].decode() == r.json()['document_id']


def test_broker_error_is_not_success():
    r = client(ProducerStub(error='unavailable')).post('/upload', files={'file': ('a.pdf', b'x')})
    assert r.status_code == 503


@pytest.mark.parametrize(
    "name,data,expected",
    [
        ("a.txt", b"x", 400),
        ("a.pdf", b"", 400),
        ("a.png", b"x" * (main.MAX_FILE_BYTES + 1), 413),
    ],
    ids=["invalid_extension", "empty_file", "too_large"],
)
def test_invalid_upload(name, data, expected):
    p = ProducerStub()
    assert client(p).post('/upload', files={'file': (name, data)}).status_code == expected
    assert not p.messages


def message(data=b'content', key=None):
    doc_id = key or uuid.uuid4().hex
    return SimpleNamespace(key=lambda: doc_id.encode(), headers=lambda: [('filename', b'../../report.pdf')], value=lambda: data)


def test_save_and_redelivery(tmp_path):
    m = message()
    target = consumer.save_file(m, tmp_path)
    assert target.read_bytes() == b'content'
    assert consumer.save_file(m, tmp_path) == target
    assert list(tmp_path.iterdir()) == [target]


def test_commit_after_save(tmp_path, monkeypatch):
    monkeypatch.setattr(consumer, "extract_text", lambda path: {"text": "hello", "status": "text_extracted"})
    m = message()
    calls = []
    def commit(**kwargs):
        assert (tmp_path / (m.key().decode() + '.pdf')).read_bytes() == b'content'
        assert (tmp_path / (m.key().decode() + '.txt')).read_text() == 'hello'
        assert (tmp_path / (m.key().decode() + '.json')).exists()
        assert kwargs['asynchronous'] is False
        calls.append(kwargs)
    consumer.process_message(SimpleNamespace(commit=commit), m, tmp_path)
    assert len(calls) == 1


def test_failed_save_does_not_commit(tmp_path, monkeypatch):
    calls = []
    def fail(*args):
        raise OSError('disk unavailable')
    monkeypatch.setattr(consumer.os, 'replace', fail)
    with pytest.raises(OSError):
        consumer.process_message(SimpleNamespace(commit=lambda **kw: calls.append(kw)), message(), tmp_path)
    assert not calls
    assert not list(tmp_path.iterdir())


def test_bad_id_does_not_escape_folder(tmp_path):
    with pytest.raises(ValueError):
        consumer.save_file(message(key='../../outside'), tmp_path)
    assert not list(tmp_path.iterdir())
