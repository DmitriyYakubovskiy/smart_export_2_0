import json
from pathlib import Path
from types import SimpleNamespace
import uuid

import pymupdf
import pytest
from PIL import Image, ImageDraw, ImageFont

from app import consumer
from app.ocr import extract_text


def test_digital_pdf(tmp_path):
    path = tmp_path / 'text.pdf'
    with pymupdf.open() as doc:
        doc.new_page().insert_text((72, 72), 'Invoice number 12345')
        doc.save(path)
    result = extract_text(path)
    assert '12345' in result['text']
    assert result['methods'] == ['pdf_text']


@pytest.mark.parametrize('extension', ['png', 'pdf'])
def test_real_tesseract(tmp_path, extension):
    image = Image.new('RGB', (1400, 250), 'white')
    font = ImageFont.truetype('DejaVuSans.ttf', 48)
    ImageDraw.Draw(image).text((40, 70), 'Invoice 12345 Документ', font=font, fill='black')
    path = tmp_path / ('scan.' + extension)
    image.save(path)
    result = extract_text(path)
    assert '12345' in result['text']
    assert 'Документ' in result['text']
    assert result['methods'] == ['tesseract']


def test_empty_image_is_reviewable_and_committed(tmp_path):
    path = tmp_path / 'blank.png'
    Image.new('RGB', (200, 200), 'white').save(path)
    data = path.read_bytes()
    path.unlink()
    identifier = uuid.uuid4().hex
    message = SimpleNamespace(key=lambda: identifier.encode(),
        headers=lambda: [('filename', b'blank.png')], value=lambda: data)
    calls = []
    consumer.process_message(SimpleNamespace(commit=lambda **kw: calls.append(kw)), message, tmp_path)
    result = json.loads((tmp_path / (identifier + '.json')).read_text())
    assert result['status'] == 'empty_text'
    assert result['review_required'] is True
    assert len(calls) == 1


@pytest.mark.parametrize('failure', ['ocr', 'result_write'])
def test_processing_failure_does_not_commit(tmp_path, monkeypatch, failure):
    calls = []
    def fail(*args):
        raise RuntimeError('processing failed')
    if failure == 'ocr':
        monkeypatch.setattr(consumer, 'extract_text', fail)
    else:
        monkeypatch.setattr(consumer, 'extract_text', lambda p: {'text': 'text', 'status': 'text_extracted'})
        monkeypatch.setattr(consumer, 'write_result', fail)
    message = SimpleNamespace(key=lambda: uuid.uuid4().hex.encode(),
        headers=lambda: [('filename', b'test.pdf')], value=lambda: b'content')
    with pytest.raises(RuntimeError):
        consumer.process_message(SimpleNamespace(commit=lambda **kw: calls.append(kw)), message, tmp_path)
    assert not calls
