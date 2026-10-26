"""Acceptance gaps reproduced against the unmodified uploaded project.

These tests assert proposed stronger requirements R1-R4. They are not marked
xfail: pytest must report the current discrepancies as failures.
Kafka is replaced at its boundary; OCR/PDF and filesystem code remain real.
"""
import io
from types import SimpleNamespace
import uuid

import pymupdf
import pytest
from PIL import Image, ImageDraw, ImageFont
from fastapi.testclient import TestClient
from app import main, consumer
from app.ocr import extract_text

class AcknowledgingProducer:
    def __init__(self):self.messages=[]
    def produce(self,topic,**message):self.messages.append((topic,message))
    def flush(self,timeout):
        if self.messages:self.messages[-1][1]['on_delivery'](None,None)
        return 0

def api(producer):
    main.app.state.producer=producer
    return TestClient(main.app)

@pytest.mark.parametrize('filename,mime',[('corrupt.pdf','application/pdf'),('corrupt.png','image/png')],ids=['pdf','png'])
def test_R1_corrupt_content_rejected_before_queue(filename,mime):
    """R1: unsupported/corrupt contents receive 400 and never reach Kafka."""
    producer=AcknowledgingProducer()
    response=api(producer).post('/upload',files={'file':(filename,b'not a document',mime)})
    assert response.status_code==400, f'Corrupt {filename} was accepted: HTTP {response.status_code}'
    assert producer.messages==[]

def test_control_valid_png_still_queued():
    producer=AcknowledgingProducer();data=io.BytesIO()
    Image.new('RGB',(100,100),'white').save(data,format='PNG')
    response=api(producer).post('/upload',files={'file':('valid.png',data.getvalue(),'image/png')})
    assert response.status_code==202
    assert len(producer.messages)==1

def test_R2_mixed_content_on_same_page_preserves_scan(tmp_path):
    """R2: extracting a page with a header must also include its scanned body."""
    image=Image.new('RGB',(1400,250),'white')
    font=ImageFont.truetype('DejaVuSans.ttf',48)
    ImageDraw.Draw(image).text((40,70),'SCAN 98765',font=font,fill='black')
    buffer=io.BytesIO();image.save(buffer,format='PNG')
    path=tmp_path/'same-page.pdf'
    with pymupdf.open() as doc:
        page=doc.new_page()
        page.insert_text((50,40),'HEADER 12345')
        page.insert_image(pymupdf.Rect(40,100,550,200),stream=buffer.getvalue())
        doc.save(path)
    result=extract_text(path)
    assert '12345' in result['text']
    assert '98765' in result['text'], f'Scanned content lost: {result!r}'

def test_R3_same_id_different_content_does_not_overwrite(tmp_path):
    """R3: a repeated document ID cannot silently replace the original bytes."""
    identifier=uuid.uuid4().hex
    def msg(data):
        return SimpleNamespace(key=lambda:identifier.encode('ascii'),
            headers=lambda:[('filename',b'report.pdf')],value=lambda:data)
    original=consumer.save_file(msg(b'original bytes'),tmp_path)
    try:consumer.save_file(msg(b'different bytes'),tmp_path)
    except (ValueError,FileExistsError):pass
    assert original.read_bytes()==b'original bytes', 'The original was silently overwritten'

def test_R4_http_retry_with_idempotency_key_has_one_document():
    """R4: retrying an identical request with the same key has one logical ID."""
    producer=AcknowledgingProducer();client=api(producer)
    data=io.BytesIO();Image.new('RGB',(100,100),'white').save(data,format='PNG')
    kwargs={'headers':{'Idempotency-Key':'regression-repeat-001'},
            'files':{'file':('valid.png',data.getvalue(),'image/png')}}
    first=client.post('/upload',**kwargs);second=client.post('/upload',**kwargs)
    assert first.status_code==second.status_code==202
    assert first.json()['document_id']==second.json()['document_id'], 'Retry created another document ID'
    assert len(producer.messages)==1
