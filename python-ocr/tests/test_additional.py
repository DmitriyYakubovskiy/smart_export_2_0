import pymupdf
import pytest
from PIL import Image, ImageDraw

from app import main
from app.ocr import extract_text
from test_pipeline import ProducerStub, client


@pytest.mark.parametrize(
    "size,expected",
    [
        (main.MAX_FILE_BYTES - 1, 202),
        (main.MAX_FILE_BYTES, 202),
        (main.MAX_FILE_BYTES + 1, 413),
    ],
)
def test_file_size_boundaries(size, expected):
    producer = ProducerStub()

    response = client(producer).post(
        "/upload",
        files={"file": ("test.pdf", b"x" * size)},
    )

    assert response.status_code == expected
    assert len(producer.messages) == (1 if expected == 202 else 0)


def test_mixed_pdf(tmp_path):
    path = tmp_path / "mixed.pdf"

    image = Image.new("RGB", (1000, 200), "white")
    ImageDraw.Draw(image).text(
        (40, 60), "SCAN 98765", fill="black"
    )
    image_path = tmp_path / "scan.png"
    image.save(image_path)

    with pymupdf.open() as doc:
        page = doc.new_page()
        page.insert_text((72, 72), "TEXT 12345")

        page = doc.new_page()
        page.insert_image(
            pymupdf.Rect(0, 0, 500, 100),
            filename=str(image_path),
        )
        doc.save(path)

    result = extract_text(path)

    assert "12345" in result["text"]
    assert "98765" in result["text"]