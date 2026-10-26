"""Extract PDF text; use Tesseract for images and scanned pages."""
import os
import subprocess
import tempfile
from pathlib import Path

import pymupdf
from PIL import Image, ImageOps

MAX_PIXELS = 25_000_000
MAX_PAGES = 20


def recognize(image: Image.Image) -> str:
    if image.width * image.height > MAX_PIXELS:
        raise ValueError('Изображение превышает 25 миллионов пикселей')
    image = ImageOps.exif_transpose(image).convert('RGBA')
    background = Image.new('RGBA', image.size, 'white')
    background.alpha_composite(image)
    with tempfile.TemporaryDirectory(prefix='ocr-') as directory:
        source = Path(directory) / 'page.png'
        background.convert('RGB').save(source)
        result = subprocess.run(
            [os.getenv('TESSERACT_CMD', 'tesseract'), str(source), 'stdout',
             '-l', os.getenv('OCR_LANG', 'rus+eng'), '--psm', '3'],
            capture_output=True, timeout=60, check=True,
        )
    return result.stdout.decode('utf-8').strip()


def extract_text(path: Path) -> dict:
    pages = []
    methods = []
    if path.suffix.lower() == '.png':
        with Image.open(path) as image:
            if image.format != 'PNG':
                raise ValueError('Содержимое файла не соответствует PNG')
            pages.append(recognize(image))
            methods.append('tesseract')
    else:
        with pymupdf.open(path) as document:
            if not document.is_pdf or document.needs_pass:
                raise ValueError('Ожидался PDF без пароля')
            if not 1 <= len(document) <= MAX_PAGES:
                raise ValueError('Допустимо от 1 до 20 страниц PDF')
            for page in document:
                text = page.get_text('text', sort=True).strip()
                method = 'pdf_text'
                if not text:
                    scale = 300 / 72
                    if (page.rect.width * scale + 1) * (page.rect.height * scale + 1) > MAX_PIXELS:
                        raise ValueError('Страница PDF слишком велика для OCR')
                    pixmap = page.get_pixmap(dpi=300, colorspace=pymupdf.csRGB, alpha=False)
                    text = recognize(Image.frombytes('RGB', [pixmap.width, pixmap.height], pixmap.samples))
                    method = 'tesseract'
                pages.append(text)
                methods.append(method)
    empty_pages = [i + 1 for i, text in enumerate(pages) if not text]
    return {
        'text': '\n\f\n'.join(pages),
        'status': 'empty_text' if all(not text for text in pages) else 'text_extracted',
        'review_required': bool(empty_pages),
        'empty_pages': empty_pages,
        'page_count': len(pages),
        'methods': methods,
    }
