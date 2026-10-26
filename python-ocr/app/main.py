import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from confluent_kafka import Producer, KafkaException
from fastapi import FastAPI, File, HTTPException, Request, UploadFile

TOPIC = "uploaded-files"
MAX_FILE_BYTES = 5 * 1024 * 1024
ALLOWED_EXTENSIONS = {".png", ".pdf"}


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.producer = Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        "enable.idempotence": True,
        "acks": "all",
        "message.max.bytes": 6 * 1024 * 1024,
        "message.timeout.ms": 10000,
    })
    yield
    app.state.producer.flush(12)


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    # This checks the API process, not broker availability.
    return {"status": "ok"}


@app.post("/upload", status_code=202)
def upload(request: Request, file: UploadFile = File(...)):
    try:
        # Also handle filenames sent using Windows path separators.
        filename = Path((file.filename or "").replace("\\", "/")).name
        if not filename or len(filename.encode("utf-8")) > 255:
            raise HTTPException(400, "Название файла обязательно, максимум 255 байт UTF-8")
        extension = Path(filename).suffix.lower()
        if extension not in ALLOWED_EXTENSIONS:
            raise HTTPException(400, "Допустимы только PNG и PDF")

        data = file.file.read(MAX_FILE_BYTES + 1)
        if not data:
            raise HTTPException(400, "Пустой файл")
        if len(data) > MAX_FILE_BYTES:
            raise HTTPException(413, "Максимальный размер файла — 5 МиБ")

        document_id = uuid.uuid4().hex
        delivery = []

        def on_delivery(error, message):
            delivery.append(error)

        producer = request.app.state.producer
        try:
            # File bytes are the message value. Metadata goes in headers.
            producer.produce(
                TOPIC,
                key=document_id.encode("ascii"),
                value=data,
                headers={"filename": filename.encode("utf-8")},
                on_delivery=on_delivery,
            )
            producer.flush(12)
        except (KafkaException, BufferError) as exc:
            raise HTTPException(503, "Не удалось отправить файл в Kafka") from exc

        if not delivery or delivery[0] is not None:
            raise HTTPException(503, "Подтверждение Kafka не получено; результат отправки может быть неизвестен")

        # Kafka acknowledged the message. The consumer might still be processing it.
        return {"status": "queued", "document_id": document_id, "filename": filename}
    finally:
        file.file.close()
