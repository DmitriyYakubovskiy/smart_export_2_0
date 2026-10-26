# FastAPI → Kafka → сохранение файлов

Это отдельный минимальный проект со структурой python-ocr/app. OCR выполняется в app/ocr.py после сохранения оригинала.

## Размещение файлов

| Путь от корня python-ocr | Назначение |
|---|---|
| app/__init__.py | Пакет приложения |
| app/main.py | API и отправка в Kafka |
| app/consumer.py | Потребитель и сохранение |
| received/ | Полученные файлы |
| requirements.txt | Python-зависимости |
| Dockerfile | Образ API и потребителя |
| compose.yaml | Kafka и Python-сервисы |

Перенесите содержимое архива в существующий python-ocr: замените app/main.py и app/consumer.py, добавьте Dockerfile и compose.yaml в корень. Папку received с существующими файлами сохраняйте. Если requirements.txt уже содержит другие зависимости, объедините списки. Существующий .gitignore также сохраните. Все команды запуска ниже выполняются из корня python-ocr.

## Как работает

1. POST /upload принимает multipart-поле `file` с PNG/PDF до 5 МиБ.
2. API отправляет байты файла в топик `uploaded-files`, UUID — в ключ, исходное имя — в заголовок.
3. После подтверждения брокера API отвечает HTTP 202: `status=queued`.
4. Потребитель группы `file-savers` читает сообщение и сохраняет `received/<document_id>.pdf` или `.png`.
5. Потребитель извлекает текст PDF либо распознаёт PNG/скан Tesseract. Рядом сохраняет UUID.txt и UUID.json. Offset подтверждается после записи всех трёх файлов. API сам не создаёт постоянную копию в папке received; временное хранение multipart обеспечивает FastAPI.

## Запуск всего в Docker

Требуется Docker с Compose v2. В каталоге с compose.yaml:

```bash
docker compose up -d --build
docker compose ps -a
docker compose logs -f consumer
```

Сервис kafka-init один раз создаёт топик и завершается с кодом 0 — это нормально.

Открыть http://localhost:8000/docs → POST /upload → Try it out → выбрать файл → Execute.

Или в PowerShell (именно curl.exe):

```powershell
curl.exe -F "file=@C:\Documents\example.pdf" http://localhost:8000/upload
```

Linux/macOS:

```bash
curl -F "file=@example.pdf" http://localhost:8000/upload
```

Пример ответа (ID будет другим):

```json
{"status":"queued","document_id":"a6ec2c07e90f4cda9e93eca2da090de72","filename":"example.pdf"}
```

Результат появится в папке `received` рядом с compose.yaml. Полный путь: `received/a6ec2c07e90f4cda9e93eca2da090de72.pdf`. Файлы с одинаковыми исходными именами получают разные ID. Папку при необходимости можно создать заранее.

## Kafka в Docker, Python локально

Остановить контейнерные API/consumer, если они уже запущены:

```bash
docker compose stop api consumer
docker compose up -d kafka kafka-init
python -m venv .venv
```

Активировать окружение (`.venv\Scripts\Activate.ps1` в PowerShell или `source .venv/bin/activate` в Linux), затем:

```bash
pip install -r requirements.txt
python run.py
```

Во втором терминале с тем же окружением:

```bash
python -m app.consumer
```

По умолчанию локальные клиенты используют localhost:29092, контейнеры — kafka:9092. При локальном запуске consumer сохраняет файлы в received в корне python-ocr независимо от рабочего каталога.

## Проверка очереди

1. Выполнить `docker compose stop consumer`.
2. Загрузить файл: API должен вернуть queued; нового файла ещё нет.
3. Выполнить `docker compose start consumer`.
4. Проверить `docker compose logs consumer` и наличие файла по ID в received.
5. Сравнить содержимое с оригиналом; в PowerShell — `Get-FileHash` для обоих файлов.

Остановка всех сервисов: `docker compose down`. Файлы received и именованный том Kafka сохраняются. `docker compose down -v` удаляет данные Kafka, включая позиции чтения.

## Повторная обработка и ограничения

- Kafka не удаляет сообщение после чтения: сохраняет позицию потребителя. Повторное чтение того же события пишет тот же UUID-файл. Это доставка «как минимум один раз», а не общая гарантия exactly-once.
- Если OCR, запись или commit завершились ошибкой, consumer останавливается; Docker его перезапускает. Ошибочное сообщение не пропускается. Постоянно некорректное сообщение заблокирует эту партицию до исправления; dead-letter очередь пока не добавлена.
- При потере подтверждения публикации API может вернуть 503, хотя Kafka уже получила сообщение. Новый HTTP-запрос создаёт новый ID и может создать дубликат. Идемпотентность producer не устраняет дубли между разными HTTP-запросами.
- Хранение Kafka настроено на 168 часов. Это срок хранения сообщений, включая ещё не обработанные. Именованный том сохраняет их при пересоздании контейнера; один брокер не даёт репликационной отказоустойчивости.
- Размер API — до 5 МиБ, Kafka — 6 МиБ с запасом на заголовки. Передача целого файла выбрана для этого небольшого прототипа. Для больших документов стоит сохранять файл в объектное хранилище, а в Kafka отправлять ID/ссылку.
- Проверяется расширение и размер, не содержимое PNG/PDF. Multipart разбирается до обработчика; лимит не заменяет ограничение тела запроса на входном сервере.
- Порты привязаны к localhost, Kafka без аутентификации в локальном Docker-контуре. Это конфигурация разработки.

## Проверки

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

Тесты проверяют бинарную публикацию, ответ при ошибке брокера, валидацию, повторное сохранение, порядок commit и отказ диска. Kafka в них заменена заглушкой. Docker и живой брокер в среде подготовки недоступны; полный запуск Compose ещё нужно проверить командами выше. 15 тестов прошли, включая реальный OCR PNG и скана PDF с русским и английским текстом, пустой результат и отсутствие commit при ошибках OCR/записи. Kafka заменена заглушкой; Docker не запускался.

Документация: https://hub.docker.com/r/apache/kafka ; https://kafka.apache.org/community/downloads/ ; https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html


## Обновление OCR

Добавьте app/ocr.py, замените app/consumer.py и обновите requirements.txt и Dockerfile. Существующие received, конфиги и другие зависимости сохраните. run.py и config.json в архиве соответствуют локальному запуску API на 127.0.0.1:8030; контейнерный API использует прежний порт 8000 из Compose.

Docker устанавливает Tesseract и языки rus/eng автоматически при сборке:

```powershell
docker compose up -d --build consumer
docker compose logs -f consumer
```

Локально выполните `pip install -r requirements.txt` и отдельно установите Tesseract с rus и eng. Если executable отсутствует в PATH, в терминале потребителя задайте путь (пример):

```powershell
$env:TESSERACT_CMD = "C:\Program Files\Tesseract-OCR\tesseract.exe"
& $env:TESSERACT_CMD --list-langs
python -m app.consumer
```

В списке языков должны быть rus и eng. OCR_LANG меняет языки; по умолчанию rus+eng. API запускается отдельным терминалом командой `python run.py`. Go отправляет на http://127.0.0.1:8030/upload.

Результаты: received/<UUID>.pdf или .png — оригинал, <UUID>.txt — текст UTF-8, <UUID>.json — статус и метаданные. Если на любой странице нет текста, review_required=true; если текста нет вообще, status=empty_text. Пустой результат сохраняется и подтверждается в Kafka. Ошибка распознавания/записи останавливает consumer без подтверждения; устраните причину и перезапустите его. Постоянно ошибочное сообщение блокирует партицию, очереди ошибок пока нет.

Это проверка наличия текста, а не его точности. PDF с текстовым слоем обрабатывается без OCR. Изображения на страницах с частичным текстовым слоем отдельно не распознаются. Поддерживается до 20 страниц, до 25 млн пикселей на изображение/отрисованную страницу; тайм-аут Tesseract — 60 секунд на страницу. PDF с паролем не поддерживается. max.poll.interval.ms увеличен до 30 минут. Каждая запись файла атомарна, но три файла не образуют общую транзакцию; JSON записывается последним.

Старые подтверждённые сообщения сами не обработаются заново: для проверки загрузите документ повторно. Тесты OCR требуют Tesseract, rus/eng и DejaVuSans.ttf (Linux: fonts-dejavu-core).

## Автоматический HTML-отчёт

Один раз установите зависимости тестирования: `python -m pip install -r requirements-dev.txt`. Затем из корня python-ocr запускайте `python -m pytest -q`. Настройки pytest.ini автоматически сохраняют автономный HTML в reports/test-report.html. Каталог создаётся автоматически; следующий запуск перезаписывает отчёт. Открыть в PowerShell: `Start-Process .\reports\test-report.html`. Отчёт содержит результаты, длительности и подробности ошибок. Tesseract должен быть доступен там, где запускаются тесты; наличие его только в контейнере не делает его доступным локальному Python.
