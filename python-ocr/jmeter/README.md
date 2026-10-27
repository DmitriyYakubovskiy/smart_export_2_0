# Apache JMeter в Docker

Проверка Python API POST /upload реальными multipart-запросами. JMeter 5.6.3
скачивается с архива Apache, SHA-512 проверяется при сборке. Java ставится
внутри контейнера, на Windows устанавливать её не требуется.

## Первый запуск (PowerShell из python-ocr)

```powershell
git pull
New-Item -ItemType Directory -Force .\jmeter-input, .\jmeter-reports | Out-Null
```

Скопируйте корректный синтетический PDF/PNG до 5 МиБ в `jmeter-input`.
Не используйте повреждённый PDF: текущий consumer может повторно падать на нём.
Дождитесь обработки старой очереди и проверьте загрузку одного документа.

```powershell
docker compose up -d --build
$env:TEST_FILE = "medical_data_nlp_ru.pdf"
$env:USERS = "1"
$env:LOOPS = "1"
$env:RAMP_UP = "0"
$env:DURATION = "30"
$env:PAUSE_MS = "200"
docker compose -f compose.yaml -f compose.jmeter.yaml run --rm --build --no-deps jmeter
```

JMeter обращается к `http://api:8000/upload` в сети Compose.
`localhost:8030` используется из Windows, внутри JMeter он не подходит.
`--no-deps` означает, что API/Kafka/consumer необходимо запустить заранее.
Проверка /health перед тестом подтверждает доступность API, но не Kafka.
Первая сборка скачивает Java-образ и JMeter, поэтому занимает больше времени.

## Нагрузка

После успешной проверки одного файла:

```powershell
$env:USERS = "5"
$env:LOOPS = "10"
$env:RAMP_UP = "5"
$env:DURATION = "120"
docker compose -f compose.yaml -f compose.jmeter.yaml run --rm --no-deps jmeter
```

Это до 50 загрузок: 5 потоков по 10 повторов с разгоном за 5 секунд.
Каждый поток ждёт ответа и затем выполняет следующий запрос, пауза 200 мс.
Фактическая интенсивность зависит от времени ответа; это не фиксированные RPS.
Длительность 120 секунд является верхним пределом подачи нагрузки, а не
обязательным временем удержания. При завершении повторов тест закончится раньше.
Запрос, начатый до лимита, может закончиться после него.
Повторите отдельно с USERS=1, 5, 10. Дождитесь обработки очереди между запусками.
Для удержания нагрузки увеличивайте LOOPS, учитывая число создаваемых документов.

## Отчёт

В `jmeter-reports/<дата-время>-<pid>/` создаются:
- `html/index.html`: графики, времена ответа, процентили и ошибки;
- `results.jtl`: CSV результатов с document_id принятых документов;
- `jmeter.log`: диагностика;
- `settings.txt`: настройки, размер и SHA-256 входного документа.

Откройте `html/index.html` в браузере. Если браузер блокирует локальные ресурсы:

```powershell
py -m http.server 8090 --bind 127.0.0.1 --directory .\jmeter-reports
```

Откройте http://localhost:8090 и выберите каталог запуска, затем html/.
Сервер завершить Ctrl+C. В отчёт не сохраняются содержимое документа и тела ответов.
Ноль в коде завершения JMeter сам по себе НЕ означает отсутствие ошибок:
смотрите Error % и сообщения assertions в отчёте/JTL.
Проверяются HTTP 202, JSON status=queued и корректный document_id.

## Что измеряется

Отчёт показывает время приёма документа API и подтверждения Kafka.
202 не означает завершение OCR. Для сквозного времени и проверки TXT/JSON
используйте отдельный `tools/load_test.py`, сверяя условия двух запусков.
Не запускайте оба генератора одновременно при сравнении результатов.
Потеря текста на смешанных страницах не выявляется этой HTTP-проверкой.

Параллельно наблюдайте реальные ресурсы и очередь:

```powershell
docker stats
docker compose logs --since=10m api consumer
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group file-savers
```

Запишите CPU/RAM компьютера, лимиты Docker, commit проекта, тип документа,
число страниц, размер, нагрузку, Error %, p95/p99, максимальный LAG и перезапуски.
Нулевой LAG сверяйте с наличием результатов для принятых document_id.
Порог p95 и допустимую долю ошибок задайте до испытания. Выполните три повтора
каждого уровня. Высокая доля ошибок уже на одном потоке требует диагностики
сервиса до увеличения нагрузки. CLI JMeter выбран для нагрузки согласно Apache.

Источники:
- https://jmeter.apache.org/usermanual/get-started.html
- https://jmeter.apache.org/usermanual/generating-dashboard.html
