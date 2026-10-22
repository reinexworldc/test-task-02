# test-task-02

Сервис принимает платёж, возвращает `202` и обрабатывает его в фоне.
Вместо платёжного шлюза — эмуляция: задержка 2–5 секунд, 90% успешных оплат.
После обработки сервис отправляет результат на webhook.

Python 3.12, FastAPI, Pydantic v2, SQLAlchemy 2, PostgreSQL, RabbitMQ (FastStream).

## Запуск

```bash
cp .env.example .env
docker compose up --build -d
```

Миграции применяются автоматически перед запуском API и consumer.
API доступен на `localhost:8000`, RabbitMQ UI — на
[localhost:15672](http://localhost:15672), логин и пароль: `payments`.
PostgreSQL доступен на порту `5432`, RabbitMQ — на `5672`.

Если `5432` уже занят, добавьте `POSTGRES_PORT=55433` в `.env`.
Для подключения к БД с хоста тогда нужно поменять порт и в `DATABASE_URL`.
Внутри Compose адреса БД и брокера настроены отдельно.

```bash
docker compose logs -f api consumer
docker compose down
```

Данные остаются в volumes. `docker compose down -v` удалит их.
Пароли в Compose и API ключ из примера рассчитаны на локальный запуск.

## Запросы

Для всех эндпоинтов нужен заголовок `X-API-Key`. Значение берётся из `.env`.
В примерах ниже используется ключ из `.env.example`.

Создать платёж:

```bash
curl -i http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: local-development-key-change-me' \
  -H 'Idempotency-Key: order-42' \
  -H 'Content-Type: application/json' \
  -d '{
    "amount": "1250.50",
    "currency": "RUB",
    "description": "Order 42",
    "metadata": {"order_id": 42},
    "webhook_url": "http://host.docker.internal:9000/webhook"
  }'
```

Ответ — `202 Accepted`:

```json
{
  "payment_id": "50f01297-b4ed-486e-952d-31a7d3f8de0a",
  "status": "pending",
  "created_at": "2026-10-22T07:00:00Z"
}
```

`Idempotency-Key` обязателен: от 1 до 255 символов без пробелов.
Повторный запрос с тем же ключом и телом вернёт существующий платёж.
Если тело отличается, сервис ответит `409`. Суммы `100` и `100.00` считаются
одинаковыми, порядок ключей в JSON тоже не влияет на сравнение.

Сумма должна быть положительной, максимум 16 цифр до запятой и две после.
Передавать её удобнее строкой. Валюты: `RUB`, `USD`, `EUR`.
`metadata` можно не указывать, остальные поля из примера обязательны.

Получить платёж — подставьте UUID из ответа на создание:

```bash
curl http://localhost:8000/api/v1/payments/50f01297-b4ed-486e-952d-31a7d3f8de0a \
  -H 'X-API-Key: local-development-key-change-me'
```

В ответе будут данные платежа, статус, `created_at`, `processed_at`, число попыток
`attempts` и время доставки уведомления `webhook_delivered_at`.
Ошибка webhook не меняет результат оплаты.

Коды ошибок: `401` — неверный или отсутствующий API ключ, `404` — платёж не найден,
`409` — конфликт ключа идемпотентности, `422` — ошибка входных данных.

Ещё есть `/health` с проверкой подключения к БД и `/openapi.json` со схемой API.
Оба требуют `X-API-Key`. Swagger и ReDoc отключены.

## Webhook

Для локальной проверки запустите приёмник в другом терминале:

```bash
uv run python scripts/webhook_receiver.py
```

Он слушает порт `9000` и печатает полученные уведомления. Адрес в примере запроса
подходит для Docker Desktop. На Linux добавьте в сервис `consumer` параметр
`extra_hosts: ['host.docker.internal:host-gateway']` или укажите другой адрес,
доступный из контейнера.

Сервис отправляет POST с полями `event_id`, `event` и `payment`.
`event` равен `payment.processed`, `payment` содержит те же поля, что и GET платежа.
`event_id` и заголовок `Idempotency-Key` равны UUID платежа.
По этому значению получатель должен отсеивать повторные уведомления.

Успешным считается любой ответ `2xx`. Redirect, timeout и остальные коды приводят
к повторной попытке. `webhook_delivered_at` сохраняется после ответа получателя,
поэтому в самом уведомлении это поле ещё `null`.

## Обработка и повторы

Платёж и запись outbox создаются в одной транзакции. Relay работает в процессе
consumer: выбирает готовые записи через `FOR UPDATE SKIP LOCKED` и публикует их
в RabbitMQ. `published_at` выставляется только после publisher confirm.
Если брокер недоступен, запись остаётся в БД до следующей попытки публикации.

Exchange `payments` направляет сообщения с ключом `payments.new` в одноимённую
очередь. Очередь и exchange — durable, сообщения — persistent.
Consumer один, с `prefetch=1`. Он сохраняет результат шлюза перед отправкой webhook,
поэтому ошибка уведомления не приводит к повторной оплате.

Всего три попытки, включая первую. Задержки перед второй и третьей — 2 и 4 секунды.
Номер попытки и следующее событие сохраняются в одной транзакции; время повтора
хранится в `outbox.available_at`. Перезапуск consumer не сбрасывает повторы.
После третьей ошибки сообщение уходит через `payments.dlx` с ключом `payments.dead`
в очередь `payments.dlq`. Автоматического разбора DLQ нет.
Некорректные сообщения и ссылки на отсутствующие платежи попадают туда сразу.

Сбой БД не расходует попытку: сообщение возвращается в очередь с паузой 2 секунды.
При сбое самого эмулятора используются те же три попытки. Если все они закончились
ошибкой, платёж остаётся `pending`, а сообщение — в DLQ.

Доставка at-least-once. Падение между публикацией и коммитом outbox может дать дубль;
consumer проверяет сохранённый номер попытки и отметку доставки webhook.
Если получатель уже ответил, а consumer упал до коммита, webhook придёт повторно.
При замене эмулятора настоящим шлюзом ему тоже нужно передавать ключ идемпотентности.

## Разработка

Зависимости устанавливаются через [uv](https://docs.astral.sh/uv/):

```bash
uv sync --frozen
cp .env.example .env
docker compose up -d postgres rabbitmq
uv run alembic upgrade head
uv run uvicorn app.api:app --reload
```

Consumer запускается отдельно: `uv run faststream run app.consumer:app`.
Если API и consumer уже работают в Docker, сначала остановите их:
`docker compose stop api consumer`.

Настройки перечислены в `.env.example`. `API_KEY` обязателен, минимум 16 символов.
`RETRY_BASE_SECONDS` задаёт базовую задержку повторов, `WEBHOOK_TIMEOUT_SECONDS` —
таймаут HTTP операций, `OUTBOX_POLL_SECONDS` — интервал опроса outbox.

## Проверки

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest -q
```

По умолчанию тесты используют SQLite в памяти. Для теста конкурентных запросов
нужен PostgreSQL. Укажите отдельную БД: тесты пересоздают таблицы перед каждым запуском.

```bash
docker compose exec postgres createdb -U payments payments_test
TEST_DATABASE_URL=postgresql+asyncpg://payments:payments@localhost:5432/payments_test \
  uv run pytest -q
```

Если меняли `POSTGRES_PORT`, замените `5432` в команде выше.
Миграции можно сверить с моделями через `uv run alembic check`.
В CI эти проверки выполняются с PostgreSQL 17.

Два сквозных теста запускаются отдельно при работающем Compose:

```bash
LIVE_TESTS=1 uv run pytest tests/test_live.py -q
```

Они поднимают приёмник на порту `9001`, проверяют доставку webhook и сообщение
в RabbitMQ DLQ после трёх ошибок. Адрес приёмника рассчитан на Docker Desktop.
Если API ключ отличается от примера, передайте его через `LIVE_API_KEY`.
