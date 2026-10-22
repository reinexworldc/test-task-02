# test-task-02

Микросервис асинхронной обработки платежей на Python 3.12, FastAPI, Pydantic v2,
SQLAlchemy 2 (asyncpg), PostgreSQL и RabbitMQ/FastStream. Миграции — Alembic.

## Запуск

Нужны Docker Engine и Docker Compose v2:

```bash
cp .env.example .env
# При необходимости замените API_KEY в .env (минимум 16 символов).
docker compose up --build -d
docker compose ps
```

Compose дождётся готовности PostgreSQL, применит миграцию отдельным сервисом
`migrate`, затем запустит API и один consumer. Outbox relay работает внутри процесса
consumer. Порты: API — `8000`, PostgreSQL — `5432`, RabbitMQ — `5672`, панель
RabbitMQ — [localhost:15672](http://localhost:15672) (payments / payments).
Данные БД и брокера сохраняются в Docker volumes.
Если порт 5432 занят локальным PostgreSQL, запускайте с
`POSTGRES_PORT=55433 docker compose up --build -d`; при локальном подключении
используйте соответствующий порт в `DATABASE_URL` и `TEST_DATABASE_URL`.

```bash
docker compose logs -f api consumer
docker compose down
# Удаление всех локальных данных при необходимости:
# docker compose down -v
```

Секрет `.env` исключён из Git. Учётные данные PostgreSQL/RabbitMQ в Compose
предназначены для локального запуска.

## API

Все HTTP эндпоинты требуют `X-API-Key`, включая `/health` и `/openapi.json`.
Автоматические публичные Swagger/ReDoc маршруты отключены.
Примеры ниже используют ключ из `.env.example`.

### Создание платежа

Обязательные заголовки: `X-API-Key`, `Idempotency-Key` (1–255 символов без пробелов).
Поддерживаются RUB, USD, EUR; сумма положительная, до 16 цифр в целой части и
до двух знаков после запятой. Для точности передавайте сумму строкой.
`description` и `webhook_url` обязательны, `metadata` по умолчанию `{}`.

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

Ответ `202 Accepted`:

```json
{
  "payment_id": "50f01297-b4ed-486e-952d-31a7d3f8de0a",
  "status": "pending",
  "created_at": "2026-10-22T07:00:00Z"
}
```

Повтор с тем же ключом и теми же данными возвращает тот же payment_id и актуальный
статус с кодом `202`. Нормализация суммы делает `100`, `100.0`, `100.00`
эквивалентными. Порядок ключей JSON не влияет на сравнение. Другие данные при
том же ключе дают `409 Conflict`. Уникальность обеспечивается ограничением БД,
включая конкурентные запросы. Неправильные входные данные дают `422`, отсутствующий
или неверный API ключ — `401`.

### Получение платежа

```bash
curl http://localhost:8000/api/v1/payments/50f01297-b4ed-486e-952d-31a7d3f8de0a \
  -H 'X-API-Key: local-development-key-change-me'

curl http://localhost:8000/health \
  -H 'X-API-Key: local-development-key-change-me'

curl http://localhost:8000/openapi.json \
  -H 'X-API-Key: local-development-key-change-me'
```

GET возвращает `payment_id`, `amount`, `currency`, `description`, `metadata`,
`status`, `webhook_url`, `created_at`, `processed_at`, `attempts`,
`webhook_delivered_at`. Несуществующий UUID даёт `404`, неверный формат UUID — `422`.
`attempts` — число завершённых попыток обработки/уведомления; `processed_at` — время
сохранения результата шлюза. Ошибка webhook не меняет статус самого платежа.

### Webhook

Получатель должен принимать POST и отвечать любым `2xx`. Redirect не выполняется.
Пример тела (поле `payment` содержит подробности платежа, как в GET):

```json
{
  "event_id": "50f01297-b4ed-486e-952d-31a7d3f8de0a",
  "event": "payment.processed",
  "payment": {
    "payment_id": "50f01297-b4ed-486e-952d-31a7d3f8de0a",
    "amount": "1250.50",
    "currency": "RUB",
    "description": "Order 42",
    "metadata": {"order_id": 42},
    "status": "succeeded",
    "webhook_url": "http://host.docker.internal:9000/webhook",
    "created_at": "2026-10-22T07:00:00Z",
    "processed_at": "2026-10-22T07:00:04Z",
    "attempts": 1,
    "webhook_delivered_at": null
  }
}
```

Стабильные `event_id` и заголовок webhook `Idempotency-Key` равны UUID платежа:
получатель должен дедуплицировать уведомления по этому значению. Поле
`webhook_delivered_at` заполняется после успешного ответа, поэтому в самом
уведомлении оно `null`. API ключ сервиса получателю webhook не передаётся.

Для проверки на хосте можно запустить простой приёмник:

```bash
uv run python scripts/webhook_receiver.py
```

На Docker Desktop адрес приёмника из контейнера — `host.docker.internal:9000`.
На Linux добавьте consumer параметр `extra_hosts: ['host.docker.internal:host-gateway']`
либо используйте доступный из контейнера адрес. Webhook URL должен принадлежать
доверенному получателю; сетевые ограничения исходящих запросов для публичного
развёртывания задаются отдельно.

## Доставка, очереди и повторы

1. API в одной транзакции создаёт `payments` и `outbox`. Вызова RabbitMQ в HTTP
   запросе нет: недоступность брокера не теряет созданный платёж.
2. Relay выбирает готовое событие через `FOR UPDATE SKIP LOCKED`, публикует
   persistent сообщение с `mandatory=True` и publisher confirms. Только после
   подтверждения брокера выставляет `published_at`. Ошибка публикации откатывает
   транзакцию, событие остаётся для следующего опроса.
3. Durable direct exchange `payments` маршрутизирует `payments.new` в durable
   очередь `payments.new`. Один обработчик, prefetch=1. Шлюз эмулируется задержкой
   2–5 секунд, вероятность `succeeded` — 90%, `failed` — 10%.
4. Результат шлюза сохраняется в отдельной транзакции до отправки webhook.
   Блокировка строки платежа защищает от одновременной обработки. Повторная
   доставка не вызывает шлюз для платежа с сохранённым результатом.
5. Ошибка webhook (network/timeout/любой не-2xx) сохраняет номер попытки и новое
   outbox событие в одной транзакции. Всего **3 попытки**, включая первоначальную;
   задержки перед второй и третьей: `2` и `4` секунды по умолчанию. Они хранятся в
   `available_at`, переживают перезапуск и не блокируют обработчик ожиданием.
   При ошибке самого эмулятора используется тот же бюджет попыток.
6. Третья ошибка создаёт событие для `payments.dlx`, routing key `payments.dead`,
   очередь `payments.dlq`. DLQ не потребляется автоматически; сообщение содержит
   payment_id, attempt и класс ошибки. Подробности можно посмотреть в RabbitMQ UI.
   У `payments.new` также настроен DLX: некорректные события и ссылки на отсутствующие
   платежи отклоняются сразу как неисправимые.
7. Сообщение подтверждается после фиксации результата либо очередной попытки в БД.
   Сбой инфраструктуры БД приводит к nack/requeue с паузой 2 секунды: он не расходует
   бюджет бизнес-попыток, поскольку результат не зафиксирован. Outbox при недоступном
   брокере также продолжает публикацию до восстановления связи.

Гарантия — **at-least-once**. Если брокер подтвердил публикацию, но процесс упал до
коммита outbox, публикация повторится. Сохранённые номера попыток и отметка доставки
подавляют такие дубли. При падении после HTTP ответа webhook, но до коммита БД
уведомление может прийти повторно: здесь требуется дедупликация получателем.
Аналогичное окно до сохранения результата эмуляции не имеет внешнего денежного
эффекта; при замене эмулятора реальным шлюзом нужно передавать ему payment_id как
ключ идемпотентности.

При постоянном сбое шлюза платёж может остаться `pending` с `attempts=3` и событием
в DLQ: результат оплаты неизвестен. При отказе webhook после оплаты статус остаётся
`succeeded` или `failed`, а `webhook_delivered_at` остаётся `null`. Outbox записи
сохраняются для аудита; очистка и административный replay DLQ в это задание не входят.

## Конфигурация

| Переменная | Значение по умолчанию / назначение |
| --- | --- |
| `API_KEY` | Обязательный статический ключ, минимум 16 символов |
| `DATABASE_URL` | PostgreSQL URL с драйвером `postgresql+asyncpg` |
| `RABBITMQ_URL` | AMQP URL |
| `OUTBOX_POLL_SECONDS` | `1`, период опроса outbox |
| `RETRY_BASE_SECONDS` | `2`, база экспоненциальной задержки |
| `WEBHOOK_TIMEOUT_SECONDS` | `10`, таймаут HTTP операций webhook |

Compose заменяет адреса БД и брокера адресами своих сервисов; локальный запуск
использует значения из `.env`. Задержки retry — минимальные: опрос relay и загрузка
очереди могут увеличить фактическое время.

## Разработка и тесты

Нужны Python 3.12+ и [uv](https://docs.astral.sh/uv/). `uv.lock` фиксирует зависимости.

```bash
uv sync --frozen
cp .env.example .env
# Для локальных процессов достаточно контейнеров инфраструктуры:
docker compose up -d postgres rabbitmq
uv run alembic upgrade head
uv run uvicorn app.api:app --reload
# В другом терминале:
uv run faststream run app.consumer:app
```

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest -q
```

Обычный набор тестов использует SQLite в памяти без внешних сервисов. Для проверки
PostgreSQL блокировок/уникальности и конкурентной идемпотентности нужна **отдельная
тестовая база**: fixture пересоздаёт таблицы `payments` и `outbox` перед каждым тестом.

```bash
docker compose exec postgres createdb -U payments payments_test
TEST_DATABASE_URL=postgresql+asyncpg://payments:payments@localhost:5432/payments_test \
  uv run pytest -q
uv run alembic check
```

Проверяются API ключи, валидация, идемпотентность и конфликты, атомарность outbox,
откат при ошибке публикации, задержки retries, дедупликация consumer, оба результата
шлюза, HTTP ответы webhook и DLQ. Без PostgreSQL один конкурентный тест пропускается.

Для полного HTTP → PostgreSQL → RabbitMQ → consumer → webhook/DLQ теста
запустите Compose. На Docker Desktop:

```bash
LIVE_TESTS=1 uv run pytest tests/test_live.py -q
```

Эти два теста временно открывают HTTP приёмник на порту 9001, создают платежи
в работающем сервисе и проверяют настоящий RabbitMQ DLQ через management API.
По умолчанию они пропускаются. При другом API ключе передайте `LIVE_API_KEY`.
В GitHub Actions настроены lint, форматирование, миграции и тесты на PostgreSQL 17.

## Структура

- `app/api.py`, `schemas.py` — HTTP API и валидация.
- `app/models.py`, `db.py` — модели и асинхронные сессии.
- `app/messaging.py` — RabbitMQ topology и publisher confirms.
- `app/outbox.py` — транзакционный relay.
- `app/consumer.py`, `processing.py` — один consumer, шлюз, webhook, retry/DLQ.
- `migrations/` — явная начальная миграция обеих таблиц.
- `tests/` — автоматические тесты.
