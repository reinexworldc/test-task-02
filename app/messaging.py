from faststream.rabbit import Channel, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import get_settings

exchange = RabbitExchange("payments", durable=True)
dlx = RabbitExchange("payments.dlx", durable=True)
queue = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key="payments.new",
    arguments={
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "payments.dead",
    },
)
dead_queue = RabbitQueue("payments.dlq", durable=True, routing_key="payments.dead")
broker = RabbitBroker(
    get_settings().rabbitmq_url,
    default_channel=Channel(publisher_confirms=True, on_return_raises=True, prefetch_count=1),
)


async def declare_topology():
    await broker.declare_exchange(exchange)
    declared_dlx = await broker.declare_exchange(dlx)
    declared_dead = await broker.declare_queue(dead_queue)
    await declared_dead.bind(declared_dlx, routing_key="payments.dead")
    declared_exchange = await broker.declare_exchange(exchange)
    declared_queue = await broker.declare_queue(queue)
    await declared_queue.bind(declared_exchange, routing_key="payments.new")
