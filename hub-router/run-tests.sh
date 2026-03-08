#!/bin/bash
# run-tests.sh

set -e

echo "=== Запуск тестов для 4-analyzer ==="

# Путь к Hub Router JAR
HUB_ROUTER_JAR="scripts/hub-router.jar"

if [ ! -f "$HUB_ROUTER_JAR" ]; then
    echo "❌ Hub Router JAR не найден: $HUB_ROUTER_JAR"
    exit 1
fi

# Запуск Docker контейнеров
echo "Запуск Docker контейнеров (Kafka, PostgreSQL)..."
docker-compose up -d

# Ждем запуск Kafka и PostgreSQL
echo "Ожидание запуска контейнеров (30 секунд)..."
sleep 30

# Запуск Hub Router
echo "Запуск Hub Router на порту 59090..."
java -jar "$HUB_ROUTER_JAR" \
    --hub-router.execution.mode=ANALYZE \
    --grpc.server.port=59090 \
    --hub-router.execution.immediate-logging.enabled=false \
    --hub-router.execution.output.info-enabled=true \
    --hub-router.execution.output.trace-enabled=true \
    --hub-router.execution.output.console=true &

HUB_ROUTER_PID=$!
echo "Hub Router PID: $HUB_ROUTER_PID"

# Ждем запуск Hub Router
echo "Ожидание запуска Hub Router (30 секунд)..."
sleep 30

# Проверка, что процесс жив
if kill -0 $HUB_ROUTER_PID 2>/dev/null; then
    echo "✅ Hub Router работает"
else
    echo "❌ Hub Router не запустился"
    exit 1
fi

# Запуск тестов (ваши тесты здесь)
echo "Запуск тестов..."
# ...

# Остановка Hub Router
kill $HUB_ROUTER_PID
echo "✅ Hub Router остановлен"

# Остановка Docker контейнеров (опционально)
# docker-compose down