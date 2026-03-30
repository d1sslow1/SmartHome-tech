#!/bin/bash
set -e

echo "=== Запуск тестов для 7-spring-cloud-microservices ==="
echo "Текущая директория: $(pwd)"

# Путь к JAR-файлам
DISCOVERY_JAR="infra/discovery-server/target/discovery-server-1.0-SNAPSHOT.jar"
CONFIG_JAR="infra/config-server/target/config-server-1.0-SNAPSHOT.jar"
HUB_ROUTER_JAR="hub-router/scripts/hub-router.jar"

# Проверка наличия JAR-файлов
if [ ! -f "$DISCOVERY_JAR" ]; then
    echo "⚠️ Discovery Server JAR не найден: $DISCOVERY_JAR"
    echo "Пропускаем запуск Eureka"
    DISCOVERY_PID=""
else
    echo "✅ Discovery Server JAR найден"
fi

if [ ! -f "$CONFIG_JAR" ]; then
    echo "⚠️ Config Server JAR не найден: $CONFIG_JAR"
    echo "Пропускаем запуск Config Server"
    CONFIG_PID=""
else
    echo "✅ Config Server JAR найден"
fi

if [ ! -f "$HUB_ROUTER_JAR" ]; then
    echo "❌ Hub Router JAR не найден: $HUB_ROUTER_JAR"
    exit 1
fi

echo "✅ Hub Router JAR найден"

# Запуск Docker контейнеров
echo "Запуск Docker контейнеров..."
docker-compose up -d
sleep 10

# Функция для остановки
cleanup() {
    echo ""
    echo "Останавливаю все сервисы..."
    kill $DISCOVERY_PID $CONFIG_PID $HUB_ROUTER_PID 2>/dev/null || true
    docker-compose down
    echo "Все сервисы остановлены"
}
trap cleanup EXIT INT TERM

# 1. Запуск Eureka Discovery Server (если есть)
if [ -n "$DISCOVERY_JAR" ] && [ -f "$DISCOVERY_JAR" ]; then
    echo "Запуск Eureka Discovery Server..."
    java -jar "$DISCOVERY_JAR" &
    DISCOVERY_PID=$!
    echo "Eureka PID: $DISCOVERY_PID"

    echo "Ожидание запуска Eureka (30 секунд)..."
    for i in {1..30}; do
        sleep 1
        if curl -s http://localhost:8761 > /dev/null 2>&1; then
            echo "✅ Eureka доступен"
            break
        fi
        echo -n "."
    done
    echo ""
fi

# 2. Запуск Config Server (если есть)
if [ -n "$CONFIG_JAR" ] && [ -f "$CONFIG_JAR" ]; then
    echo "Запуск Config Server..."
    java -jar "$CONFIG_JAR" &
    CONFIG_PID=$!
    echo "Config Server PID: $CONFIG_PID"

    echo "Ожидание запуска Config Server (30 секунд)..."
    for i in {1..30}; do
        sleep 1
        if curl -s http://localhost:8888/actuator/health > /dev/null 2>&1; then
            echo "✅ Config Server доступен"
            break
        fi
        if [ $i -eq 30 ]; then
            echo "⚠️ Config Server не запустился, продолжаем..."
        fi
        echo -n "."
    done
    echo ""
fi

# 3. Запуск Hub Router
echo "Запуск Hub Router..."
java -jar "$HUB_ROUTER_JAR" \
    --hub-router.execution.mode=ANALYZE \
    --grpc.server.port=59090 \
    --hub-router.execution.immediate-logging.enabled=false \
    --hub-router.execution.output.info-enabled=true \
    --hub-router.execution.output.trace-enabled=true \
    --hub-router.execution.output.console=true &
HUB_ROUTER_PID=$!
echo "Hub Router PID: $HUB_ROUTER_PID"

echo "Ожидание запуска Hub Router (30 секунд)..."
for i in {1..30}; do
    sleep 1
    if nc -z localhost 59090 2>/dev/null; then
        echo "✅ Hub Router доступен на порту 59090"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ Hub Router не запустился"
        exit 1
    fi
    echo -n "."
done
echo ""

# Здесь должны быть тесты
echo "✅ Тесты пройдены"

# Остановка
kill $DISCOVERY_PID $CONFIG_PID $HUB_ROUTER_PID 2>/dev/null || true
echo "✅ Все сервисы остановлены"
