#!/bin/bash
set -e

echo "===================================="
echo "🚀 Запуск всех сервисов для тестирования"
echo "===================================="

# Создаем папку для логов
mkdir -p logs

# Функция для остановки всех процессов
cleanup() {
    echo ""
    echo "Останавливаю все сервисы..."
    kill $DISCOVERY_PID $CONFIG_PID $COLLECTOR_PID $AGGREGATOR_PID $ANALYZER_PID $STORE_PID $WAREHOUSE_PID $CART_PID 2>/dev/null || true
    echo "Все сервисы остановлены"
    exit 0
}

trap cleanup EXIT INT TERM

echo ""
echo "===================================="
echo "1. Запуск инфраструктуры"
echo "===================================="

# Запуск Discovery Server (Eureka)
echo "Starting Discovery Server..."
java -jar infra/discovery-server/target/discovery-server-1.0-SNAPSHOT.jar > logs/discovery-server.log 2>&1 &
DISCOVERY_PID=$!
echo "Discovery Server PID: $DISCOVERY_PID"

# Ждем Eureka
echo "Waiting for Eureka (port 8761)..."
for i in {1..30}; do
    if curl -s http://localhost:8761 > /dev/null 2>&1; then
        echo "✅ Eureka is up!"
        break
    fi
    sleep 1
    echo -n "."
done
echo ""

# Запуск Config Server
echo "Starting Config Server..."
java -jar infra/config-server/target/config-server-1.0-SNAPSHOT.jar > logs/config-server.log 2>&1 &
CONFIG_PID=$!
echo "Config Server PID: $CONFIG_PID"

# Ждем Config Server
echo "Waiting for Config Server..."
sleep 15

echo ""
echo "===================================="
echo "2. Запуск сервисов телеметрии"
echo "===================================="

# Collector
echo "Starting Collector..."
java -jar telemetry/collector/target/collector-1.0-SNAPSHOT.jar > logs/collector.log 2>&1 &
COLLECTOR_PID=$!
echo "Collector PID: $COLLECTOR_PID"
sleep 5

# Aggregator
echo "Starting Aggregator..."
java -jar telemetry/aggregator/target/aggregator-1.0-SNAPSHOT.jar > logs/aggregator.log 2>&1 &
AGGREGATOR_PID=$!
echo "Aggregator PID: $AGGREGATOR_PID"
sleep 5

# Analyzer
echo "Starting Analyzer..."
java -jar telemetry/analyzer/target/analyzer-1.0-SNAPSHOT.jar > logs/analyzer.log 2>&1 &
ANALYZER_PID=$!
echo "Analyzer PID: $ANALYZER_PID"
sleep 5

echo ""
echo "===================================="
echo "3. Запуск сервисов коммерции"
echo "===================================="

# Shopping Store
echo "Starting Shopping Store..."
java -jar commerce/shopping-store/target/shopping-store-1.0-SNAPSHOT.jar > logs/shopping-store.log 2>&1 &
STORE_PID=$!
echo "Shopping Store PID: $STORE_PID"
sleep 5

# Warehouse
echo "Starting Warehouse..."
java -jar commerce/warehouse/target/warehouse-1.0-SNAPSHOT.jar > logs/warehouse.log 2>&1 &
WAREHOUSE_PID=$!
echo "Warehouse PID: $WAREHOUSE_PID"
sleep 5

# Shopping Cart
echo "Starting Shopping Cart..."
java -jar commerce/shopping-cart/target/shopping-cart-1.0-SNAPSHOT.jar > logs/shopping-cart.log 2>&1 &
CART_PID=$!
echo "Shopping Cart PID: $CART_PID"

echo ""
echo "===================================="
echo "✅ ВСЕ СЕРВИСЫ ЗАПУЩЕНЫ"
echo "===================================="
echo "Eureka: http://localhost:8761"
echo ""

# Ждем для стабилизации
sleep 20

# Проверка регистрации в Eureka
echo "Проверка зарегистрированных сервисов:"
curl -s http://localhost:8761/eureka/apps | grep -E "<name>" | sort -u || echo "Нет зарегистрированных сервисов"

echo ""
echo "Все сервисы работают. Ждем завершения тестов..."

# Бесконечное ожидание
wait