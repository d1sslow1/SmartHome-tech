#!/bin/bash
set -e

echo "===================================="
echo "🚀 Запуск всех сервисов SmartHome-tech"
echo "===================================="
echo ""

# Создание папки для логов
mkdir -p logs

# Функция для остановки
cleanup() {
    echo ""
    echo "Останавливаю все сервисы..."
    kill $DISCOVERY_PID $CONFIG_PID $COLLECTOR_PID $AGGREGATOR_PID $ANALYZER_PID $STORE_PID $WAREHOUSE_PID $CART_PID 2>/dev/null || true
    echo "Все сервисы остановлены"
    exit 0
}

trap cleanup EXIT INT TERM

echo "===================================="
echo "Запуск инфраструктурных сервисов..."
echo "===================================="

# 1. Eureka Discovery Server
echo "[1/8] Запуск Eureka Discovery Server..."
java -jar infra/discovery-server/target/discovery-server-1.0-SNAPSHOT.jar > logs/discovery-server.log 2>&1 &
DISCOVERY_PID=$!
echo "✅ Eureka запущен (PID: $DISCOVERY_PID)"

echo "Ожидание запуска Eureka (30 секунд)..."
for i in {1..30}; do
    sleep 1
    if curl -s http://localhost:8761 > /dev/null 2>&1; then
        echo "✅ Eureka доступен на порту 8761"
        break
    fi
    echo -n "."
done
echo ""

# 2. Config Server
echo "[2/8] Запуск Config Server..."
java -jar infra/config-server/target/config-server-1.0-SNAPSHOT.jar > logs/config-server.log 2>&1 &
CONFIG_PID=$!
echo "✅ Config Server запущен (PID: $CONFIG_PID)"

echo "Ожидание регистрации Config Server в Eureka (30 секунд)..."
for i in {1..30}; do
    sleep 1
    if curl -s http://localhost:8761/eureka/apps/CONFIG-SERVER 2>/dev/null | grep -q "UP"; then
        echo "✅ Config Server зарегистрирован в Eureka"
        break
    fi
    echo -n "."
done
echo ""

echo "===================================="
echo "Запуск сервисов телеметрии..."
echo "===================================="

# 3. Collector
echo "[3/8] Запуск Collector..."
java -jar telemetry/collector/target/collector-1.0-SNAPSHOT.jar > logs/collector.log 2>&1 &
COLLECTOR_PID=$!
echo "✅ Collector запущен (PID: $COLLECTOR_PID)"
sleep 5

# 4. Aggregator
echo "[4/8] Запуск Aggregator..."
java -jar telemetry/aggregator/target/aggregator-1.0-SNAPSHOT.jar > logs/aggregator.log 2>&1 &
AGGREGATOR_PID=$!
echo "✅ Aggregator запущен (PID: $AGGREGATOR_PID)"
sleep 5

# 5. Analyzer
echo "[5/8] Запуск Analyzer..."
java -jar telemetry/analyzer/target/analyzer-1.0-SNAPSHOT.jar > logs/analyzer.log 2>&1 &
ANALYZER_PID=$!
echo "✅ Analyzer запущен (PID: $ANALYZER_PID)"
sleep 5

echo "===================================="
echo "Запуск сервисов коммерции..."
echo "===================================="

# 6. Shopping Store
echo "[6/8] Запуск Shopping Store..."
java -jar commerce/shopping-store/target/shopping-store-1.0-SNAPSHOT.jar > logs/shopping-store.log 2>&1 &
STORE_PID=$!
echo "✅ Shopping Store запущен (PID: $STORE_PID)"
sleep 5

# 7. Warehouse
echo "[7/8] Запуск Warehouse..."
java -jar commerce/warehouse/target/warehouse-1.0-SNAPSHOT.jar > logs/warehouse.log 2>&1 &
WAREHOUSE_PID=$!
echo "✅ Warehouse запущен (PID: $WAREHOUSE_PID)"
sleep 5

# 8. Shopping Cart
echo "[8/8] Запуск Shopping Cart..."
java -jar commerce/shopping-cart/target/shopping-cart-1.0-SNAPSHOT.jar > logs/shopping-cart.log 2>&1 &
CART_PID=$!
echo "✅ Shopping Cart запущен (PID: $CART_PID)"

echo "===================================="
echo "✅ ВСЕ СЕРВИСЫ ЗАПУЩЕНЫ"
echo "===================================="
echo "📊 Eureka Dashboard: http://localhost:8761"
echo "===================================="

# Ждем 10 секунд для проверки
sleep 10

# Проверка что все сервисы зарегистрировались
echo ""
echo "Проверка регистрации в Eureka..."
curl -s http://localhost:8761/eureka/apps | grep -E "<name>" || echo "⚠️ Нет зарегистрированных сервисов"

echo ""
echo "Для остановки нажмите Ctrl+C"

# Бесконечное ожидание
wait