#!/bin/bash
set -e

echo "===================================="
echo "🚀 Запуск сервисов телеметрии SmartHome-tech"
echo "===================================="
echo ""

# Создание папки для логов
mkdir -p logs

# Проверка JAR-файлов
echo "Проверка JAR-файлов..."

if [ ! -f "./telemetry/collector/target/collector-1.0-SNAPSHOT.jar" ]; then
    echo "❌ Collector JAR не найден"
    exit 1
fi

if [ ! -f "./telemetry/aggregator/target/aggregator-1.0-SNAPSHOT.jar" ]; then
    echo "❌ Aggregator JAR не найден"
    exit 1
fi

if [ ! -f "./telemetry/analyzer/target/analyzer-1.0-SNAPSHOT.jar" ]; then
    echo "❌ Analyzer JAR не найден"
    exit 1
fi

echo "✅ Все JAR-файлы найдены"
echo ""

# Запуск Docker контейнеров
echo "Запуск Docker контейнеров (Kafka, PostgreSQL)..."
docker-compose up -d
sleep 10

# Функция для остановки
cleanup() {
    echo ""
    echo "Останавливаю все сервисы..."
    kill $DISCOVERY_PID $CONFIG_PID $COLLECTOR_PID $AGGREGATOR_PID $ANALYZER_PID 2>/dev/null || true
    docker-compose down
    echo "Все сервисы остановлены"
    exit 0
}

trap cleanup EXIT INT TERM

echo "===================================="
echo "Запуск инфраструктурных сервисов..."
echo "===================================="

# 1. Eureka Discovery Server
echo "[1/5] Запуск Eureka Discovery Server..."
if [ -f "./infra/discovery-server/target/discovery-server-1.0-SNAPSHOT.jar" ]; then
    java -jar infra/discovery-server/target/discovery-server-1.0-SNAPSHOT.jar > logs/discovery-server.log 2>&1 &
    DISCOVERY_PID=$!
    echo "✅ Eureka запущен (PID: $DISCOVERY_PID)"
else
    echo "⚠️ Eureka JAR не найден, пропускаем"
fi

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
echo "[2/5] Запуск Config Server..."
if [ -f "./infra/config-server/target/config-server-1.0-SNAPSHOT.jar" ]; then
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
else
    echo "⚠️ Config Server JAR не найден, пропускаем"
fi

echo "===================================="
echo "Запуск сервисов телеметрии..."
echo "===================================="

# 3. Collector
echo "[3/5] Запуск Collector..."
java -jar telemetry/collector/target/collector-1.0-SNAPSHOT.jar > logs/collector.log 2>&1 &
COLLECTOR_PID=$!
echo "✅ Collector запущен (PID: $COLLECTOR_PID)"
sleep 5

# 4. Aggregator
echo "[4/5] Запуск Aggregator..."
java -jar telemetry/aggregator/target/aggregator-1.0-SNAPSHOT.jar > logs/aggregator.log 2>&1 &
AGGREGATOR_PID=$!
echo "✅ Aggregator запущен (PID: $AGGREGATOR_PID)"
sleep 5

# 5. Analyzer
echo "[5/5] Запуск Analyzer..."
java -jar telemetry/analyzer/target/analyzer-1.0-SNAPSHOT.jar > logs/analyzer.log 2>&1 &
ANALYZER_PID=$!
echo "✅ Analyzer запущен (PID: $ANALYZER_PID)"

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
if curl -s http://localhost:8761/eureka/apps 2>/dev/null | grep -q "<application>"; then
    echo "✅ Сервисы зарегистрированы в Eureka"
else
    echo "⚠️ Нет зарегистрированных сервисов"
fi

echo ""
echo "Для остановки нажмите Ctrl+C"

# Бесконечное ожидание
wait
