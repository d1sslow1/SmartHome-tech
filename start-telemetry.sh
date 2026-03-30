#!/bin/bash
# start-telemetry.sh - полная версия для запуска всех сервисов

set -e

echo "===================================="
echo "🚀 Запуск сервисов телеметрии SmartHome-tech"
echo "===================================="
echo ""

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_status() {
    echo -e "${BLUE}[$(date +'%H:%M:%S')]${NC} $1"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

# Создание папки для логов
mkdir -p logs

# Проверка JAR-файлов
print_status "Проверка JAR-файлов..."

if [ ! -f "./infra/discovery-server/target/discovery-server-1.0-SNAPSHOT.jar" ]; then
    print_error "Discovery Server JAR не найден"
    exit 1
fi

if [ ! -f "./infra/config-server/target/config-server-1.0-SNAPSHOT.jar" ]; then
    print_error "Config Server JAR не найден"
    exit 1
fi

if [ ! -f "./telemetry/collector/target/collector-1.0-SNAPSHOT.jar" ]; then
    print_error "Collector JAR не найден"
    exit 1
fi

if [ ! -f "./telemetry/aggregator/target/aggregator-1.0-SNAPSHOT.jar" ]; then
    print_error "Aggregator JAR не найден"
    exit 1
fi

if [ ! -f "./telemetry/analyzer/target/analyzer-1.0-SNAPSHOT.jar" ]; then
    print_error "Analyzer JAR не найден"
    exit 1
fi

print_success "Все JAR-файлы найдены"
echo ""

# Проверка Docker
print_status "Проверка Docker..."

if ! command -v docker &> /dev/null; then
    print_error "Docker не установлен"
    exit 1
fi

if ! docker info &> /dev/null; then
    print_error "Docker не запущен"
    exit 1
fi

print_success "Docker доступен"

# Запуск Docker контейнеров
print_status "Запуск Docker контейнеров (Kafka, PostgreSQL)..."
docker-compose up -d
sleep 10

# Функция для остановки
cleanup() {
    echo ""
    print_warning "Останавливаю все сервисы..."
    kill $DISCOVERY_PID $CONFIG_PID $COLLECTOR_PID $AGGREGATOR_PID $ANALYZER_PID 2>/dev/null || true
    docker-compose down
    print_success "Все сервисы остановлены"
    exit 0
}

trap cleanup EXIT INT TERM

echo "===================================="
print_status "Запуск инфраструктурных сервисов..."
echo "===================================="

# 1. Eureka Discovery Server
print_status "[1/5] Запуск Eureka Discovery Server..."
java -jar infra/discovery-server/target/discovery-server-1.0-SNAPSHOT.jar > logs/discovery-server.log 2>&1 &
DISCOVERY_PID=$!
print_success "Eureka запущен (PID: $DISCOVERY_PID)"

print_status "Ожидание запуска Eureka (30 секунд)..."
for i in {1..30}; do
    sleep 1
    if curl -s http://localhost:8761 > /dev/null 2>&1; then
        print_success "Eureka доступен на порту 8761"
        break
    fi
    echo -n "."
done
echo ""

# 2. Config Server
print_status "[2/5] Запуск Config Server..."
java -jar infra/config-server/target/config-server-1.0-SNAPSHOT.jar > logs/config-server.log 2>&1 &
CONFIG_PID=$!
print_success "Config Server запущен (PID: $CONFIG_PID)"

print_status "Ожидание регистрации Config Server в Eureka (30 секунд)..."
for i in {1..30}; do
    sleep 1
    if curl -s http://localhost:8761/eureka/apps/CONFIG-SERVER 2>/dev/null | grep -q "UP"; then
        print_success "Config Server зарегистрирован в Eureka"
        break
    fi
    echo -n "."
done
echo ""

echo "===================================="
print_status "Запуск сервисов телеметрии..."
echo "===================================="

# 3. Collector
print_status "[3/5] Запуск Collector..."
java -jar telemetry/collector/target/collector-1.0-SNAPSHOT.jar > logs/collector.log 2>&1 &
COLLECTOR_PID=$!
print_success "Collector запущен (PID: $COLLECTOR_PID)"
sleep 5

# 4. Aggregator
print_status "[4/5] Запуск Aggregator..."
java -jar telemetry/aggregator/target/aggregator-1.0-SNAPSHOT.jar > logs/aggregator.log 2>&1 &
AGGREGATOR_PID=$!
print_success "Aggregator запущен (PID: $AGGREGATOR_PID)"
sleep 5

# 5. Analyzer
print_status "[5/5] Запуск Analyzer..."
java -jar telemetry/analyzer/target/analyzer-1.0-SNAPSHOT.jar > logs/analyzer.log 2>&1 &
ANALYZER_PID=$!
print_success "Analyzer запущен (PID: $ANALYZER_PID)"

echo "===================================="
print_success "🎉 ВСЕ СЕРВИСЫ ЗАПУЩЕНЫ"
echo "===================================="
echo "📊 Eureka Dashboard: http://localhost:8761"
echo "📁 Логи сохранены в папке ./logs/"
echo "===================================="
echo "Для остановки нажмите Ctrl+C"

# Показ логов в реальном времени
print_status "Отображение логов (нажмите Ctrl+C для остановки)..."
tail -f logs/discovery-server.log logs/config-server.log logs/collector.log logs/aggregator.log logs/analyzer.log