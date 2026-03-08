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
NC='\033[0m' # No Color

# Функция для вывода с цветом
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

# Проверка наличия JAR-файлов
print_status "Проверка JAR-файлов..."

if [ ! -f "./telemetry/collector/target/collector-1.0-SNAPSHOT.jar" ]; then
    print_error "Collector JAR не найден"
    print_warning "Выполните сборку проекта: mvn clean install -DskipTests"
    exit 1
else
    print_success "Collector: ./telemetry/collector/target/collector-1.0-SNAPSHOT.jar"
fi

if [ ! -f "./telemetry/aggregator/target/aggregator-1.0-SNAPSHOT.jar" ]; then
    print_error "Aggregator JAR не найден"
    exit 1
else
    print_success "Aggregator: ./telemetry/aggregator/target/aggregator-1.0-SNAPSHOT.jar"
fi

if [ ! -f "./telemetry/analyzer/target/analyzer-1.0-SNAPSHOT.jar" ]; then
    print_error "Analyzer JAR не найден"
    exit 1
else
    print_success "Analyzer: ./telemetry/analyzer/target/analyzer-1.0-SNAPSHOT.jar"
fi

if [ ! -f "./hub-router/scripts/hub-router.jar" ]; then
    print_error "Hub Router JAR не найден по пути: ./hub-router/scripts/hub-router.jar"
    exit 1
else
    print_success "Hub Router: ./hub-router/scripts/hub-router.jar"
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
docker-compose down 2>/dev/null || true
docker-compose up -d

print_status "Ожидание запуска контейнеров (30 секунд)..."
sleep 30

# Проверка контейнеров
if ! docker ps | grep -q "kafka"; then
    print_error "Kafka не запустилась"
    exit 1
else
    print_success "Kafka запущена"
fi

if ! docker ps | grep -q "postgres"; then
    print_error "PostgreSQL не запустился"
    exit 1
else
    print_success "PostgreSQL запущен"
fi

echo ""

# Функция для остановки всех процессов
cleanup() {
    echo ""
    print_warning "Останавливаю все сервисы..."

    if [ ! -z "$COLLECTOR_PID" ]; then
        kill $COLLECTOR_PID 2>/dev/null || true
        print_success "Collector остановлен (PID: $COLLECTOR_PID)"
    fi

    if [ ! -z "$AGGREGATOR_PID" ]; then
        kill $AGGREGATOR_PID 2>/dev/null || true
        print_success "Aggregator остановлен (PID: $AGGREGATOR_PID)"
    fi

    if [ ! -z "$HUB_ROUTER_PID" ]; then
        kill $HUB_ROUTER_PID 2>/dev/null || true
        print_success "Hub Router остановлен (PID: $HUB_ROUTER_PID)"
    fi

    if [ ! -z "$ANALYZER_PID" ]; then
        kill $ANALYZER_PID 2>/dev/null || true
        print_success "Analyzer остановлен (PID: $ANALYZER_PID)"
    fi

    print_warning "Останавливаю Docker контейнеры..."
    docker-compose down

    print_success "Все сервисы остановлены"
    exit 0
}

trap cleanup EXIT INT TERM

echo "===================================="
print_status "Запуск сервисов..."
echo "===================================="
echo ""

# ===== 1. COLLECTOR =====
print_status "[1/4] Запуск Collector (REST API на 8081, gRPC на 59091)..."
java -jar ./telemetry/collector/target/collector-1.0-SNAPSHOT.jar \
    --server.port=8081 \
    --grpc.server.port=59091 > logs/collector.log 2>&1 &
COLLECTOR_PID=$!
print_success "Collector запущен (PID: $COLLECTOR_PID)"

print_status "Ожидание запуска Collector на порту 8081..."
for i in {1..15}; do
    sleep 1
    if nc -z localhost 8081 2>/dev/null; then
        print_success "Collector доступен на порту 8081"
        break
    fi
    if [ $i -eq 15 ]; then
        print_warning "Collector не отвечает, но продолжаем..."
    fi
    echo -n "."
done
echo ""

# ===== 2. AGGREGATOR =====
print_status "[2/4] Запуск Aggregator..."
java -jar ./telemetry/aggregator/target/aggregator-1.0-SNAPSHOT.jar > logs/aggregator.log 2>&1 &
AGGREGATOR_PID=$!
print_success "Aggregator запущен (PID: $AGGREGATOR_PID)"
sleep 3

# ===== 3. HUB ROUTER =====
print_status "[3/4] Запуск Hub Router на порту 59090..."
java -jar ./hub-router/scripts/hub-router.jar \
    --hub-router.execution.mode=ANALYZE \
    --grpc.server.port=59090 \
    --hub-router.execution.immediate-logging.enabled=false \
    --hub-router.execution.output.info-enabled=true \
    --hub-router.execution.output.trace-enabled=true \
    --hub-router.execution.output.console=true \
    --hub-router.skip-summary-on-startup=false > logs/hub-router.log 2>&1 &
HUB_ROUTER_PID=$!
print_success "Hub Router запущен (PID: $HUB_ROUTER_PID)"

print_status "Ожидание запуска Hub Router на порту 59090..."
for i in {1..20}; do
    sleep 1
    if nc -z localhost 59090 2>/dev/null; then
        print_success "Hub Router доступен на порту 59090"
        break
    fi
    if [ $i -eq 20 ]; then
        print_error "Hub Router не запустился за 20 секунд"
        exit 1
    fi
    echo -n "."
done
echo ""

# ===== 4. ANALYZER =====
print_status "[4/4] Запуск Analyzer..."
java -jar ./telemetry/analyzer/target/analyzer-1.0-SNAPSHOT.jar > logs/analyzer.log 2>&1 &
ANALYZER_PID=$!
print_success "Analyzer запущен (PID: $ANALYZER_PID)"

# Финальная проверка
echo ""
print_status "Финальная проверка всех сервисов..."
sleep 5

ALL_RUNNING=true

if kill -0 $COLLECTOR_PID 2>/dev/null; then
    print_success "Collector работает (PID: $COLLECTOR_PID)"
else
    print_error "Collector не работает"
    ALL_RUNNING=false
fi

if kill -0 $AGGREGATOR_PID 2>/dev/null; then
    print_success "Aggregator работает (PID: $AGGREGATOR_PID)"
else
    print_error "Aggregator не работает"
    ALL_RUNNING=false
fi

if kill -0 $HUB_ROUTER_PID 2>/dev/null; then
    print_success "Hub Router работает (PID: $HUB_ROUTER_PID)"
else
    print_error "Hub Router не работает"
    ALL_RUNNING=false
fi

if kill -0 $ANALYZER_PID 2>/dev/null; then
    print_success "Analyzer работает (PID: $ANALYZER_PID)"
else
    print_error "Analyzer не работает"
    ALL_RUNNING=false
fi

echo "===================================="
if [ "$ALL_RUNNING" = true ]; then
    print_success "🎉 ВСЕ СЕРВИСЫ УСПЕШНО ЗАПУЩЕНЫ"
    echo "===================================="
    echo "📊 Информация о сервисах:"
    echo "  Collector:   REST API http://localhost:8081"
    echo "               gRPC порт 59091"
    echo "  Aggregator:  Kafka consumer"
    echo "  Hub Router:  gRPC порт 59090"
    echo "  Analyzer:    PostgreSQL :5432, Kafka consumer"
    echo "===================================="
    echo "📁 Логи сохранены в папке ./logs/"
    echo "===================================="
    echo "Для остановки нажмите Ctrl+C"
else
    print_error "Один из сервисов не работает"
    exit 1
fi

# Создание папки для логов, если её нет
mkdir -p logs

# Показываем логи в реальном времени
print_status "Показ логов (нажмите Ctrl+C для остановки)..."
tail -f logs/*.log