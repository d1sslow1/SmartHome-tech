#!/bin/bash
# start-telemetry.sh - полный запуск всех сервисов

set -e

echo "Проверка наличия JAR-файлов и запуск нужных сервисов..."

# Проверка JAR-файлов
if [ -f "./telemetry/collector/target/collector-1.0-SNAPSHOT.jar" ]; then
  echo "Collector: ./telemetry/collector/target/collector-1.0-SNAPSHOT.jar"
else
  echo "❌ Collector JAR не найден"
  exit 1
fi

if [ -f "./telemetry/aggregator/target/aggregator-1.0-SNAPSHOT.jar" ]; then
  echo "Aggregator: ./telemetry/aggregator/target/aggregator-1.0-SNAPSHOT.jar"
else
  echo "❌ Aggregator JAR не найден"
  exit 1
fi

if [ -f "./telemetry/analyzer/target/analyzer-1.0-SNAPSHOT.jar" ]; then
  echo "Analyzer: ./telemetry/analyzer/target/analyzer-1.0-SNAPSHOT.jar"
else
  echo "❌ Analyzer JAR не найден"
  exit 1
fi

# Текущая ветка
BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo "Текущая ветка: $BRANCH"

# Функция для остановки всех процессов
cleanup() {
    echo "Останавливаю все сервисы..."
    kill $COLLECTOR_PID $AGGREGATOR_PID $ANALYZER_PID $HUB_ROUTER_PID 2>/dev/null || true
    echo "Все сервисы остановлены"
}

trap cleanup EXIT INT TERM

# ===== ЗАПУСК HUB ROUTER =====
echo "⏳ Запуск сервиса hub-router..."

# Ищем hub-router.jar в разных местах
HUB_ROUTER_JAR=""
if [ -f "scripts/hub-router.jar" ]; then
    HUB_ROUTER_JAR="scripts/hub-router.jar"
elif [ -f "scripts/windows/hub-router.jar" ]; then
    HUB_ROUTER_JAR="scripts/windows/hub-router.jar"
elif [ -f "hub-router.jar" ]; then
    HUB_ROUTER_JAR="hub-router.jar"
fi

if [ -n "$HUB_ROUTER_JAR" ]; then
    echo "Найден Hub Router JAR: $HUB_ROUTER_JAR"

    # Запускаем Hub Router в фоне
    java -jar "$HUB_ROUTER_JAR" \
        --hub-router.execution.mode=ANALYZE \
        --hub-router.execution.immediate-logging.enabled=false \
        --hub-router.execution.output.info-enabled=true \
        --hub-router.execution.output.trace-enabled=true \
        --hub-router.execution.output.console=true \
        --hub-router.execution.output.file=false \
        --hub-router.skip-summary-on-startup=false &

    HUB_ROUTER_PID=$!
    echo "✅ Сервис hub-router успешно запущен (PID: $HUB_ROUTER_PID)"

    # Ждем 30 секунд для полного запуска
    echo "Ожидание полного запуска Hub Router (30 секунд)..."
    for i in {1..30}; do
        sleep 1
        echo -n "."
        if [ $i -eq 30 ]; then
            echo ""
            echo "✅ Ожидание завершено"
        fi
    done
else
    echo "⚠️ Hub Router JAR не найден, продолжаем без него"
fi
# ==============================

# Запуск collector
echo "⏳ Запуск сервиса collector..."
java -jar ./telemetry/collector/target/collector-1.0-SNAPSHOT.jar &
COLLECTOR_PID=$!
echo "✅ Сервис collector успешно запущен."

# Запуск aggregator
echo "⏳ Запуск сервиса aggregator..."
java -jar ./telemetry/aggregator/target/aggregator-1.0-SNAPSHOT.jar &
AGGREGATOR_PID=$!
echo "✅ Сервис aggregator успешно запущен."

# Запуск analyzer
echo "⏳ Запуск сервиса analyzer..."
java -jar ./telemetry/analyzer/target/analyzer-1.0-SNAPSHOT.jar &
ANALYZER_PID=$!
echo "✅ Сервис analyzer успешно запущен."

# Ждем запуска всех сервисов
echo "Ожидание стабилизации всех сервисов..."
sleep 10

echo "✅ Все сервисы запущены и готовы к работе"

# Бесконечное ожидание (чтобы сервисы продолжали работать)
wait