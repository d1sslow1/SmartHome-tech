#!/bin/bash
# start-telemetry.sh - запуск сервисов телеметрии

set -e

echo "Проверка наличия JAR-файлов и запуск нужных сервисов..."

# Проверка наличия JAR-файлов
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

# Функция для остановки всех процессов при завершении
cleanup() {
    echo "Останавливаю все сервисы..."
    kill $COLLECTOR_PID $AGGREGATOR_PID $ANALYZER_PID $HUB_ROUTER_PID 2>/dev/null || true
    echo "Все сервисы остановлены"
}

trap cleanup EXIT INT TERM

# ===== ЗАПУСК HUB ROUTER =====
echo "⏳ Запуск сервиса hub-router..."

# Путь к JAR-файлу Hub Router
HUB_ROUTER_JAR="scripts/hub-router.jar"

if [ -f "$HUB_ROUTER_JAR" ]; then
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

    # Ждем запуска Hub Router - ЭТО ГЛАВНОЕ ИЗМЕНЕНИЕ
    echo "Ожидание запуска Hub Router на порту 59090..."

    # Ждем 30 секунд вместо 10
    for i in {1..30}; do
        sleep 1
        if nc -z localhost 59090 2>/dev/null; then
            echo "✅ Hub Router доступен на порту 59090"
            break
        fi
        if [ $i -eq 30 ]; then
            echo "❌ Hub Router не запустился за 30 секунд"
            exit 1
        fi
        echo -n "."
    done
else
    echo "❌ Hub Router JAR не найден по пути: $HUB_ROUTER_JAR"
    exit 1
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

# Финальная проверка
echo "Проверка всех запущенных сервисов..."
sleep 5

if kill -0 $COLLECTOR_PID 2>/dev/null && \
   kill -0 $AGGREGATOR_PID 2>/dev/null && \
   kill -0 $ANALYZER_PID 2>/dev/null && \
   kill -0 $HUB_ROUTER_PID 2>/dev/null; then
    echo "✅ Все сервисы успешно запущены и работают"
else
    echo "❌ Один из сервисов не работает"
    exit 1
fi

# Бесконечное ожидание (чтобы сервисы продолжали работать)
echo "Все сервисы запущены. Ожидание команд..."
wait