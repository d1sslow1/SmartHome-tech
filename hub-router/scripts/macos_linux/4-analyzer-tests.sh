#!/bin/bash
# Локальный запуск тестов Hub Router для проверки полного комплекса Collector (grpc) + Aggregator + Analyzer

set -e

JAR_PATH="$(dirname "$0")/../hub-router.jar"

# Функция для остановки Hub Router при завершении скрипта
cleanup() {
    echo "Останавливаю Hub Router..."
    kill $HUB_ROUTER_PID 2>/dev/null || true
    wait $HUB_ROUTER_PID 2>/dev/null || true
    echo "Hub Router остановлен"
}

# Устанавливаем обработчик для остановки Hub Router при выходе
trap cleanup EXIT INT TERM

if [[ "$1" == "info" ]]; then
  java -jar "$JAR_PATH" info
else
  echo "Запуск Hub Router (режим: ANALYZE) на порту 59091..."

  # Запускаем Hub Router в фоне
  java -jar "$JAR_PATH" \
    --hub-router.execution.mode=ANALYZE \
    --hub-router.execution.immediate-logging.enabled=false \
    --hub-router.execution.output.info-enabled=true \
    --hub-router.execution.output.trace-enabled=true \
    --hub-router.execution.output.console=true \
    --hub-router.execution.output.file=false \
    --hub-router.skip-summary-on-startup=false &

  HUB_ROUTER_PID=$!
  echo "Hub Router запущен с PID: $HUB_ROUTER_PID"

  # Ждем 5 секунд, чтобы Hub Router успел запуститься
  echo "Ожидание запуска Hub Router..."
  sleep 5

  # Проверяем, что процесс жив
  if kill -0 $HUB_ROUTER_PID 2>/dev/null; then
    echo "✅ Hub Router успешно запущен"
  else
    echo "❌ Ошибка запуска Hub Router"
    exit 1
  fi

  # Ждем, пока Hub Router не будет остановлен (или скрипт не завершится)
  wait $HUB_ROUTER_PID
fi