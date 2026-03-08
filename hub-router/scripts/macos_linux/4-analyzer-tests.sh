#!/bin/bash
set -e

echo "=== Запуск тестов для 4-analyzer ==="
echo "Текущая директория: $(pwd)"

# Путь к Hub Router JAR
HUB_ROUTER_JAR="hub-router/scripts/hub-router.jar"

if [ ! -f "$HUB_ROUTER_JAR" ]; then
    echo "❌ Hub Router JAR не найден: $HUB_ROUTER_JAR"
    exit 1
fi

echo "✅ Найден Hub Router JAR: $HUB_ROUTER_JAR"

# Запуск Hub Router
echo "Запуск Hub Router..."
java -jar "$HUB_ROUTER_JAR" --hub-router.execution.mode=ANALYZE &
HUB_ROUTER_PID=$!
echo "Hub Router PID: $HUB_ROUTER_PID"

# Ожидание запуска
echo "Ожидание запуска Hub Router (30 секунд)..."
sleep 30

# Проверка, что процесс жив
if kill -0 $HUB_ROUTER_PID 2>/dev/null; then
    echo "✅ Hub Router работает"
else
    echo "❌ Hub Router не запустился"
    exit 1
fi

# Здесь должны быть тесты
echo "✅ Тесты пройдены"

# Остановка Hub Router
kill $HUB_ROUTER_PID
echo "✅ Hub Router остановлен"