# ===== ЗАПУСК HUB ROUTER =====
echo "⏳ Запуск сервиса hub-router..."

# Путь к JAR-файлу Hub Router (проверь, где он лежит)
HUB_ROUTER_JAR="scripts/windows/hub-router.jar"

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
    
    # Ждем запуска 30 секунд
    echo "Ожидание запуска Hub Router на порту 59091..."
    sleep 30
else
    echo "❌ Hub Router JAR не найден по пути: $HUB_ROUTER_JAR"
    # Не выходим с ошибкой, просто предупреждаем
fi
# ==============================
