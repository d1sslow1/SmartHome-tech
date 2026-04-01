#!/bin/bash
set -e

echo "===================================="
echo "STARTING ALL SERVICES"
echo "===================================="

# Функция для проверки доступности порта
wait_for_port() {
    local port=$1
    local service=$2
    local max_attempts=30
    local attempt=1

    echo "Waiting for $service on port $port..."
    while ! nc -z localhost $port > /dev/null 2>&1; do
        if [ $attempt -ge $max_attempts ]; then
            echo "❌ $service not available after $max_attempts attempts"
            return 1
        fi
        echo "Attempt $attempt: $service not ready yet..."
        sleep 2
        ((attempt++))
    done
    echo "✅ $service is ready on port $port"
    return 0
}

# 1. START DISCOVERY SERVER
echo ""
echo "=== Starting Discovery Server (Eureka) ==="
java -jar infra/discovery-server/target/discovery-server-1.0-SNAPSHOT.jar > logs/discovery.log 2>&1 &
DISCOVERY_PID=$!
echo "Discovery Server PID: $DISCOVERY_PID"

# Wait for Eureka
wait_for_port 8761 "Eureka"

# 2. START CONFIG SERVER
echo ""
echo "=== Starting Config Server ==="
java -jar infra/config-server/target/config-server-1.0-SNAPSHOT.jar > logs/config.log 2>&1 &
CONFIG_PID=$!
echo "Config Server PID: $CONFIG_PID"

# Wait for Config Server to register with Eureka
sleep 15

# 3. START COLLECTOR
echo ""
echo "=== Starting Collector ==="
java -jar telemetry/collector/target/collector-1.0-SNAPSHOT.jar > logs/collector.log 2>&1 &
COLLECTOR_PID=$!
echo "Collector PID: $COLLECTOR_PID"

# Wait for Collector gRPC port
wait_for_port 59091 "Collector (gRPC)"

# 4. START AGGREGATOR
echo ""
echo "=== Starting Aggregator ==="
java -jar telemetry/aggregator/target/aggregator-1.0-SNAPSHOT.jar > logs/aggregator.log 2>&1 &
AGGREGATOR_PID=$!
echo "Aggregator PID: $AGGREGATOR_PID"
sleep 5

# 5. START ANALYZER
echo ""
echo "=== Starting Analyzer ==="
java -jar telemetry/analyzer/target/analyzer-1.0-SNAPSHOT.jar > logs/analyzer.log 2>&1 &
ANALYZER_PID=$!
echo "Analyzer PID: $ANALYZER_PID"
sleep 5

# 6. START SHOPPING STORE
echo ""
echo "=== Starting Shopping Store ==="
java -jar commerce/shopping-store/target/shopping-store-1.0-SNAPSHOT.jar > logs/shopping-store.log 2>&1 &
STORE_PID=$!
echo "Shopping Store PID: $STORE_PID"
sleep 5

# 7. START WAREHOUSE
echo ""
echo "=== Starting Warehouse ==="
java -jar commerce/warehouse/target/warehouse-1.0-SNAPSHOT.jar > logs/warehouse.log 2>&1 &
WAREHOUSE_PID=$!
echo "Warehouse PID: $WAREHOUSE_PID"
sleep 5

# 8. START SHOPPING CART
echo ""
echo "=== Starting Shopping Cart ==="
java -jar commerce/shopping-cart/target/shopping-cart-1.0-SNAPSHOT.jar > logs/shopping-cart.log 2>&1 &
CART_PID=$!
echo "Shopping Cart PID: $CART_PID"

echo ""
echo "===================================="
echo "✅ ALL SERVICES STARTED"
echo "===================================="
echo ""
echo "Registered services in Eureka:"
curl -s http://localhost:8761/eureka/apps | grep -E "<name>" | sort -u || echo "No services registered yet"
echo ""

# Keep running
wait