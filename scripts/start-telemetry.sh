#!/bin/bash
set -e

echo "===================================="
echo "🚀 STARTING ALL SERVICES"
echo "===================================="

# 1. START DISCOVERY SERVER (EUREKA)
echo "Starting Discovery Server on port 8761..."
java -jar infra/discovery-server/target/discovery-server-1.0-SNAPSHOT.jar &
DISCOVERY_PID=$!

echo "Waiting for Eureka (max 30 sec)..."
for i in {1..30}; do
    if curl -s http://localhost:8761 > /dev/null 2>&1; then
        echo "✅ Eureka is running"
        break
    fi
    sleep 1
done

# 2. START CONFIG SERVER
echo "Starting Config Server..."
java -jar infra/config-server/target/config-server-1.0-SNAPSHOT.jar &
CONFIG_PID=$!

echo "Waiting for Config Server (15 sec)..."
sleep 15

# 3. START COLLECTOR
echo "Starting Collector..."
java -jar telemetry/collector/target/collector-1.0-SNAPSHOT.jar &
COLLECTOR_PID=$!
sleep 5

# 4. START AGGREGATOR
echo "Starting Aggregator..."
java -jar telemetry/aggregator/target/aggregator-1.0-SNAPSHOT.jar &
AGGREGATOR_PID=$!
sleep 5

# 5. START ANALYZER
echo "Starting Analyzer..."
java -jar telemetry/analyzer/target/analyzer-1.0-SNAPSHOT.jar &
ANALYZER_PID=$!
sleep 5

# 6. START SHOPPING STORE
echo "Starting Shopping Store..."
java -jar commerce/shopping-store/target/shopping-store-1.0-SNAPSHOT.jar &
STORE_PID=$!
sleep 5

# 7. START WAREHOUSE
echo "Starting Warehouse..."
java -jar commerce/warehouse/target/warehouse-1.0-SNAPSHOT.jar &
WAREHOUSE_PID=$!
sleep 5

# 8. START SHOPPING CART
echo "Starting Shopping Cart..."
java -jar commerce/shopping-cart/target/shopping-cart-1.0-SNAPSHOT.jar &
CART_PID=$!

echo ""
echo "===================================="
echo "✅ ALL SERVICES STARTED"
echo "===================================="
echo "Eureka: http://localhost:8761"
echo ""
echo "Waiting for services to register..."

# Wait for all services to be ready
sleep 20

echo ""
echo "Checking registered services:"
curl -s http://localhost:8761/eureka/apps | grep -E "<name>" | sort -u

echo ""
echo "Services are running. Press Ctrl+C to stop."

# Keep running
wait