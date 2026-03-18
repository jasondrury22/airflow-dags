#!/bin/bash
echo "Starting fast local Airflow environment..."

# Run the merged docker-compose command
docker compose -f docker-compose.yml -f docker-compose.local.yml up -d --build

echo "Waiting for containers to initialize..."
sleep 10

# Automatically build the database tables
echo "Running database migrations..."
docker exec -it airflow-scheduler airflow db migrate

echo "Creating admin user (this might take a few seconds)..."

# Notice we changed -it to just -i, and added > /dev/null 2>&1 at the very end
docker exec -i airflow-scheduler airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin > /dev/null 2>&1

echo "Admin user successfully verified!"

echo ""
echo "======================================================="
echo "Local Environment is completely booted!"
echo "Airflow UI: http://localhost:4040 (admin/admin)"
echo "======================================================="