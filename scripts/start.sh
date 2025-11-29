#!/bin/bash

# Start Real-Time Stock ETL Platform
# This script starts all services and streaming jobs

set -e

echo "=========================================="
echo "Starting Real-Time Stock ETL Platform"
echo "=========================================="
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker Desktop."
    exit 1
fi

echo "✓ Docker is running"
echo ""

# Check if .env file exists
if [ ! -f .env ]; then
    if [ -f .env.example ]; then
        echo "⚠️  .env file not found. Creating from .env.example..."
        cp .env.example .env
        echo "✓ .env file created"
    else
        echo "⚠️  .env file not found. Using default environment variables."
        echo "   (You can create .env manually if you need custom configuration)"
    fi
else
    echo "✓ .env file exists"
fi
echo ""

# Start Docker Compose services
echo "Starting Docker services..."
docker compose up -d

echo ""
echo "Waiting for services to be healthy (30 seconds)..."
sleep 30

echo ""
echo "=========================================="
echo "Service Status"
echo "=========================================="
docker compose ps

echo ""
echo "=========================================="
echo "Spark Streaming Jobs"
echo "=========================================="
echo ""
echo "✓ Spark streaming jobs are now AUTOMATED!"
echo "  They start automatically with docker compose up -d"
echo ""
echo "Jobs running:"
echo "  - spark-bronze-job (Bronze Layer)"
echo "  - spark-silver-job (Silver Layer)"
echo "  - spark-gold-job (Gold Layer)"
echo ""
echo "Check job status:"
echo "  docker compose ps | grep spark-"
echo ""
echo "View logs:"
echo "  docker logs spark-bronze-job"
echo "  docker logs spark-silver-job"
echo "  docker logs spark-gold-job"
echo ""
echo "Or check Spark UI: http://localhost:7004"
echo ""

echo "=========================================="
echo "Access URLs"
echo "=========================================="
echo ""
echo "  Grafana:          http://localhost:7010 (admin/admin)"
echo "  Airflow:          http://localhost:7009 (admin/admin)"
echo "  Redpanda Console: http://localhost:7003"
echo "  Spark Master UI:  http://localhost:7004"
echo ""

echo "=========================================="
echo "System Started Successfully! 🚀"
echo "=========================================="

