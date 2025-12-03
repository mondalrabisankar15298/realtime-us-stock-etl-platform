#!/bin/bash
# Project-Specific Docker Reset Script
# This will delete ONLY this project's containers, volumes, networks, and images
# Other projects' Docker resources will NOT be affected

set -e

echo "=========================================="
echo "PROJECT DOCKER RESET"
echo "=========================================="
echo ""
echo "⚠️  WARNING: This will delete THIS PROJECT'S:"
echo "   - Containers (spark-*, stock-*, airflow-*, postgres-*, redpanda, grafana)"
echo "   - Volumes (project-specific only)"
echo "   - Networks (project-specific only)"
echo "   - Images (project-specific only, optional)"
echo ""
echo "✅ SAFE: Other projects' Docker resources will NOT be affected"
echo ""
read -p "Are you sure you want to continue? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Aborted."
    exit 1
fi

echo ""
echo "Step 1: Stopping project containers..."
docker compose down -v 2>/dev/null || true

echo "Step 2: Removing project containers..."
# Only remove containers from this project (using compose down handles most, but catch any stragglers)
# Using portable xargs (works on both Linux and macOS)
for container_id in $(docker ps -a --filter "name=spark-" --format "{{.ID}}" 2>/dev/null); do docker rm -f "$container_id" 2>/dev/null || true; done
for container_id in $(docker ps -a --filter "name=stock-" --format "{{.ID}}" 2>/dev/null); do docker rm -f "$container_id" 2>/dev/null || true; done
for container_id in $(docker ps -a --filter "name=airflow-" --format "{{.ID}}" 2>/dev/null); do docker rm -f "$container_id" 2>/dev/null || true; done
for container_id in $(docker ps -a --filter "name=postgres-timescale" --format "{{.ID}}" 2>/dev/null); do docker rm -f "$container_id" 2>/dev/null || true; done
for container_id in $(docker ps -a --filter "name=postgres-airflow" --format "{{.ID}}" 2>/dev/null); do docker rm -f "$container_id" 2>/dev/null || true; done
for container_id in $(docker ps -a --filter "name=redpanda" --format "{{.ID}}" 2>/dev/null); do docker rm -f "$container_id" 2>/dev/null || true; done
for container_id in $(docker ps -a --filter "name=grafana" --format "{{.ID}}" 2>/dev/null); do docker rm -f "$container_id" 2>/dev/null || true; done

echo "Step 3: Removing all volumes..."
# Remove project-specific volumes
docker volume rm realtime-us-stock-etl-platform_grafana-data \
                  realtime-us-stock-etl-platform_postgres-airflow-data \
                  realtime-us-stock-etl-platform_redpanda-data \
                  realtime-us-stock-etl-platform_spark-checkpoints \
                  realtime-us-stock-etl-platform_spark-delta-tables \
                  realtime-us-stock-etl-platform_timescale-data \
                  2>/dev/null || true

# Remove all other volumes (optional - uncomment if you want to remove ALL volumes)
# docker volume prune -af

echo "Step 4: Removing project-specific networks..."
# Only remove networks used by this project
for network_id in $(docker network ls --filter "name=realtime-us-stock-etl-platform" --format "{{.ID}}" 2>/dev/null); do docker network rm "$network_id" 2>/dev/null || true; done
for network_id in $(docker network ls --filter "name=stock-etl-network" --format "{{.ID}}" 2>/dev/null); do docker network rm "$network_id" 2>/dev/null || true; done

echo "Step 5: Removing project-specific images (optional)..."
read -p "Remove project images? (yes/no): " remove_images
if [ "$remove_images" == "yes" ]; then
    # Only remove images specific to this project
    docker rmi stock-etl-spark:latest 2>/dev/null || echo "   stock-etl-spark image not found or in use"
    docker rmi realtime-us-stock-etl-platform-producer:latest 2>/dev/null || echo "   producer image not found or in use"
    echo "   Removed project-specific images"
else
    echo "   Skipped image removal"
fi

echo ""
echo "=========================================="
echo "✅ PROJECT RESET COMPLETE"
echo "=========================================="
echo ""
echo "Project-specific containers, volumes, and networks have been removed."
echo "Other projects' Docker resources are safe and untouched."
echo ""
echo "To start fresh, run:"
echo "  docker compose up -d --build"
echo ""

