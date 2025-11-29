# Spark Image Fix

## Issue
The `bitnami/spark` image is not available on Docker Hub. We need to use an alternative.

## Solution Options

### Option 1: Use Official Apache Spark Image (Recommended)
The official `apache/spark` image is available and works well.

### Option 2: Build Custom Spark Image
Use the provided `Dockerfile.spark` to build a custom image with Delta Lake support.

## Quick Fix - Use Official Apache Spark

The docker-compose.yml has been updated to use `apache/spark:3.4.1` which is available and tested.

## If You Still See Errors

1. **Pull the image manually:**
   ```bash
   docker pull apache/spark:3.4.1
   ```

2. **Or build custom image:**
   ```bash
   docker build -f Dockerfile.spark -t stock-etl-spark:latest .
   ```
   
   Then update docker-compose.yml to use:
   ```yaml
   image: stock-etl-spark:latest
   ```

## Current Status

The project is configured to use `apache/spark:3.4.1` which is the official Apache Spark image and includes all necessary components.

