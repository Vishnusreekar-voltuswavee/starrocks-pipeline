#!/bin/bash

# MySQL to Iceberg to StarRocks Pipeline - Startup Script
# This script sets up the environment and starts the FastAPI application

set -e

echo "==================================="
echo "MySQL → Iceberg → StarRocks Pipeline"
echo "==================================="

# Set Spark environment variables
export SPARK_HOME=/home/voltus-wave/starrocks/spark_install/spark-3.5.0-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

# Set Java home if needed
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

echo "Environment:"
echo "  SPARK_HOME: $SPARK_HOME"
echo "  JAVA_HOME: $JAVA_HOME"
echo "  Python: $(which python3)"
echo ""

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Error: .env file not found!"
    echo "Please copy .env.example to .env and configure your credentials."
    exit 1
fi

# Activate virtual environment
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

echo "Activating virtual environment..."
source venv/bin/activate

# Install/update dependencies
echo "Installing dependencies..."
pip install -q -r requirements.txt

# Run the application
echo ""
echo "Starting FastAPI server..."
echo "  API Docs: http://localhost:8000/docs"
echo "  Health: http://localhost:8000/health"
echo "  Logs: logs/pipeline.log"
echo ""

python -m app.main
