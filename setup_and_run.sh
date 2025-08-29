#!/bin/bash

echo "Setting up On-Premise Kafka Environment"

# Create project structure
mkdir -p kafka-onprem/config
cd kafka-onprem

# Install Python dependencies
#this assumes you have Python and pip installed
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Start Kafka with Docker
echo "Starting Kafka cluster..."
docker-compose up -d

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 30

# Check if Kafka is running
if docker ps | grep -q kafka; then
    echo "Kafka cluster is running!"
    echo "Kafka UI available at: http://localhost:8080"
    echo ""
    echo "You can now run:"
    echo "  python producer.py  (to generate events)"
    echo "  python consumer.py  (to process events)"
else
    echo "Error: Kafka cluster failed to start"
    exit 1
fi