#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Define custom ports to avoid conflicts
NATS_PORT=4223
NATS_MONITORING_PORT=8223

# Start the NATS server in the background
echo "Starting NATS server on port ${NATS_PORT}..."
nats-server -js -p ${NATS_PORT} -m ${NATS_MONITORING_PORT} &
NATS_PID=$!
echo "NATS server started with PID ${NATS_PID}"

# Function to clean up the NATS server
cleanup() {
  echo "Cleaning up NATS server (PID: ${NATS_PID})..."
  kill ${NATS_PID}
}

# Set a trap to run the cleanup function on script exit, ensuring the
# server is stopped even if the script fails.
trap cleanup EXIT

# Give the server a moment to start up before clients connect.
sleep 2

# Build the project's "fat jar" which includes dependencies.
echo "Building the project..."
./gradlew fatJar

# --- Run the desired benchmark ---
# You can change this variable to any benchmark you want to run,
# e.g., 'jspullsubmulti', 'reqreply', 'pubonly'
BENCHMARK_TO_RUN="jspullsub"

echo "Running the ${BENCHMARK_TO_RUN} benchmark..."
java -cp build/libs/jnats-2.21.5-SNAPSHOT-fat.jar io.nats.examples.autobench.NatsAutoBench nats://localhost:${NATS_PORT} ${BENCHMARK_TO_RUN}

echo "Benchmark finished successfully."
