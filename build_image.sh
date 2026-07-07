#!/bin/bash
set -e

echo "Building local/ingestion:latest with podman..."
podman build -t local/ingestion:latest .

echo "Loading the image into the k3s containerd registry..."
podman save local/ingestion:latest | sudo k3s ctr images import -
echo "Done!"
