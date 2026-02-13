#!/usr/bin/env bash
# Build Dagster webserver and daemon images with ol-orchestrate-lib
#
# Usage:
#   ./dg_deployments/build-images.sh [--push] [--registry REGISTRY] [--tag TAG]
#
# Options:
#   --push              Push images to registry after building
#   --registry REGISTRY Docker registry (default: ol-data-platform)
#   --tag TAG           Image tag (default: latest)
#
# Examples:
#   ./dg_deployments/build-images.sh
#   ./dg_deployments/build-images.sh --tag v1.0.0
#   ./dg_deployments/build-images.sh --push --registry myregistry.io/dagster --tag v1.0.0

set -euo pipefail

# Default values
REGISTRY="ol-data-platform"
TAG="latest"
PUSH=false

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --push)
      PUSH=true
      shift
      ;;
    --registry)
      REGISTRY="$2"
      shift 2
      ;;
    --tag)
      TAG="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Image names
WEBSERVER_IMAGE="${REGISTRY}/webserver:${TAG}"
DAEMON_IMAGE="${REGISTRY}/daemon:${TAG}"

echo "Building Dagster images with ol-orchestrate-lib..."
echo "  Webserver: ${WEBSERVER_IMAGE}"
echo "  Daemon: ${DAEMON_IMAGE}"
echo ""

# Build webserver
echo "▶ Building webserver image..."
docker build \
  -f dg_deployments/Dockerfile.webserver \
  -t "${WEBSERVER_IMAGE}" \
  .

echo "✓ Webserver image built: ${WEBSERVER_IMAGE}"
echo ""

# Build daemon
echo "▶ Building daemon image..."
docker build \
  -f dg_deployments/Dockerfile.daemon \
  -t "${DAEMON_IMAGE}" \
  .

echo "✓ Daemon image built: ${DAEMON_IMAGE}"
echo ""

# Push if requested
if [ "$PUSH" = true ]; then
  echo "▶ Pushing images to registry..."
  docker push "${WEBSERVER_IMAGE}"
  docker push "${DAEMON_IMAGE}"
  echo "✓ Images pushed successfully"
  echo ""
fi

echo "Build complete!"
echo ""
echo "To use these images with Dagster Helm chart, update your values.yaml:"
echo ""
echo "dagsterWebserver:"
echo "  image:"
echo "    repository: ${REGISTRY}/webserver"
echo "    tag: ${TAG}"
echo "    pullPolicy: IfNotPresent"
echo ""
echo "dagsterDaemon:"
echo "  image:"
echo "    repository: ${REGISTRY}/daemon"
echo "    tag: ${TAG}"
echo "    pullPolicy: IfNotPresent"
