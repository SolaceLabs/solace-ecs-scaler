#!/bin/bash

_TARGET_IMAGE=solace-ecs-scaler

if [[ "$1" = "-h" || "$1" = "--help" ]]; then
  echo
  echo "Builds docker image for Solace ECS Scaler"
  echo "Usage: "
  echo "  ./buildDockerImage.sh [ TAG ]"
  echo
  echo "  - TAG will be 1.0 if not specified"
fi

IMAGE_TAG=1.0
if [[ x"$1" != "x" ]]; then
  IMAGE_TAG=$1
else
  echo "Defaulting Image Tag to: 1.0"
fi

docker build . -t $_TARGET_IMAGE:$IMAGE_TAG

if [[ $? -ne 0 ]]; then
  echo "*** Failed to create the docker image ***"
  exit 1
else
  echo
  echo "Created Docker Image: $_TARGET_IMAGE"
fi

exit 0
