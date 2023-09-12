#!/bin/bash

_TARGET_IMAGE=solace-ecs-scaler

if [[ -z $2 || "$1" = "-h" || "$1" = "--help" ]]; then
  echo
  echo "Script to run docker container for image: solace-ecs-scaler:TAG"
  echo "Usage: "
  echo "  ./runDockerContainer.sh CONFIG_FILE ENV_FILE [ TAG ]"
  echo
  echo "  - CONFIG_FILE (yaml configuration) is required"
  echo "  - ENV_FILE Environment file with AWS Credentials is requied"
  echo "  - CONFIG_FILE and ENV_FILE should be specified as absolute paths"
  echo "    or with ./ prefix to work with Docker"
  echo "  - TAG is assumed to be 1.0 if not specified"
  exit 0
fi

IMAGE_TAG=1.0
if [[ x"$3" != "x" ]]; then
  IMAGE_TAG=$3
fi

docker run -d -v $1:/opt/ecs-scaler/config.yaml --env-file $2 --name solace-ecs-scaler $_TARGET_IMAGE:$IMAGE_TAG

exit $?
