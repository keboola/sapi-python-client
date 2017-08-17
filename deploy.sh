#!/bin/bash
set -e

docker login -u="$QUAY_USERNAME" -p="$QUAY_PASSWORD" quay.io
docker tag ${KBC_APP_REPOSITORY} quay.io/${KBC_APP_REPOSITORY}:${TRAVIS_TAG}
docker tag ${KBC_APP_REPOSITORY} quay.io/${KBC_APP_REPOSITORY}:latest
docker images
docker push quay.io/${KBC_APP_REPOSITORY}:${TRAVIS_TAG}
docker push quay.io/${KBC_APP_REPOSITORY}:latest
