#!/bin/bash

set -euo pipefail
set -x

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# prints out the tag you should use for docker images, only doing "latest"
# on the release branch, but otherwise using the circle tag or branch as
# the tag on docker.  Suffix the tag with DOCKER_TAG_SUFFIX if set.
function docker_tag() {
  DOCKER_TAG="${DOCKER_TAG-${CIRCLE_TAG-${CIRCLE_BRANCH}}}${DOCKER_TAG_SUFFIX-}"
  DOCKER_TAG=$(echo "$DOCKER_TAG" | sed -e 's#.*/##')
  if [ -z "$DOCKER_TAG" ]; then
    echo -n "unknown"
    return 1
  fi
  if [ "$DOCKER_TAG" = "latest" ]; then
    echo -n "latest-branch"
    return
  fi
  if [ "$DOCKER_TAG" = "release" ]; then
    echo -n "latest"
    return
  fi
  echo -n "$DOCKER_TAG"
}

docker build -t quay.io/signalfx/gateway:$(docker_tag) $SCRIPT_DIR/..

if [[ "DOCKER_PUSH" == "1" ]]; then
    docker push quay.io/signalfx/gateway:$(docker_tag)
fi
