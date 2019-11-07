#!/bin/bash

set -x
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ -z  "$BUILDER" ]; then
  BUILDER=dev
fi

BUILD_VERSION=$(git describe --tags HEAD)
COMMIT_SHA=$(git log -n1 --pretty=format:%H)

LD_FLAGS="-X main.Version=$BUILD_VERSION -X main.BuildDate=$(date --rfc-3339=seconds | sed 's/ /T/') -s -w"
CGO_ENABLED=0 go build -ldflags "$LD_FLAGS" -v -installsuffix . -o gateway
echo "{
    \"name\": \"gateway\",
    \"version\": \"$BUILD_VERSION\",
    \"builder\": \"$BUILDER\",
    \"commit\": \"$COMMIT_SHA\"
  }" > $SCRIPT_DIR/../buildInfo.json

# Print out some debugging stuff to the console so we can check the build
pwd
find .
cat $SCRIPT_DIR/../buildInfo.json
