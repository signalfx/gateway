#!/bin/bash

#
#
# Only use me on travis
#
#

set -x
set -e

if [ -z  "$COMMIT_SHA" ]; then
  COMMIT_SHA=$(git log -n1 --pretty=format:%H)
fi
# TODO: Automate finding BIN_NAME applications
BIN_VERSION="1.0-SNAPSHOT"
if [ -z  "$BUILDER" ]; then
  BUILDER=dev
fi

echo "{
  \"name\": \"metricproxy\",
  \"version\": \"$BIN_VERSION\",
  \"builder\": \"$BUILDER\",
  \"commit\": \"$COMMIT_SHA\"
}" > buildInfo.json

# Print out some debugging stuff to the console so we can check the build
pwd
find .

go get -u github.com/signalfx/gobuild
go get -u github.com/alecthomas/gometalinter
gometalinter --install

./scripts/gobuild.sh
