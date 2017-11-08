#!/bin/bash

#
#
# Only use me on travis
#
#

set -x
set -e

# Print out some debugging stuff to the console so we can check the build
pwd
find .

go get -u github.com/signalfx/gobuild
go get -u github.com/alecthomas/gometalinter
gometalinter --install

./scripts/gobuild.sh
