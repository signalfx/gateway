#!/bin/bash
set -x
#
# ---- Check shell scripts
# 
shellcheck install.sh || exit 1
shellcheck signalfxproxy || exit 1
# Yo dawg
shellcheck travis_check.sh || exit 1

#
# ---- Check markdown
#
# Note: there is one line (a curl) that we can't help but make long
echo -e "# Ignore Header" > /tmp/ignore_header.md
cat /tmp/ignore_header.md README.md | grep -v curl | grep -v 'Build Status' | mdl || exit 1

#
# ---- Check JSON
#
set -e
# Want example config file to be valid json
python -m json.tool < exampleSfdbproxy.conf > /dev/null
set +e

#
# ---- Check go syntax
#
rm -f /tmp/a /tmp/no_100_coverage || exit 1
go install . || exit 1
find . -type f -name \*.go | grep -v '.git' | xargs -n1 -P8 gofmt -w -l -s > /tmp/a || exit 1
# I want to print it out for debugging purposes, while still existing if exist
[[ ! -s /tmp/a ]] || cat /tmp/a
[[ ! -s /tmp/a ]] || exit 1

gocyclo -over 10 . | grep -v skiptestcoverage > /tmp/a
[[ ! -s /tmp/a ]] || cat /tmp/a
[[ ! -s /tmp/a ]] || exit 1

find . -type f -name \*.go | grep -v ".git" | xargs -n1 -P8 golint > /tmp/a || exit 1
[[ ! -s /tmp/a ]] || cat /tmp/a
[[ ! -s /tmp/a ]] || exit 1

find . -type f -name \*.go | grep -v '.git' | xargs -n1 -P8 go vet > /tmp/a || exit 1
[[ ! -s /tmp/a ]] || cat /tmp/a
[[ ! -s /tmp/a ]] || exit 1

#
# ---- Check for 100% code coverage
#
go test -cover -race -covermode=atomic -parallel=8 ./... | grep -v 'skiptestcoverage' | grep -v "100.0% of statements" > /tmp/no_100_coverage
[[ ! -s /tmp/no_100_coverage ]] || cat /tmp/no_100_coverage
[[ ! -s /tmp/no_100_coverage ]] || exit 1
go test -run=none -bench=. ./... || exit 1
echo "OK!"
