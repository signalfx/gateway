#!/bin/bash
set -x

#
# ---- Check shell scripts
# 
shellcheck install.sh || exit 1
shellcheck metricproxy || exit 1
# Yo dawg
shellcheck travis_check.sh || exit 1

#
# ---- Check markdown
#
# Note: there is one line (a curl) that we can't help but make long
echo -e "# Ignore Header" > /tmp/ignore_header.md
cat /tmp/ignore_header.md README.md | grep -av curl | grep -av 'Build Status' | mdl --warnings || exit 1

#
# ---- Check JSON
#
set -e
# Want example config file to be valid json
python -m json.tool < exampleSfdbproxy.conf > /dev/null
set +e

#
# ----- gofmt will check for code formatting issues
#
rm -f /tmp/a /tmp/no_100_coverage || exit 1
go install . || exit 1
find . -type f -name \*.go | grep -av '.git' | xargs -n1 -P8 gofmt -w -l -s > /tmp/a || exit 1
# I want to print it out for debugging purposes, while still existing if exist
[[ ! -s /tmp/a ]] || cat /tmp/a
[[ ! -s /tmp/a ]] || exit 1

#
# ---- goimports will reorg the imports
#
find . -type f -name \*.go | grep -av '.git' | xargs -n1 -P8 goimports -w -l > /tmp/a || exit 1
[[ ! -s /tmp/a ]] || cat /tmp/a
[[ ! -s /tmp/a ]] || exit 1

#
# ---- gocyclo checks for cyclomatic complexity over 10
#
gocyclo -over 10 . | grep -v skiptestcoverage > /tmp/a
[[ ! -s /tmp/a ]] || cat /tmp/a
[[ ! -s /tmp/a ]] || exit 1

#
# ---- go lint does static variable name and doc checks
#
find . -type f -name \*.go | grep -av ".git" | xargs -n1 -P8 golint -min_confidence=.3 > /tmp/a || exit 1
[[ ! -s /tmp/a ]] || cat /tmp/a
[[ ! -s /tmp/a ]] || exit 1

#
# ---- go vet will do basic static error checking
#
find . -type f -name \*.go | grep -av '.git' | xargs -n1 -P8 go vet > /tmp/a || exit 1
[[ ! -s /tmp/a ]] || cat /tmp/a
[[ ! -s /tmp/a ]] || exit 1

#
# ---- Check for 100% code coverage and data races.  Increase possiblity of
#      races with cpu 2.  Timeout long running tests.  Should really be 1s, but
#      want to give travis-ci some time.
#
go test -cover -covermode atomic -race -parallel=8 -timeout 3s -cpu 4  ./... | grep -av 'skiptestcoverage' | grep -av "100.0% of statements" > /tmp/no_100_coverage
[[ ! -s /tmp/no_100_coverage ]] || cat /tmp/no_100_coverage
[[ ! -s /tmp/no_100_coverage ]] || exit 1

#
# ---- Run benchmarks only
#
go test -run=none -bench=. -benchtime 10ms ./... || exit 1
echo "OK!"
