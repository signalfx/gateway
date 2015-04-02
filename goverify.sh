#!/bin/bash
set -x

GOPATH=$(godep path):$GOPATH
go env
#
# ----- gofmt will check for code formatting issues
#
rm -f /tmp/a /tmp/no_100_coverage || exit 1
go install . || exit 1

git ls-files "*.go" | grep -av '.git' | grep -av 'Godep' | xargs -n1 -P8 gofmt -w -l -s > /tmp/a || exit 1
# I want to print it out for debugging purposes, while still existing if exist
[[ ! -s /tmp/a ]] || cat /tmp/a
[[ ! -s /tmp/a ]] || exit 1

#
# ---- goimports will reorg the imports
#
git ls-files "*.go" | grep -av 'Godep' | xargs -n1 -P8 goimports -w -l > /tmp/a || exit 1
[[ ! -s /tmp/a ]] || cat /tmp/a
[[ ! -s /tmp/a ]] || exit 1

#
# ---- gocyclo checks for cyclomatic complexity over 10
#
gocyclo -over 10 . | grep -av 'Godep' | grep -v skiptestcoverage > /tmp/a
[[ ! -s /tmp/a ]] || cat /tmp/a
[[ ! -s /tmp/a ]] || exit 1

#
# ---- go lint does static variable name and doc checks
#
git ls-files "*.go" | grep -av 'Godep' | xargs -n1 -P8 golint -min_confidence=.3 > /tmp/a || exit 1
[[ ! -s /tmp/a ]] || cat /tmp/a
[[ ! -s /tmp/a ]] || exit 1

#
# ---- go vet will do basic static error checking
#
git ls-files "*.go" | grep -av 'Godep' | xargs -n1 -P8 go vet > /tmp/a || exit 1
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
