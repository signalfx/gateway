#!/bin/bash
set -x
shellcheck install.sh || exit 1
shellcheck signalfxproxy || exit 1
shellcheck travis_check.sh || exit 1
set -e
python -m json.tool < exampleSfdbproxy.conf
set +e

rm -f /tmp/a || exit 1
go install . || exit 1
find . -type f | grep -v '.git' | grep '.go' | xargs -n1 -P8 go fmt > /tmp/a || exit 1
[[ ! -s /tmp/a ]] || cat /tmp/a
[[ ! -s /tmp/a ]] || exit 1
find . -type f | grep -v ".git" | grep '.go' | xargs -n1 -P8 golint > /tmp/a || exit 1
[[ ! -s /tmp/a ]] || cat /tmp/a
[[ ! -s /tmp/a ]] || exit 1
find . -type f | grep -v '.git' | grep '.go' | xargs -n1 -P8 go vet > /tmp/a || exit 1
[[ ! -s /tmp/a ]] || cat /tmp/a
[[ ! -s /tmp/a ]] || exit 1
mdl README.md || exit 1
rm -f /tmp/no_100_coverage
go test -cover -covermode=atomic -parallel=8 ./... | grep -v "100.0% of statements" > /tmp/no_100_coverage
[[ ! -s /tmp/no_100_coverage ]] || cat /tmp/no_100_coverage
[[ ! -s /tmp/no_100_coverage ]] || exit 1
echo "OK!"
