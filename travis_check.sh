#!/bin/bash
set -x
rm -f /tmp/a || exit 1
./format_all.sh > /tmp/a
[[ ! -s /tmp/a ]] || cat /tmp/a
[[ ! -s /tmp/a ]] || exit 1
./lint_all.sh > /tmp/a
[[ ! -s /tmp/a ]] || cat /tmp/a
[[ ! -s /tmp/a ]] || exit 1
./vet_all.sh > /tmp/a
[[ ! -s /tmp/a ]] || cat /tmp/a
[[ ! -s /tmp/a ]] || exit 1
mdl README.md || exit 1
go test -cover -v ./... || exit 1

rm /tmp/no_100_coverage
go test -cover ./... | grep -v "github.com/signalfuse/signalfxproxy\t" | grep -v "100.0% of statements" > /tmp/no_100_coverage
[[ ! -s /tmp/no_100_coverage ]] || cat /tmp/no_100_coverage
[[ ! -s /tmp/no_100_coverage ]] || exit 1
echo "OK!"
