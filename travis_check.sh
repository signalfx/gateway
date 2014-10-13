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
go test -v ./... || exit 1
