#!/bin/bash
set -x

#
# ---- Check shell scripts
# 
shellcheck install.sh || exit 1
shellcheck gateway_initd || exit 1
shellcheck scripts/build.sh || exit 1
shellcheck scripts/gobuild.sh || exit 1
# Yo dawg
shellcheck scripts/travis_check.sh || exit 1

#
# ---- Check markdown
#
# Note: there is one line (a curl) that we can't help but make long
# Verify rules at https://github.com/mivok/markdownlint/blob/master/docs/RULES.md
echo -e "# Ignore Header" > /tmp/ignore_header.md
cat /tmp/ignore_header.md README.md | grep -av 'Circle CI' | grep -av curl | mdl --warnings || exit 1

#
# ---- Check JSON
#
set -e
# Want example config file to be valid json
python -m json.tool < exampleGateway.conf > /dev/null

./scripts/build.sh
