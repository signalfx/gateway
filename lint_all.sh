#!/bin/bash
# Lints all the golang files in this repository
git ls-files | grep '.go' | xargs -n1 golint
