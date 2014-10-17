#!/bin/bash
# Lints all the golang files in this repository
find . -type f | grep -v ".git" | grep '.go' | xargs -n1 golint
