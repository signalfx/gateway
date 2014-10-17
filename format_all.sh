#!/bin/bash
find . -type f | grep -v '.git' | grep '.go' | xargs -n1 go fmt
