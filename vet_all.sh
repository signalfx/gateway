#!/bin/bash
# Runs go's vet checker on all golang files in this repository
find . -type f | grep -v '.git' | grep '.go' | xargs -n1 go vet
