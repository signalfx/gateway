#!/bin/bash
# Runs go's vet checker on all golang files in this repository
git ls-files | grep '.go' | xargs -n1 go vet
