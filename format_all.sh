#!/bin/bash
git ls-files | grep go | xargs -n1 go fmt
