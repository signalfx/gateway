#!/bin/bash
set -ex

CIRCLEUTIL_TAG="v1.41"
DEFAULT_GOLANG_VERSION="1.11.1"

export GOLANG_VERSION="1.11.1"
export GOROOT="$HOME/go_circle"
export GOPATH="$HOME/.go_circle"
export GOPATH_INTO="$HOME/lints"
export PATH="$GOROOT/bin:$GOPATH/bin:$GOPATH_INTO:$PATH"
export DOCKER_STORAGE="$HOME/docker_images"
export IMPORT_PATH="github.com/$CIRCLE_PROJECT_USERNAME/$CIRCLE_PROJECT_REPONAME"

GO_COMPILER_PATH="$HOME/gover"
SRC_PATH="$GOPATH/src/$IMPORT_PATH"

function docker_url() {
  echo -n "quay.io/signalfx/gateway:$(docker_tag)"
}

# Cache phase of circleci
function do_cache() {
  [ ! -d "$HOME/circleutil" ] && git clone https://github.com/signalfx/circleutil.git "$HOME/circleutil"
  (
    cd "$HOME/circleutil"
    git fetch -a -v
    git fetch --tags
    git reset --hard $CIRCLEUTIL_TAG
  )
  . "$HOME/circleutil/scripts/common.sh"
  mkdir -p "$GO_COMPILER_PATH"
  install_all_go_versions "$GO_COMPILER_PATH"
  install_go_version "$GO_COMPILER_PATH" "$DEFAULT_GOLANG_VERSION"
  go get -u github.com/signalfx/gobuild
  go get -u github.com/alecthomas/gometalinter
  mkdir -p "$GOPATH_INTO"
  install_shellcheck "$GOPATH_INTO"
  copy_local_to_path "$SRC_PATH"
  BUILD_VERSION=$(git describe --tags HEAD)
  if [ -z  "$COMMIT_SHA" ]; then
    COMMIT_SHA=$(git log -n1 --pretty=format:%H)
  fi
  if [ -z  "$BUILDER" ]; then
    BUILDER=circle
  fi
  (
    cd "$SRC_PATH"
    load_docker_images
    LD_FLAGS="-X main.Version=$BUILD_VERSION -X main.BuildDate=$(date --rfc-3339=seconds | sed 's/ /T/') -s -w"
    CGO_ENABLED=0 go build -ldflags "$LD_FLAGS" -v -installsuffix . -o gateway
    echo "{
      \"name\": \"gateway\",
      \"version\": \"$BUILD_VERSION\",
      \"builder\": \"$BUILDER\",
      \"commit\": \"$COMMIT_SHA\"
    }" > buildInfo.json
    docker build -t "$(docker_url)" .
    cache_docker_image "$(docker_url)" gateway
  )
}

# Test phase of circleci
function do_test() {
  . "$HOME/circleutil/scripts/common.sh"
  (
    cd "$SRC_PATH"
    shellcheck install.sh
    shellcheck scripts/circle.sh
    shellcheck gateway_initd
    echo -e "# Ignore Header" > /tmp/ignore_header.md
    python -m json.tool < exampleGateway.conf > /dev/null
  )
  install_go_version "$GO_COMPILER_PATH" "$DEFAULT_GOLANG_VERSION"
  gometalinter --install --update
  gobuild -verbose install
  for GO_VERSION in $GO_TESTED_VERSIONS; do
    install_go_version "$GO_COMPILER_PATH" "$GO_VERSION"
    rm -rf "$GOPATH/pkg"
    go version
    go env
    go tool | grep cover || go get golang.org/x/tools/cmd/cover
    (
      cd "$SRC_PATH"
      go clean -x ./...
      go test -race -timeout 60s ./...
    )
  done
  install_go_version "$GO_COMPILER_PATH" "$GOLANG_VERSION"
  (
    cd "$SRC_PATH"
    go clean -x ./...
    x=$(gobuild list | grep -v ^vendor)
    for y in $x; do
      gobuild check "$y" -verbose -verbosefile "$CIRCLE_ARTIFACTS/gobuildout.$y.txt"
    done
  )
}

# Deploy phase of circleci
function do_deploy() {
  . "$HOME/circleutil/scripts/common.sh"
  (
    if [ "$DOCKER_PUSH" == "1" ]; then
      docker push "$(docker_url)"
    fi
  )
}

function do_all() {
  do_cache
  do_test
  do_deploy
}

case "$1" in
  cache)
    do_cache
    ;;
  test)
    do_test
    ;;
  deploy)
    do_deploy
    ;;
  all)
    do_all
    ;;
  *)
  echo "Usage: $0 {cache|test|deploy|all}"
    exit 1
    ;;
esac
