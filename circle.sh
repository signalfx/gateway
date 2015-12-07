#!/bin/bash
set -ex

CIRCLEUTIL_TAG="v1.35"

export GOLANG_VERSION="1.5.1"
export GOROOT="$HOME/go_circle"
export GOPATH="$HOME/.go_circle"
export GOPATH_INTO="$HOME/lints"
export PATH="$GOROOT/bin:$GOPATH/bin:$GOPATH_INTO:$PATH"
export DOCKER_STORAGE="$HOME/docker_images"
export IMPORT_PATH="github.com/$CIRCLE_PROJECT_USERNAME/$CIRCLE_PROJECT_REPONAME"

GO_COMPILER_PATH="$HOME/gover"
SRC_PATH="$GOPATH/src/$IMPORT_PATH"

function docker_url() {
  echo -n "quay.io/signalfx/metricproxy:$(docker_tag)"
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
  install_go_version "$GO_COMPILER_PATH" "$GOLANG_VERSION"
  versioned_goget "github.com/cep21/gobuild:v1.3" "github.com/tools/godep:master"
  mkdir -p "$GOPATH_INTO"
  install_shellcheck "$GOPATH_INTO"
  gem install mdl
  copy_local_to_path "$SRC_PATH"
  (
    cd "$SRC_PATH"
    load_docker_images
    GOPATH="$GOPATH:$(godep path)" CGO_ENABLED=0 go build -v -installsuffix .
    docker build -t "$(docker_url)" .
    cache_docker_image "$(docker_url)" metricproxy
  )
}

# Test phase of circleci
function do_test() {
  . "$HOME/circleutil/scripts/common.sh"
  go version
  go env
  (
    cd "$SRC_PATH"
    shellcheck install.sh
    shellcheck circle.sh
    shellcheck metricproxy_initd
    echo -e "# Ignore Header" > /tmp/ignore_header.md
    cat /tmp/ignore_header.md README.md | grep -av curl | grep -av 'Build Status' | mdl --warnings
    python -m json.tool < exampleSfdbproxy.conf > /dev/null
    env GOPATH="$GOPATH:$(godep path)" gobuild -verbose -verbosefile "$CIRCLE_ARTIFACTS/gobuildout.txt"
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
