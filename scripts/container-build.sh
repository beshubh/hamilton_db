#!/bin/sh
# Builds the hamilton image with Apple's `container`. Its builder (1.7.x) walks
# the whole context before applying .dockerignore and stalls on target/, so we
# build from a copy of the repo that already leaves out the ignored paths.
set -eu
repo=$(cd "$(dirname "$0")/.." && pwd)
ctx=$(mktemp -d)
trap 'rm -rf "$ctx"' EXIT
rsync -a --exclude-from="$repo/.dockerignore" "$repo/" "$ctx/"
cp "$repo/Dockerfile" "$ctx/"
container build --cpus "${CPUS:-6}" --memory "${MEMORY:-8g}" \
    -t "${IMAGE:-hamilton:dev}" "$@" "$ctx"
