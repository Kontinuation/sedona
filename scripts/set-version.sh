#!/bin/bash
set -e

VERSION=$1

if [ -z "$VERSION" ]; then
  echo "Usage: $0 <version>"
  exit 1
fi

echo "Setting version to $VERSION"

# Update Maven version (handles multimodule)
mvn versions:set -DnewVersion="$VERSION" -DgenerateBackupPoms=false -DprocessAllModules=true

# Update Python version in pyproject.toml
sed -i "s/^version = \".*\"/version = \"$VERSION\"/" python/pyproject.toml

# Update Python version in sedona/version.py
sed -i "s/^version = \".*\"/version = \"$VERSION\"/" python/sedona/version.py

echo "Version updated to $VERSION"
