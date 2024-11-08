#!/bin/bash
 
# Read the current version from the VERSION file
current_version=$(cat VERSION)
 
# Split the version into an array
IFS='.' read -r -a version_parts <<< "$current_version"
 
# Increment the patch version
major=${version_parts[0]}
minor=${version_parts[1]}
patch=${version_parts[2]}
patch=$((patch + 1))
 
# Construct the new version
new_version="$major.$minor.$patch"
 
# Write the new version back to the VERSION file
echo "$new_version" > ./VERSION
 
echo "Updated version to $new_version"
