#!/bin/bash

env

export BUILD_ARM_USERNAME=$(get_env BUILD_ARM_USERNAME)
export BUILD_ARM_TOKEN=$(get_env BUILD_ARM_TOKEN)
export BUILD_ARM_PASSWORD=$(get_env BUILD_ARM_TOKEN)

echo "Begin Compliance custom script"

source "$(dirname "${BASH_SOURCE[0]}")/.cra/install-jdk.sh"

set -ex

# perform the build so compliance checks can scan everything
./gradlew assemble

echo "End Compliance custom script"