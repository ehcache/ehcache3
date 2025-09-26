#!/usr/bin/env bash

echo "Begin CRA custom script"

if [[ "${PIPELINE_DEBUG:-0}" == 1 ]]; then
    trap env EXIT
    env | sort
    set -x
fi

export BUILD_ARM_TOKEN=$(get_env jfrog_func_is_token)
export BUILD_ARM_PASSWORD=$(get_env jfrog_func_is_token)
export BUILD_ARM_USERNAME=Ibm-webmethods-integration-server@ibm.com

echo "End CRA custom script"

