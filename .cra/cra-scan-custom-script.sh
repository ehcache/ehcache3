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

source "$(dirname "${BASH_SOURCE[0]}")/install-jdk.sh"

# debug: dump all scripts
find /opt/commons/compliance-checks | while read filename ; do
    echo =========== BEGIN FILE $filename ===========
    cat $filename
    echo =========== END   FILE $filename ===========
done

echo "End CRA custom script"

