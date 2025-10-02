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


which java 
java -version
cat /etc/*elease*
id

# Install JDK 17
dnf install -y java-17-openjdk-devel
export JAVA_HOME=/usr/lib/jvm/java-17
$JAVA_HOME/bin/java -version


# debug: dump all scripts
find /opt/commons/compliance-checks | while read filename ; do
    echo =========== BEGIN FILE $filename ===========
    cat $filename
    echo =========== END   FILE $filename ===========
done

echo "End CRA custom script"

