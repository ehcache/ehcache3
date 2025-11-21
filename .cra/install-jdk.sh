#!/usr/bin/env bash

# This script is broken out because it's used from both SSP and PR scans

which java 
java -version
cat /etc/*elease*
id

# create a gradle environment on the build agent
mkdir -p ~/.gradle/

echo "Using pre-installed Java $JAVA_VERSION in $JAVA_HOME and creating toolchains.xml"

cat > ~/.gradle/gradle.properties << EOF
sonatypeUser = $BUILD_ARM_USERNAME
sonatypePwd = $BUILD_ARM_TOKEN
java${JAVA_VERSION}Home = $JAVA_HOME
EOF

# Install JDK 17
dnf install -y java-17-openjdk-devel
export JAVA_HOME=/usr/lib/jvm/java-17
$JAVA_HOME/bin/java -version
echo "java17Home = $JAVA_HOME" >> ~/.gradle/gradle.properties

cat ~/.gradle/gradle.properties
