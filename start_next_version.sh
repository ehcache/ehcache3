#!/usr/bin/env bash

###
# Call this script when starting a new major or minor version.
# It will create the branch for the previous version and update all the required files to start the new version.
#
# See https://github.com/ehcache/ehcache3/wiki/dev.release for details.
#
###

# to exit in case of error
set -e
# to see what's going on
set -v

function pause {
    echo
    read -p "Press [enter]  to continue"
}

echo 'Welcome to the Ehcache next version wizard'
echo
echo 'This wizard will guide you through moving from a major.minor version to another. Some steps will be performed automatically, some will require your help'
echo

read -e -p "Enter the next version (x.y): " version

short_version=${version//[.]/}

echo "Upgrading gradle.properties to ${version}"
sed -i '' "s/ehcacheVersion = .*/ehcacheVersion = ${version}-SNAPSHOT/" gradle.properties

echo "Update docs sourcedir to sourcedir${short_version}"
find docs -type f -name '*.adoc' -exec sed -i '' "s/sourcedir[0-9][0-9]/sourcedir${short_version}/g" {} \;

echo "Update version in site content to ${version}"
find docs -type f -name '*.adoc' -exec sed -i '' "s/version: [0-9]\.[0-9]/version: ${version}/" {} \;

echo "Add new XSDs for ${version}"
sed -i '' "s/\/\/ needle_for_core_xsd/** Location for ${version}: \`http:\/\/www.ehcache.org\/schema\/ehcache-core-${version}.xsd\`\\
\/\/ needle_for_core_xsd/" docs/src/docs/asciidoc/user/xsds.adoc
sed -i '' "s/\/\/ needle_for_107_xsd/** Location for ${version}: \`http:\/\/www.ehcache.org\/schema\/ehcache-107-ext-${version}.xsd\`\\
\/\/ needle_for_107_xsd/" docs/src/docs/asciidoc/user/xsds.adoc
sed -i '' "s/\/\/ needle_for_transactions_xsd/** Location for ${version}: \`http:\/\/www.ehcache.org\/schema\/ehcache-tx-ext-${version}.xsd\`\\
\/\/ needle_for_transactions_xsd/" docs/src/docs/asciidoc/user/xsds.adoc
sed -i '' "s/\/\/ needle_for_clustered_xsd/** Location for ${version}: \`http:\/\/www.ehcache.org\/schema\/ehcache-clustered-ext-${version}.xsd\`\\
\/\/ needle_for_clustered_xsd/" docs/src/docs/asciidoc/user/xsds.adoc
