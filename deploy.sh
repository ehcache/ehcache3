#!/usr/bin/env bash

###
# Call this script to perform a release of Ehcache (Maven central, website, kit and all).
#
# See https://github.com/ehcache/ehcache3/wiki/dev.release for details.
#
# Set a dryRun variable if you want to skip commits and pushes
###

# to exit in case of error
set -e
# to see what's going on
#set -v

function pause {
    echo
    read -p "Press [enter]  to continue"
}

echo 'Welcome to the Ehcache release wizard'
echo
echo 'This wizard will guide you through an Ehcache release. Some steps will be performed automatically, some will require your help'
echo

if [ -z "$git_origin" ]; then
  git_origin=git@github.com:ehcache/ehcache3.git
fi
current_branch=$(git branch | grep '^\*' | cut -d ' ' -f 2)

read -e -p "You want to deploy from the git branch named ${current_branch}, is that right? (Y/n): " YN
[[ $YN != "y" && $YN != "Y" && $YN != "" ]] && (echo "Please checkout the correct branch and restart this script" && exit 1)

echo
echo 'We will now make sure you are up-to-date with the origin'
echo

git pull $git_origin $current_branch

if [ ! -z "$(git status --porcelain)" ]; then
  echo 'You have local changes. Please remove them and relaunch this script'
  exit 1
fi

echo
echo 'All good'
echo

read -e -p "Which version do you want to release? " version

# A major release will end with 0. e.g. 3.7.0, 3.8.0
if [ "$(echo $version | cut -d '.' -f 3)" == "0" ]; then
  is_major=1
else
  is_major=0
fi

# A latest version will always be deployed from master. Bugfix of ealier versions will be from a release/x.y branch
if [ "$current_branch" == "master" ]; then
  is_latest_version=1
else
  is_latest_version=0
fi

read -e -p "You want to deploy version ${version} from branch ${current_branch}. Is that correct? (Y/n)" YN
[[ $YN != "y" && $YN != "Y" && $YN != "" ]] && (echo "Aborting due to wrong input" && exit 1)

major_version="$(echo $version | cut -d'.' -f 1).$(echo $version | cut -d'.' -f 2)"
short_major_version=${major_version//[.]/}

echo
echo 'We will start by configuring GitHub correctly'
echo "First make sure you have a milestone for version ${version} at https://github.com/ehcache/ehcache3/milestones"
echo "If you don't, create it. Name it ${version}"

read -e -p "What is the milestone number? (look at the URL) " milestone

echo
echo 'Now attach any closed issues and PR since the last version'
read -e -p 'What was the previous version? ' previous_version
echo 'A helpful git log will now be printed'
echo
git --no-pager log v${previous_version}..HEAD
pause

echo "Now, let's create an issue for the release"
echo "It contains checkboxes that you will check along the release"
echo "Open https://github.com/ehcache/ehcache3/issues"
echo "Press 'New Issue'"
echo "Set the title to 'Release ${version}'"
echo "Attach the issue to the milestone ${version}"
echo "Assign the issue to you"
echo "Set the description to the content of dist/templates/github-release-issue.md"
echo "Create the issue"
pause

echo "We will now"
echo "1- Create a local release branch named release-${version}."
echo "2- Commit the final version and tag it."
echo "Only the tag will be pushed to origin (not the branch)."
echo

sed -i '' "s/%VERSION%/${version}/g" dist/templates/github-release.md
sed -i '' "s/%MILESTONE%/${milestone}/g" dist/templates/github-release.md
sed -i '' "s/%MAJORVERSION%/${major_version}/g" dist/templates/github-release.md
echo "Please add a little description for the release in dist/templates/github-release.md"
pause

sed -i '' "s/ehcacheVersion = .*/ehcacheVersion = ${version}/" gradle.properties
if [ -z "$dryRun" ]; then
  git checkout -b release-${version}
  git add gradle.properties dist/templates/github-release.md
  git commit -m "Version ${version}"
  git tag -m ":ship: Release ${version}" -v${version}
  git push $git_origin v${version}
else
  echo git checkout -b release-${version}
  echo git add gradle.properties dist/templates/github-release.md
  echo git commit -m "Version ${version}"
  echo git tag -m ":ship: Release ${version}" v${version}
  echo git push $git_origin v${version}
fi

echo
echo 'Now launch the release to Maven central'
echo '1- Open http://jenkins.terracotta.eur.ad.sag:8080/view/All%20Pipelines/job/publishers-10.2.0/job/ehcache-releaser-3.x/'
echo '2- Press "Build with parameters'
echo "3- Enter v${version} in the git_tag field"
echo "4- Come back here when it's done"
pause

echo
echo "We will now create a GitHub release"
echo "Open https://github.com/ehcache/ehcache3/tags"
echo "On our new tag v${version}, you will see three dots at the far right"
echo "Click on it and select 'Create Release'"
echo "Set the Tag version to v${version}"
echo "Set the Release title to 'Ehcache ${version}'"
echo "Set the following in the description"
release_description=$(< dist/templates/github-release.md)
echo "$release_description"
pause

echo "Add the binaries from Maven central to the release"
echo "They will be downloaded in dist/build/binaries"
mkdir -p dist/build/binaries
pushd dist/build/binaries
wget https://repo1.maven.org/maven2/org/ehcache/ehcache/${version}/ehcache-${version}-javadoc.jar
wget https://repo1.maven.org/maven2/org/ehcache/ehcache/${version}/ehcache-${version}-spi-javadoc.jar
wget https://repo1.maven.org/maven2/org/ehcache/ehcache/${version}/ehcache-${version}.jar
wget https://repo1.maven.org/maven2/org/ehcache/ehcache-clustered/${version}/ehcache-clustered-${version}-javadoc.jar
wget https://repo1.maven.org/maven2/org/ehcache/ehcache-clustered/${version}/ehcache-clustered-${version}-kit.tgz
wget https://repo1.maven.org/maven2/org/ehcache/ehcache-clustered/${version}/ehcache-clustered-${version}-kit.zip
wget https://repo1.maven.org/maven2/org/ehcache/ehcache-clustered/${version}/ehcache-clustered-${version}.jar
wget https://repo1.maven.org/maven2/org/ehcache/ehcache-transactions/${version}/ehcache-transactions-${version}-javadoc.jar
wget https://repo1.maven.org/maven2/org/ehcache/ehcache-transactions/${version}/ehcache-transactions-${version}.jar
popd
pause

echo "Create the release"
pause

echo "We are doing good. Now let's attack the website"
read -e -p "Where is the ehcache.org-site clone located? (default:../ehcache.org-site): " site_dir

if [ "$site_dir" == "" ]; then
  site_dir='../ehcache.org-site'
fi

if [ $is_major ]; then
  echo "Adding XSDs since this is a major version"
  cp xml/src/main/resources/ehcache-core.xsd $site_dir/schema/ehcache-core.xsd
  cp xml/src/main/resources/ehcache-core.xsd $site_dir/schema/ehcache-core-${major_version}.xsd
  cp 107/src/main/resources/ehcache-107-ext.xsd $site_dir/schema/ehcache-107-ext.xsd
  cp 107/src/main/resources/ehcache-107-ext.xsd $site_dir/schema/ehcache-107-ext-${major_version}.xsd
  cp clustered/client/src/main/resources/ehcache-clustered-ext.xsd $site_dir/schema/ehcache-clustered-ext.xsd
  cp clustered/client/src/main/resources/ehcache-clustered-ext.xsd $site_dir/schema/ehcache-clustered-ext-${major_version}.xsd
  cp transactions/src/main/resources/ehcache-tx-ext.xsd $site_dir/schema/ehcache-tx-ext.xsd
  cp transactions/src/main/resources/ehcache-tx-ext.xsd $site_dir/schema/ehcache-tx-ext-${major_version}.xsd
fi

echo "Copy the javadoc from Maven central"
unzip "dist/build/binaries/ehcache-${version}-javadoc.jar" -d "${site_dir}/apidocs/${version}"
unzip "dist/build/binaries/ehcache-clustered-${version}-javadoc.jar" -d "${site_dir}/apidocs/${version}/clustered"
unzip "dist/build/binaries/ehcache-transactions-${version}-javadoc.jar" -d "${site_dir}/apidocs/${version}/transactions"

echo "Remove Manifests"
rm -rf "${site_dir}/apidocs/${version}/META-INF" "${site_dir}/apidocs/${version}/clustered/META-INF" "${site_dir}/apidocs/${version}/transactions/META-INF"

pushd $site_dir
if [ -z "$dryRun" ]; then
  git checkout master
  git pull origin master
  git checkout -b "ehcache${version}"
else
  echo git checkout master
  echo git pull origin master
  echo git checkout -b "ehcache${version}"
fi

if [ $is_major ]; then
  echo "Update _config.yml"
  echo "  -" >> _config.yml
  echo "    scope:" >> _config.yml
  echo "      path: \"documentation/${major_version}\"" >> _config.yml
  echo "      type: \"pages\"" >> _config.yml
  echo "    values:" >> _config.yml
  echo "      layout: \"docs35_page\"" >> _config.yml
  echo "      ehc_version: \"${major_version}\"" >> _config.yml
  echo "      ehc_javadoc_version: \"${version}\"" >> _config.yml
  echo "      ehc_checkout_dir_var: \"sourcedir38\"" >> _config.yml

  sed -i '' "s/#needle\_for\_sourcedir/    - sourcedir${short_major_version}=\/_eh${short_major_version}\\
#needle_for_sourcedir/" _config.yml
  sed -i '' "s/current: \"[0-9]\.[0-9]\"/current: \"${major_version}\"/" _config.yml
  read -e -p "What is the future version? " future_version
  sed -i '' "s/future: \"[0-9]\.[0-9]\"/future: \"${future_version}\"/" _config.yml

  echo "Update home_announcement.html"
  sed -i '' "s/Ehcache [0-9]\.[0-9] is now available/Ehcache ${major_version} is now available/" _includes/home_announcement.html

  echo "Update documentation/index.md"
  echo "Please add the following line in the current documentation section and move the existing one to history"
  echo "|[Ehcache ${major_version} User Guide](/documentation/${major_version}/) |[Core JavaDoc](/apidocs/${version}/index.html){:target=\"_blank\"} <br /> [Clustered Module JavaDoc](/apidocs/${version}/clustered/index.html){:target=\"_blank\"} <br /> [Transactions Module JavaDoc](/apidocs/${version}/transactions/index.html){:target=\"_blank\"}|"

  echo "Update schema/index.md"
  sed -i '' "s/\[\/\/\]: # (needle_core)/  * [ehcache-core-${major_version}.xsd](\/schema\/ehcache-core-${major_version}.xsd)\\
[\/\/]: # (needle_core)/" schema/index.md
  sed -i '' "s/\[\/\/\]: # (needle_107)/  * [ehcache-107-ext-${major_version}.xsd](\/schema\/ehcache-107-ext-${major_version}.xsd)\\
[\/\/]: # (needle_107)/" schema/index.md
  sed -i '' "s/\[\/\/\]: # (needle_tx)/  * [ehcache-tx-ext-${major_version}.xsd](\/schema\/ehcache-tx-ext-${major_version}.xsd)\\
[\/\/]: # (needle_tx)/" schema/index.md
  sed -i '' "s/\[\/\/\]: # (needle_clustered)/  * [ehcache-clustered-ext-${major_version}.xsd](\/schema\/ehcache-clustered-ext-${major_version}.xsd)\\
[\/\/]: # (needle_clustered)/" schema/index.md

else
  echo "Update _config.yml"
  sed -i '' "s/ehc_javadoc_version: \"${major_version}\.[0-9]\"/ehc_javadoc_version: \"${version}\"/" _config.yml

  echo "Update documentation/index.md"
  echo "Update with the following line in the current documentation section"
  echo "|[Ehcache ${major_version} User Guide](/documentation/${major_version}/) |[Core JavaDoc](/apidocs/${version}/index.html){:target=\"_blank\"} <br /> [Clustered Module JavaDoc](/apidocs/${version}/clustered/index.html){:target=\"_blank\"} <br /> [Transactions Module JavaDoc](/apidocs/${version}/transactions/index.html){:target=\"_blank\"}|"
fi

if [ $is_latest_version ]; then
  echo "Update ehc3_quickstart.html"
  sed -i '' "s/version&gt;[0-9]\.[0-9]\.[0-9]&lt;\/version/version\&gt;${version}\&lt;\/version/" _includes/ehc3_quickstart.html
fi

echo "Please make sur the docs35_page layout in _config.yml is still valid"
pause

echo "Check that README.md table about version, version_dir and branch is still accurate"
pause

read -e -p "What is the upstream repository name? " samples_upstream

if [ -z "$dryRun" ]; then
  git add .
  git commit -m "Release ${version}"
  git push --set-upstream ${samples_upstream} "ehcache${version}"
else
  echo git add .
  echo git commit -m "Release ${version}"
  echo git push --set-upstream ${samples_upstream} "ehcache${version}"
fi
popd

echo "Now please open a PR over branch ehcache${version} https://github.com/ehcache/ehcache3.org-site/pulls"
pause
popd

echo "Website deployment is done every 15 minutes"
echo "If you want to start it manually: http://jenkins.terracotta.eur.ad.sag:8080/job/websites/job/ehcache.org-site-publisher/"

echo
echo "Now please update the current and next release version in README.adoc"
echo
pause

echo
echo "Finally, close the GitHub issue and the milestone"
echo
pause

echo
echo "We now need to deploy the docker images"
echo
read -e -p "What is the terracotta platform version to deploy?" terracotta_version
read -e -p "What is the Terracotta-OSS docker clone located (default:../docker)?" docker_dir
read -e -p "Which previous image do you want to base your image on?" template_image

escaped_template_image=${template_image//./\\.}

echo "You now need to create the appropriate triggers on Docker hub"

echo "Open https://hub.docker.com/r/terracotta/sample-ehcache-client/~/settings/automated-builds/"
echo "Change the Dockerfile location of the latest tag to /${terracotta_version}/sample-ehcache-client"
echo "Add a line with tag ${terracotta_version} and Dockerfile location /${terracotta_version}/sample-ehcache-client"

echo "Open https://hub.docker.com/r/terracotta/terracotta-server-oss/~/settings/automated-builds/"
echo "Change the Dockerfile location of the latest tag to /${terracotta_version}/server"
echo "Add a line with tag ${terracotta_version} and Dockerfile location /${terracotta_version}/server"

pushd $docker_dir

cp -r $template_image $terracotta_version
sed -i '' "s/${escaped_template_image}/${terracotta_version}/g" ${terracotta_version}/sample-ehcache-client/README.md
sed -i '' "s/ehcache-clustered-[0-9]\.[0-9]\.[0-9]-kit.tgz/ehcache-clustered-${version}-kit.tgz/g" ${terracotta_version}/sample-ehcache-client/Dockerfile
sed -i '' "s/ehcache-clustered\/[0-9]\.[0-9]\.[0-9]/ehcache-clustered\/${version}/" ${terracotta_version}/sample-ehcache-client/Dockerfile

sed -i '' "s/${escaped_template_image}/${terracotta_version}/g" ${terracotta_version}/server/README.md
sed -i '' "s/ehcache-clustered-[0-9]\.[0-9]\.[0-9]-kit.tgz/ehcache-clustered-${version}-kit.tgz/g" ${terracotta_version}/server/Dockerfile
sed -i '' "s/ehcache-clustered\/[0-9]\.[0-9]\.[0-9]/ehcache-clustered\/${version}/" ${terracotta_version}/server/Dockerfile

sed -i '' "s/${escaped_template_image}/${terracotta_version}/g" ${terracotta_version}/README.md
sed -i '' "s/${escaped_template_image}/${terracotta_version}/g" ${terracotta_version}/docker-compose.yml

sed -i '' "s/ehcache [0-9]\.[0-9]\.[0-9] \/ Terracotta Server OSS ${escaped_template_image}/ehcache ${version} \/ Terracotta Server OSS ${terracotta_version}/" README.md
sed -i '' "s/\[\/\/\]: # (needle_version)/* [${terracotta_version}](\/${terracotta_version}), matches Ehcache ${version}, available from : https:\/\/github.com\/ehcache\/ehcache3\/releases\\
[\/\/]: # (needle_version)/g" README.md

if [ -z "$dryRun" ]; then
  git add .
  git commit -m "Release $terracotta_version using Ehcache $version"
  git push origin master
else
  echo git add .
  echo git commit -m "Release $terracotta_version using Ehcache $version"
  echo git push origin master
fi
popd

echo "Images should appear on Docker Hub in https://hub.docker.com/r/terracotta"
echo "Please check"
pause

if [ $is_latest_version ]; then
  echo
  echo "And last but not least, upgrade the samples"
  echo
  read -e -p "Where is the ehcache3-samples clone located? (default:../ehcache3-samples): " samples_dir

  if [ "$samples_dir" == "" ]; then
    samples_dir='../ehcache3-samples'
  fi

  pushd $samples_dir

  if [ -z "$dryRun" ]; then
    git checkout master
    git pull origin master
    git checkout -b "ehcache${version}"
  else
    echo git checkout master
    echo git pull origin master
    echo git checkout -b "ehcache${version}"
  fi

  sed -i '' "s/<ehcache3\.version>.*<\/ehcache3\.version>/<ehcache3.version>${version}<\/ehcache3.version>/" pom.xml

  sed -i '' "s/terracotta-server-oss:.*/terracotta-server-oss:${terracotta_version}/g" fullstack/README.md
  sed -i '' "s/terracotta-server-oss:.*/terracotta-server-oss:${terracotta_version}/g" fullstack/src/main/docker/terracotta-server-ha.yml
  sed -i '' "s/terracotta-server-oss:.*/terracotta-server-oss:${terracotta_version}/g" fullstack/src/main/docker/terracotta-server-single.yml

  echo "Make sure the JCache version hasn't changed. If yes, update $samples_dir/pom.xml"
  pause

  git add .
  read -e -p "What is the upstream repository name? " samples_upstream

  if [ -z "$dryRun" ]; then
    git commit -m "Upgrade to Ehcache ${version}"
    git push --set-upstream ${samples_upstream} "ehcache${version}"
  else
    echo git commit -m "Upgrade to Ehcache ${version}"
    echo git push --set-upstream ${samples_upstream} "ehcache${version}"
  fi
  popd

  echo "Now please open a PR over branch ehcache${version} https://github.com/ehcache/ehcache3-samples/pulls"
  pause
fi

echo "All done!"
echo "If needed, call ./start_next_version.sh to bump the version to the next one"
echo
echo "Have a good day!"
