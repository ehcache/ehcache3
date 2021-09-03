/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package scripts

import org.gradle.api.JavaVersion
import org.gradle.internal.jvm.Jvm

class Utils {

  String version
  String revision
  boolean isReleaseVersion

  Utils(version, logger) {
    this.version = version
    this.isReleaseVersion = !version.endsWith('SNAPSHOT')
    def tmp = System.getenv("GIT_COMMIT")
    if(tmp != null) {
      revision = tmp
    } else {
      logger.debug('Revision not found in system properties, trying command line')
      def cmd = 'git rev-parse HEAD'
      try {
        def proc = cmd.execute()
        revision = proc.text.trim()
      } catch (IOException) {
        revision = 'Unknown'
      }
    }
    logger.debug(revision)
  }

  def fillManifest(manifest, title) {
    manifest.attributes(
            'provider': 'gradle',
            'Implementation-Title': title,
            'Implementation-Version': "$version $revision",
            'Built-By': System.getProperty('user.name'),
            'Built-JDK': System.getProperty('java.version'))
    if (isReleaseVersion) {
      manifest.attributes('Build-Time': new Date().format("yyyy-MM-dd'T'HH:mm:ssZ"))
    }
  }

  def pomFiller(pom, nameVar, descriptionVar) {
    pom.withXml {
      asNode().version[0] + {
        name nameVar
        description descriptionVar
        url 'http://ehcache.org'
        organization {
          name 'Terracotta Inc., a wholly-owned subsidiary of Software AG USA, Inc.'
          url 'http://terracotta.org'
        }
        issueManagement {
          system 'Github'
          url 'https://github.com/ehcache/ehcache3/issues'
        }
        scm {
          url 'https://github.com/ehcache/ehcache3'
          connection 'scm:git:https://github.com/ehcache/ehcache3.git'
          developerConnection 'scm:git:git@github.com:ehcache/ehcache3.git'
        }
        licenses {
          license {
            name 'The Apache Software License, Version 2.0'
            url 'http://www.apache.org/licenses/LICENSE-2.0.txt'
            distribution 'repo'
          }
        }
        developers {
          developer {
            name 'Terracotta Engineers'
            email 'tc-oss@softwareag.com'
            organization 'Terracotta Inc., a wholly-owned subsidiary of Software AG USA, Inc.'
            organizationUrl 'http://ehcache.org'
          }
        }
      }
    }
  }

  static def jvmForHome(File home) {
    def java = Jvm.forHome(home).javaExecutable
    def versionCommand = "$java -version".execute()
    def version = JavaVersion.toVersion((versionCommand.err.text =~ /\w+ version "(.+)"/)[0][1])
    return Jvm.discovered(home, version)
  }
}
