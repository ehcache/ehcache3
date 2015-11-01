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

package scripts;

class Utils {

  String version

  def fillManifest(manifest, title) {
    manifest.attributes(
            'provider': 'gradle',
            'Implementation-Title': title,
            'Implementation-Version': version,
            'Built-By': System.getProperty('user.name'),
            'Built-JDK': System.getProperty('java.version'),
            'Build-Time': new Date().format("yyyy-MM-dd'T'HH:mm:ssZ")
    )
  }

  def pomFiller(pom, nameVar, descriptionVar) {
    pom.project {
      name = nameVar
      description = descriptionVar
      url = 'http://ehcache.org'
      organization {
        name = 'Terracotta Inc., a wholly-owned subsidiary of Software AG USA, Inc.'
        url = 'http://terracotta.org'
      }
      issueManagement {
        system = 'Github'
        url = 'https://github.com/ehcache/ehcache3/issues'
      }
      scm {
        url = 'https://github.com/ehcache/ehcache3'
        connection = 'scm:git:https://github.com/ehcache/ehcache3.git'
        developerConnection = 'scm:git:git@github.com:ehcache/ehcache3.git'
      }
      licenses {
        license {
          name = 'The Apache Software License, Version 2.0'
          url = 'http://www.apache.org/licenses/LICENSE-2.0.txt'
          distribution = 'repo'
        }
      }
      developers {
        developer {
          name = 'Alex Snaps'
          email = 'alex.snaps@gmail.com'
          organization = 'Terracotta Inc., a wholly-owned subsidiary of Software AG USA, Inc.'
          organizationUrl = 'http://ehcache.org'
        }
        developer {
          name = 'Louis Jacomet'
          email = 'ljacomet@gmail.com'
          organization = 'Terracotta Inc., a wholly-owned subsidiary of Software AG USA, Inc.'
          organizationUrl = 'http://ehcache.org'
        }
        developer {
          name = 'Tim Eck'
          email = 'timeck@gmail.com'
          organization = 'Terracotta Inc., a wholly-owned subsidiary of Software AG USA, Inc.'
          organizationUrl = 'http://ehcache.org'
        }
        developer {
          name = 'Chris Dennis'
          email = 'chris.w.dennis@gmail.com'
          organization = 'Terracotta Inc., a wholly-owned subsidiary of Software AG USA, Inc.'
          organizationUrl = 'http://ehcache.org'
        }
        developer {
          name = 'Ludovic Orban'
          email = 'lorban@bitronix.be'
          organization = 'Terracotta Inc., a wholly-owned subsidiary of Software AG USA, Inc.'
          organizationUrl = 'http://ehcache.org'
        }
        developer {
          name = 'Clifford W. Johnson'
          email = 'clifford.johnson@softwareag.com'
          organization = 'Terracotta Inc., a wholly-owned subsidiary of Software AG USA, Inc.'
          organizationUrl = 'http://ehcache.org'
        }
        developer {
          name = 'Aurélien Broszniowski'
          email = 'jsoftbiz@gmail.com'
          organization = 'Terracotta Inc., a wholly-owned subsidiary of Software AG USA, Inc.'
          organizationUrl = 'http://ehcache.org'
        }
        developer {
          name = 'Vitaliy Funshteyn'
          email = 'vfunshte@terracottatech.com'
          organization = 'Terracotta Inc., a wholly-owned subsidiary of Software AG USA, Inc.'
          organizationUrl = 'http://ehcache.org'
        }
        developer {
          name = 'Anthony Dahanne'
          email = 'anthony.dahanne@gmail.com'
          organization = 'Terracotta Inc., a wholly-owned subsidiary of Software AG USA, Inc.'
          organizationUrl = 'http://ehcache.org'
        }
        developer {
          name = 'Gaurav Mangalick'
          email = 'gaurav.mangalick@softwareag.com'
          organization = 'Terracotta Inc., a wholly-owned subsidiary of Software AG USA, Inc.'
          organizationUrl = 'http://ehcache.org'
        }
        developer {
          name = 'Hung Huynh'
          email = 'hhuynh@gmail.com'
          organization = 'Terracotta Inc., a wholly-owned subsidiary of Software AG USA, Inc.'
          organizationUrl = 'http://ehcache.org'
        }
      }
    }
  }
}
