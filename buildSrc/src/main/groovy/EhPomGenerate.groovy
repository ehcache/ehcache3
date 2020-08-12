

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.publish.maven.MavenPublication
import scripts.Utils

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

/**
 * EhPomGenerate:
 * Creates pom.xml and pom.properties to be included in produced jars
 * Mimics standard maven jar layout.
 */
class EhPomGenerate implements Plugin<Project> {

  @Override
  void apply(Project project) {

    def utils = new Utils(project.baseVersion, project.logger)

    project.plugins.apply 'maven-publish' // for generating pom.*

    def mavenTempResourcePath = "${project.buildDir}/mvn/META-INF/maven/${project.group}/${project.archivesBaseName}"

    project.model {
      // Write pom to temp location to be picked up later,
      // generatePomFileForMavenJavaPublication task comes from maven-publish.
      tasks.generatePomFileForMavenJavaPublication {
        destination = project.file("$mavenTempResourcePath/pom.xml")
      }
    }

    // Configure pom generation
    project.publishing {
      publications {
        mavenJava(MavenPublication) {
          artifactId project.archivesBaseName
          from project.components.java
          utils.pomFiller(pom, project.subPomName, project.subPomDesc)
          if (project.hasProperty('shadowJar')) {
            pom.withXml {
              if (asNode().dependencies.isEmpty()) {
                asNode().appendNode('dependencies')
              }
              project.configurations.shadowCompile.dependencies.each {
                def dep = asNode().dependencies[0].appendNode('dependency')
                dep.appendNode('groupId', it.group)
                dep.appendNode('artifactId', it.name)
                dep.appendNode('version', it.version)
                dep.appendNode('scope', 'compile')
              }
              project.configurations.pomOnlyCompile.dependencies.each {
                def dep = asNode().dependencies[0].appendNode('dependency')
                dep.appendNode('groupId', it.group)
                dep.appendNode('artifactId', it.name)
                dep.appendNode('version', it.version)
                dep.appendNode('scope', 'compile')
              }
              project.configurations.shadowProvided.dependencies.each {
                def dep = asNode().dependencies[0].appendNode('dependency')
                dep.appendNode('groupId', it.group)
                dep.appendNode('artifactId', it.name)
                dep.appendNode('version', it.version)
                dep.appendNode('scope', 'provided')
              }
              project.configurations.pomOnlyProvided.dependencies.each {
                def dep = asNode().dependencies[0].appendNode('dependency')
                dep.appendNode('groupId', it.group)
                dep.appendNode('artifactId', it.name)
                dep.appendNode('version', it.version)
                dep.appendNode('scope', 'provided')
              }
            }
          }
        }
      }
    }

    // Write pom.properties to temp location
    project.task('writeMavenProperties') {
      doLast {
        project.file(mavenTempResourcePath).mkdirs()
        def propertyFile = project.file "$mavenTempResourcePath/pom.properties"
        def props = new Properties()
        props.setProperty('version', project.version)
        props.setProperty('groupId', project.group)
        props.setProperty('artifactId', project.archivesBaseName)
        props.store propertyFile.newWriter(), null
      }
    }

    if (utils.isReleaseVersion) {
      //ensure that we generate maven stuff and delay resolution as the first task is created dynamically
      project.processResources.dependsOn {
        project.tasks.findAll { task ->
          task.name == 'generatePomFileForMavenJavaPublication' || task.name == 'writeMavenProperties'
        }
      }

      // Pick up pom.xml and pom.properties from temp location
      project.sourceSets {
        main {
          resources {
            srcDir "${project.buildDir}/mvn"
          }
        }
      }
    }
  }
}
