

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

    project.plugins.apply "maven-publish" // for generating pom.*

    def mavenTempResourcePath = "${project.buildDir}/mvn/META-INF/maven/${project.group}/${project.archivesBaseName}"

    // Write pom to temp location to be picked up later,
    // generatePomFileForMavenJavaPublication task comes from maven-publish.
    project.model {
      tasks.generatePomFileForMavenJavaPublication {
        destination = project.file("$mavenTempResourcePath/pom.xml")
      }
    }
    //ensure that we generate maven stuff
    project.model {
      tasks.processResources {
        dependsOn project.tasks.generatePomFileForMavenJavaPublication
        dependsOn project.tasks.writeMavenProperties
      }
    }

    // Configure pom generation
    project.publishing {
      publications {
        mavenJava(MavenPublication) {
          artifactId project.archivesBaseName
          from project.components.java
          utils.pomFiller(pom, project.subPomName, project.subPomDesc)
        }
      }
    }

    // Write pom.properties to temp location
    project.task('writeMavenProperties') {
      doLast {
        def propertyFile = project.file "$mavenTempResourcePath/pom.properties"
        def props = new Properties()
        props.setProperty("version", project.version)
        props.setProperty("groupId", project.group)
        props.setProperty("artifactId", project.archivesBaseName)
        props.store propertyFile.newWriter(), null
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
