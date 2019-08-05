import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.bundling.Zip
import org.gradle.api.tasks.javadoc.Javadoc
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
 * EhDocs
 *   Handle javadocs and API/SPI/asciidoc
 */
class EhDocs implements Plugin<Project> {

  @Override
  void apply(Project project) {
    def utils = new Utils(project.baseVersion, project.logger)
    def hashsetOfProjects = project.configurations.compile.dependencies.withType(ProjectDependency).dependencyProject +
                            project.configurations.compileOnly.dependencies.withType(ProjectDependency).dependencyProject

    project.javadoc {
      title "$project.archivesBaseName $project.version API"
      source hashsetOfProjects.javadoc.source
      classpath = project.files(hashsetOfProjects.javadoc.classpath)
      project.ext.properties.javadocExclude?.tokenize(',').each {
        exclude it.trim()
      }
    }

    if (!project.hasProperty('spiJavadocDisable')) {

      project.task('spiJavadoc', type: Javadoc) {
        title "$project.archivesBaseName $project.version API & SPI"
        source hashsetOfProjects.javadoc.source
        classpath = project.files(hashsetOfProjects.javadoc.classpath)
        exclude '**/internal/**'
        destinationDir = project.file("$project.docsDir/spi-javadoc")
      }

      project.task('spiJavadocJar', type: Jar, dependsOn: 'spiJavadoc') {
        archiveClassifier = 'spi-javadoc'
        from project.tasks.getByPath('spiJavadoc').destinationDir
      }

    }

    project.task('asciidocZip', type: Zip, dependsOn: ':docs:userDoc') {
      archiveClassifier = 'docs'
      from project.tasks.getByPath(':docs:userDoc').outputDir
    }

    project.artifacts {
      archives project.asciidocZip
      if (!project.hasProperty('spiJavadocDisable')) {
        archives project.spiJavadocJar
      }
    }

  }
}
