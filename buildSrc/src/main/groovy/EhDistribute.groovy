import com.github.jengelman.gradle.plugins.shadow.tasks.DefaultInheritManifest
import groovy.json.JsonSlurper
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.artifacts.maven.Conf2ScopeMappingContainer
import org.gradle.api.artifacts.maven.MavenDeployment
import org.gradle.api.internal.file.FileResolver
import org.gradle.api.plugins.MavenPlugin
import org.gradle.api.plugins.osgi.OsgiPluginConvention
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
 * EhDistribute
 */
class EhDistribute implements Plugin<Project> {

  @Override
  void apply(Project project) {
    def utils = new Utils(project.baseVersion, project.logger)
    def hashsetOfProjects = project.configurations.compile.dependencies.withType(ProjectDependency).dependencyProject

    project.plugins.apply 'java'
    project.plugins.apply 'maven'
    project.plugins.apply 'signing'
    project.plugins.apply 'com.github.johnrengelman.shadow'

    def OSGI_OVERRIDE_KEYS = ['Import-Package', 'Export-Package', 'Private-Package', 'Tool', 'Bnd-LastModified', 'Created-By', 'Require-Capability']

    project.configurations {
        shadowProvided
    }

    project.shadowJar {
      baseName = "$project.archivesBaseName-shadow"
      classifier = ''
      dependencies {
        exclude({ rdep -> !['org.ehcache', 'org.terracotta'].any({ prefix -> rdep.moduleGroup.startsWith(prefix) })})
      }
      mergeServiceFiles()
    }

    project.jar {
      dependsOn project.shadowJar
      from(project.zipTree(project.shadowJar.archivePath.getPath())) {
        exclude 'META-INF/MANIFEST.MF', 'LICENSE', 'NOTICE'
      }
      // LICENSE is included in root gradle build
      from "$project.rootDir/NOTICE"
    }

    project.jar.doFirst {
      manifest = new DefaultInheritManifest(getServices().get(FileResolver.class))
      manifest.inheritFrom project.shadowJar.manifest
      utils.fillManifest(manifest, project.archivesBaseName)

      def osgiConvention = new OsgiPluginConvention(project)
      def osgiManifest = osgiConvention.osgiManifest {
        classesDir = project.shadowJar.archivePath
        classpath = project.files(project.configurations.shadow, project.configurations.shadowProvided)

        // Metadata
        instructionReplace 'Bundle-Name', 'Ehcache 3'
        instructionReplace 'Bundle-SymbolicName', "org.ehcache.$project.archivesBaseName"
        instruction 'Bundle-Description', 'Ehcache is an open-source caching library, compliant with the JSR-107 standard.'
        instruction 'Bundle-DocURL', 'http://ehcache.org'
        instruction 'Bundle-License', 'LICENSE'
        instruction 'Bundle-Vendor', 'Terracotta Inc., a wholly-owned subsidiary of Software AG USA, Inc.'
        instruction 'Bundle-RequiredExecutionEnvironment', 'JavaSE-1.6'

        hashsetOfProjects.findAll({ p -> p.ext.properties.osgi}).each{ prop ->
          new JsonSlurper().parseText(prop.ext.properties.osgi).each {
            instruction(it.key, *it.value)
          }
        }

        instruction 'Export-Package', '*'
        instruction 'Import-Package', '*'
      }
      manifest.inheritFrom(osgiManifest) {
        eachEntry {
          if (it.getKey().startsWith('Bundle') || OSGI_OVERRIDE_KEYS.contains(it.getKey())) {
            it.setValue(it.getMergeValue())
          } else {
            it.setValue(it.getBaseValue())
          }
        }
      }
    }

    project.sourceJar {
      from hashsetOfProjects.flatten {
        it.sourceSets.main.allSource
      }
    }

    project.javadoc {
      title "$project.archivesBaseName $project.version API"
      source hashsetOfProjects.javadoc.source
      classpath = project.files(hashsetOfProjects.javadoc.classpath)
      project.ext.properties.javadocExclude.tokenize(',').each {
        exclude it.trim()
      }
    }

    project.task('spiJavadoc', type: Javadoc) {
      title "$project.archivesBaseName $project.version API & SPI"
      source hashsetOfProjects.javadoc.source
      classpath = project.files(hashsetOfProjects.javadoc.classpath)
      exclude '**/internal/**'
      destinationDir = project.file("$project.docsDir/spi-javadoc")
    }

    project.task('spiJavadocJar', type: Jar, dependsOn: 'spiJavadoc') {
      classifier = 'spi-javadoc'
      from project.tasks.getByPath('spiJavadoc').destinationDir
    }

    project.task('asciidocZip', type: Zip, dependsOn: ':docs:asciidoctor') {
      classifier = 'docs'
      from project.tasks.getByPath(':docs:asciidoctor').outputDir
    }

    project.artifacts {
      archives project.asciidocZip
      archives project.spiJavadocJar
    }

    project.signing {
      required { project.isReleaseVersion && project.gradle.taskGraph.hasTask("uploadArchives") }
      sign project.configurations.getByName('archives')
    }

    def artifactFiltering = {
      pom.scopeMappings.mappings.remove(project.configurations.compile)
      pom.scopeMappings.mappings.remove(project.configurations.runtime)
      pom.scopeMappings.mappings.remove(project.configurations.testCompile)
      pom.scopeMappings.mappings.remove(project.configurations.testRuntime)
      pom.scopeMappings.addMapping(MavenPlugin.COMPILE_PRIORITY, project.configurations.shadow, Conf2ScopeMappingContainer.COMPILE)
      pom.scopeMappings.addMapping(MavenPlugin.COMPILE_PRIORITY, project.configurations.shadowProvided, Conf2ScopeMappingContainer.PROVIDED)
      utils.pomFiller(pom, 'Ehcache', 'Ehcache single jar, containing all modules')
    }

    project.install {
      repositories.mavenInstaller artifactFiltering
    }

    project.uploadArchives {
      repositories {
        mavenDeployer ({
          beforeDeployment { MavenDeployment deployment -> project.signing.signPom(deployment)}

          if (project.isReleaseVersion) {
            repository(id: 'sonatype-nexus-staging', url: 'https://oss.sonatype.org/service/local/staging/deploy/maven2/') {
              authentication(userName: project.sonatypeUser, password: project.sonatypePwd)
            }
          } else {
            repository(id: 'sonatype-nexus-snapshot', url: 'https://oss.sonatype.org/content/repositories/snapshots') {
              authentication(userName: project.sonatypeUser, password: project.sonatypePwd)
            }
          }
        } << artifactFiltering)
      }
    }
  }
}
