import com.github.jengelman.gradle.plugins.shadow.tasks.DefaultInheritManifest
import groovy.json.JsonSlurper
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.internal.file.FileResolver
import org.gradle.api.plugins.osgi.OsgiPluginConvention
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
 * EhOsgi
 *   OSGI additions to the manifest controlled by osgi key in gradle.properties
 *   This plugin supports shadowJar if available
 */
class EhOsgi implements Plugin<Project> {

  @Override
  void apply(Project project) {
    def utils = new Utils(project.baseVersion, project.logger)
    def hashsetOfProjects = project.configurations.compile.dependencies.withType(ProjectDependency).dependencyProject +
                            project.configurations.compileOnly.dependencies.withType(ProjectDependency).dependencyProject
    hashsetOfProjects += project  //self also, in case the invoking project defines osgi properties

    project.plugins.apply 'java-library'
    project.plugins.apply 'maven'
    project.plugins.apply 'signing'

    def OSGI_OVERRIDE_KEYS = ['Import-Package', 'Export-Package', 'Private-Package', 'Tool', 'Bnd-LastModified', 'Created-By', 'Require-Capability']

    project.jar.doFirst {
      manifest = new DefaultInheritManifest(getServices().get(FileResolver.class))
      if (project.hasProperty('shadowJar')) {
        manifest.inheritFrom "$project.buildDir/tmp/shadowJar/MANIFEST.MF"
      }
      utils.fillManifest(manifest, project.archivesBaseName)

      def osgiConvention = new OsgiPluginConvention(project)
      def osgiManifest = osgiConvention.osgiManifest {

        if (project.hasProperty('shadowJar')) {
          classesDir = project.shadowJar.archivePath
          classpath = project.files(project.configurations.shadowCompile, project.configurations.shadowProvided)
        } else {
          classesDir = project.sourceSets.main.java.outputDir
          classpath = project.sourceSets.main.compileClasspath
        }

        // Metadata
        instructionReplace 'Bundle-Name', "$project.archivesBaseName 3"
        instructionReplace 'Bundle-SymbolicName', "org.ehcache.$project.archivesBaseName"
        instruction 'Bundle-Description', 'Ehcache is an open-source caching library, compliant with the JSR-107 standard.'
        instruction 'Bundle-DocURL', 'http://ehcache.org'
        instruction 'Bundle-License', 'LICENSE'
        instruction 'Bundle-Vendor', 'Terracotta Inc., a wholly-owned subsidiary of Software AG USA, Inc.'
        instruction 'Bundle-RequiredExecutionEnvironment', 'JavaSE-1.8'

        hashsetOfProjects.findAll({ p -> p.ext.properties.osgi}).each{ prop ->
          new JsonSlurper().parseText(prop.ext.properties.osgi).each {
            project.logger.info "OSGI: ${it.key}: ${it.value}"
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

  }
}
