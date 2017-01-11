import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.ProjectDependency
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
    project.plugins.apply EhOsgi
    project.plugins.apply EhPomMangle
    project.plugins.apply EhDocs

    def OSGI_OVERRIDE_KEYS = ['Import-Package', 'Export-Package', 'Private-Package', 'Tool', 'Bnd-LastModified', 'Created-By', 'Require-Capability']

    project.configurations {
        shadowCompile
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


    project.sourceJar {
      from hashsetOfProjects.flatten {
        it.sourceSets.main.allSource
      }
    }


    project.signing {
      required { project.isReleaseVersion && project.gradle.taskGraph.hasTask("uploadArchives") }
      sign project.configurations.getByName('archives')
    }

  }
}
