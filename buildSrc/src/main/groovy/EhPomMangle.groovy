import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.artifacts.maven.Conf2ScopeMappingContainer
import org.gradle.api.artifacts.maven.MavenDeployment
import org.gradle.api.plugins.MavenPlugin
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
 * EhPomMangle
 *   Removes all implicit dependencies from the pom
 *   and adds only what is specified in (from shadowJar)
 *
 *   project.configurations.shadowCompile  (as compile)
 *   project.configurations.shadowProvided (as provided)
 *
 *   as well as (these do not affect shadow)
 *
 *   project.configurations.pomOnlyCompile
 *   project.configurations.pomOnlyProvided
 *
 *   Also defines the pom defaults (name, desc, etc) unless overridden in gradle.properties
 *   Also sets up upload repositories
 */
class EhPomMangle implements Plugin<Project> {

  @Override
  void apply(Project project) {
    def utils = new Utils(project.baseVersion, project.logger)

    project.plugins.apply 'java-library'
    project.plugins.apply 'maven'
    project.plugins.apply 'signing'

    project.configurations {
      shadowCompile
      shadowProvided
      pomOnlyCompile
      pomOnlyProvided
    }

    project.dependencyCheck {
      skipConfigurations += ['pomOnlyCompile', 'pomOnlyProvided']
    }

    def artifactFiltering = {
      project.configurations.forEach {
        pom.scopeMappings.mappings.remove(it)
      }

      pom.scopeMappings.addMapping(MavenPlugin.COMPILE_PRIORITY, project.configurations.shadowCompile, Conf2ScopeMappingContainer.COMPILE)
      pom.scopeMappings.addMapping(MavenPlugin.COMPILE_PRIORITY, project.configurations.shadowProvided, Conf2ScopeMappingContainer.PROVIDED)

      //Anything extra to add to pom that isn't in the shadowed jar or compilation
      pom.scopeMappings.addMapping(MavenPlugin.COMPILE_PRIORITY, project.configurations.pomOnlyCompile, Conf2ScopeMappingContainer.COMPILE)
      pom.scopeMappings.addMapping(MavenPlugin.COMPILE_PRIORITY, project.configurations.pomOnlyProvided, Conf2ScopeMappingContainer.PROVIDED)

      utils.pomFiller(pom, project.subPomName, project.subPomDesc)

    }

    project.install {
      repositories.mavenInstaller artifactFiltering
    }

    project.uploadArchives {
      repositories {
        mavenDeployer ({
          beforeDeployment { MavenDeployment deployment -> project.signing.signPom(deployment)}

          if (project.isReleaseVersion) {
            repository(url: project.deployUrl) {
              authentication(userName: project.deployUser, password: project.deployPwd)
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
