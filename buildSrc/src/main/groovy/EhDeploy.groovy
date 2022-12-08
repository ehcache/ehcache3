import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.Configuration
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
 * EhDeploy
 */
class EhDeploy implements Plugin<Project> {
  @Override
  void apply(Project project) {

    def utils = new Utils(project.baseVersion, project.logger)

    project.plugins.apply 'signing'
    project.plugins.apply 'maven'
    project.plugins.apply EhPomGenerate // for generating pom.*

    project.configurations {
      providedApi
      providedImplementation

      api.extendsFrom providedApi
      implementation.extendsFrom providedImplementation
    }

    project.signing {
      required { project.isReleaseVersion && project.gradle.taskGraph.hasTask("uploadArchives") }
      sign project.configurations.getByName('archives')
    }

    def artifactFiltering = {
      project.configurations.matching {it.name.startsWith('test')}.forEach {
        pom.scopeMappings.mappings.remove(it)
      }
      pom.scopeMappings.addMapping(MavenPlugin.COMPILE_PRIORITY, project.configurations.providedApi, Conf2ScopeMappingContainer.PROVIDED)
      pom.scopeMappings.addMapping(MavenPlugin.COMPILE_PRIORITY, project.configurations.providedImplementation, Conf2ScopeMappingContainer.PROVIDED)
      project.configurations.configureEach { Configuration conf ->
        if (conf.name in [EhVoltron.VOLTRON_CONFIGURATION_NAME, EhVoltron.SERVICE_CONFIGURATION_NAME]) {
          pom.scopeMappings.addMapping(MavenPlugin.PROVIDED_COMPILE_PRIORITY, conf, Conf2ScopeMappingContainer.PROVIDED)
        }
      }

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

    def installer = project.install.repositories.mavenInstaller
    def deployer = project.uploadArchives.repositories.mavenDeployer

  }
}
