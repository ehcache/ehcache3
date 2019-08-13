import com.github.jengelman.gradle.plugins.shadow.ShadowBasePlugin
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.publish.maven.MavenPublication

class Shading implements Plugin<Project> {
  @Override
  void apply(Project project) {

    project.plugins.apply('com.github.johnrengelman.shadow')

    project.configurations.getByName(ShadowBasePlugin.CONFIGURATION_NAME) {
      extendsFrom project.configurations.create('providedShadow')
    }

    project.shadowJar {
      relocate ('org.terracotta.statistics.', 'org.ehcache.shadow.org.terracotta.statistics.')
      relocate ('org.terracotta.offheapstore.', 'org.ehcache.shadow.org.terracotta.offheapstore.')
      relocate ('org.terracotta.context.', 'org.ehcache.shadow.org.terracotta.context.')
      mergeServiceFiles()

      dependencies {
        exclude({ rdep ->
          [project.configurations.shadow, project.configurations.providedShadow]*.resolvedConfiguration.resolvedArtifacts.flatten().any {
            rdep.moduleArtifacts.contains(it);
          }
        })
      }
    }

    project.jar {
      dependsOn project.shadowJar
      from (project.zipTree(project.shadowJar.archivePath.path)) {
        exclude 'META-INF/MANIFEST.MF', 'LICENSE', 'NOTICE'
      }
      from "$project.rootDir/NOTICE"
      duplicatesStrategy = 'exclude'
    }

    project.plugins.withId('biz.aQute.bnd.builder') {
      project.jar {
        classpath = project.files(project.configurations.shadow, project.configurations.providedShadow)
      }
    }

    project.plugins.withId('maven-publish') {
      project.publishing {
        publications {
          shadow(MavenPublication) { publication ->
            project.shadow.component(publication)
          }
          withType(MavenPublication) {
            pom.withXml {
              asNode().dependencies.'*'.findAll() { xml ->
                project.configurations {
                  providedShadow.allDependencies.flatten().find { dep ->
                    if (dep instanceof ProjectDependency) {
                      return dep.group == xml.groupId.text() && dep.dependencyProject.archivesBaseName == xml.artifactId.text()
                    } else {
                      return dep.group == xml.groupId.text() && dep.name == xml.artifactId.text()
                    }
                  }.each() {
                    xml.scope*.value = 'provided'
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
