import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.ModuleDependency
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.tasks.bundling.Jar

class EhVoltron implements Plugin<Project> {

  static String VOLTRON_CONFIGURATION_NAME = 'voltron'
  static String SERVICE_CONFIGURATION_NAME = 'service'

  @Override
  void apply(Project project) {
    project.plugins.withId('java') {
      def voltron = project.configurations.create(VOLTRON_CONFIGURATION_NAME) { voltron ->
        description "Dependencies provided by Voltron from server/lib"
        canBeResolved true
        canBeConsumed true

        voltron.dependencies.add(project.dependencies.create(group: 'org.terracotta', name: 'entity-server-api', version: project.terracottaApisVersion))
        voltron.dependencies.add(project.dependencies.create(group: 'org.terracotta', name: 'standard-cluster-services', version: project.terracottaApisVersion))
        voltron.dependencies.add(project.dependencies.create(group: 'org.terracotta', name: 'packaging-support', version: project.terracottaApisVersion))
        voltron.dependencies.add(project.dependencies.create(group: 'org.slf4j', name: 'slf4j-api', version: project.slf4jVersion))
      }
      def service = project.configurations.create(SERVICE_CONFIGURATION_NAME) { service ->
        description "Services consumed by this plugin"
        canBeResolved true
        canBeConsumed true
      }

      project.configurations.getByName(JavaPlugin.API_CONFIGURATION_NAME) { api ->
        api.extendsFrom voltron
        api.extendsFrom service
      }

      project.tasks.named(JavaPlugin.JAR_TASK_NAME, Jar) {
        doFirst {
          manifest {
            attributes('Class-Path': (project.configurations.runtimeClasspath - voltron - service).collect { it.getName() }.join(' '))
          }
        }
      }
    }
  }
}
