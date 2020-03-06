import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.ModuleDependency
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.tasks.bundling.Jar

class EhVoltron implements Plugin<Project> {
  @Override
  void apply(Project project) {
    project.plugins.withId('java') {
      def voltron = project.configurations.create('voltron') { voltron ->
        description "Dependencies provided by Voltron from server/lib"
        canBeResolved true
        canBeConsumed true

        voltron.dependencies.add(project.dependencies.create(group: 'org.terracotta', name: 'entity-server-api', version: project.terracottaApisVersion))
        voltron.dependencies.add(project.dependencies.create(group: 'org.terracotta', name: 'standard-cluster-services', version: project.terracottaApisVersion))
        voltron.dependencies.add(project.dependencies.create(group: 'org.terracotta', name: 'packaging-support', version: project.terracottaApisVersion))
        voltron.dependencies.add(project.dependencies.create(group: 'org.slf4j', name: 'slf4j-api', version: project.slf4jVersion))
      }
      def service = project.configurations.create('service') { service ->
        description "Services consumed by this plugin"
        canBeResolved true
        canBeConsumed true

        service.withDependencies { deps ->
          def monitoringServiceApi = deps.find { it.group == 'org.terracotta.management' && it.name == 'monitoring-service-api' } as ModuleDependency
          if (monitoringServiceApi != null) {
            monitoringServiceApi.transitive = false
          }
        }
      }

      project.configurations.getByName(JavaPlugin.API_CONFIGURATION_NAME) { api ->
        api.extendsFrom voltron
        api.extendsFrom service
      }

      project.tasks.named(JavaPlugin.JAR_TASK_NAME, Jar) {
        doFirst {
          manifest {
            attributes('Class-Path': (project.configurations.runtimeClasspath - project.configurations.voltron - project.configurations.service).collect { it.getName() }.join(' '))
          }
        }
      }
    }
  }
}
