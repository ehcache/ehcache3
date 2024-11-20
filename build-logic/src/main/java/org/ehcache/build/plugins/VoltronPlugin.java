package org.ehcache.build.plugins;

import org.ehcache.build.conventions.JavaLibraryConvention;
import org.gradle.api.Action;
import org.gradle.api.NamedDomainObjectProvider;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.bundling.Jar;

import java.io.File;
import java.util.jar.Attributes;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;

public class VoltronPlugin implements Plugin<Project> {

  private static final String VOLTRON_CONFIGURATION_NAME = "voltron";
  private static final String SERVICE_CONFIGURATION_NAME = "service";

  @Override
  public void apply(Project project) {
    project.getPlugins().apply(JavaLibraryConvention.class);

    NamedDomainObjectProvider<Configuration> voltron = project.getConfigurations().register(VOLTRON_CONFIGURATION_NAME, config -> {
      config.setDescription("Dependencies provided by Voltron from server/lib");
      config.setCanBeConsumed(true);
      config.setCanBeResolved(true);

      DependencyHandler dependencyHandler = project.getDependencies();
      String terracottaApisVersion = project.property("terracottaApisVersion").toString();
      String slf4jVersion = project.property("slf4jVersion").toString();
      config.getDependencies().add(dependencyHandler.create("org.terracotta:entity-server-api:" + terracottaApisVersion));
      config.getDependencies().add(dependencyHandler.create("org.terracotta:standard-cluster-services:" + terracottaApisVersion));
      config.getDependencies().add(dependencyHandler.create("org.terracotta:packaging-support:" + terracottaApisVersion));
      config.getDependencies().add(dependencyHandler.create("org.slf4j:slf4j-api:" + slf4jVersion));
    });

    NamedDomainObjectProvider<Configuration> service = project.getConfigurations().register(SERVICE_CONFIGURATION_NAME, config -> {
      config.setDescription("Services consumed by this plugin");
      config.setCanBeResolved(true);
      config.setCanBeConsumed(true);
    });

    project.getConfigurations().named(JavaPlugin.API_CONFIGURATION_NAME, config -> {
      config.extendsFrom(voltron.get());
      config.extendsFrom(service.get());
    });

    project.getTasks().named(JavaPlugin.JAR_TASK_NAME, Jar.class, jar -> {
      //noinspection Convert2Lambda
      jar.doFirst(new Action<Task>() {
        @Override
        public void execute(Task task) {
          jar.manifest(manifest -> manifest.attributes(singletonMap(Attributes.Name.CLASS_PATH.toString(),
            (project.getConfigurations().getByName(JavaPlugin.RUNTIME_CLASSPATH_CONFIGURATION_NAME).minus(voltron.get()).minus(service.get()))
              .getFiles().stream().map(File::getName).collect(Collectors.joining(" ")))));
        }
      });
    });
  }
}
