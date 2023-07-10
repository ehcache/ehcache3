package org.ehcache.build.conventions;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.plugins.JavaPlugin;

public class JavaConvention implements Plugin<Project> {
  @Override
  public void apply(Project project) {
    project.getPlugins().apply(JavaBaseConvention.class);
    project.getPlugins().apply(JavaPlugin.class);
    project.getPlugins().apply(CheckstyleConvention.class);
    project.getPlugins().apply(JacocoConvention.class);
    project.getPlugins().apply(SpotbugsConvention.class);

    DependencyHandler dependencies = project.getDependencies();
    dependencies.add(JavaPlugin.IMPLEMENTATION_CONFIGURATION_NAME, "org.slf4j:slf4j-api:" + project.property("slf4jVersion"));
    dependencies.add(JavaPlugin.TEST_RUNTIME_ONLY_CONFIGURATION_NAME, "org.slf4j:slf4j-simple:" + project.property("slf4jVersion"));

    dependencies.add(JavaPlugin.TEST_IMPLEMENTATION_CONFIGURATION_NAME, "junit:junit:" + project.property("junitVersion"));
    dependencies.add(JavaPlugin.TEST_IMPLEMENTATION_CONFIGURATION_NAME, "org.assertj:assertj-core:" + project.property("assertjVersion"));
    dependencies.add(JavaPlugin.TEST_IMPLEMENTATION_CONFIGURATION_NAME, "org.hamcrest:hamcrest:" + project.property("hamcrestVersion"));
    dependencies.add(JavaPlugin.TEST_IMPLEMENTATION_CONFIGURATION_NAME, "org.mockito:mockito-core:" + project.property("mockitoVersion"));
    dependencies.add(JavaPlugin.TEST_IMPLEMENTATION_CONFIGURATION_NAME, "org.terracotta:terracotta-utilities-test-tools:" + project.property("terracottaUtilitiesVersion"));

    project.getConfigurations().all(config -> {
      config.getResolutionStrategy().dependencySubstitution(subs -> {
        subs.substitute(subs.module("org.hamcrest:hamcrest-core:1.3")).using(subs.module("org.hamcrest:hamcrest-core:" + project.property("hamcrestVersion")));
        subs.substitute(subs.module("org.hamcrest:hamcrest-library:1.3")).using(subs.module("org.hamcrest:hamcrest-library:" + project.property("hamcrestVersion")));
      });
    });
  }
}
