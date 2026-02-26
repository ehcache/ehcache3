package org.ehcache.build.conventions;

import java.util.Map;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.ModuleDependency;
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
    dependencies.add(JavaPlugin.TEST_IMPLEMENTATION_CONFIGURATION_NAME, "net.bytebuddy:byte-buddy:" + project.property("byteBuddyVersion"));
    dependencies.add(JavaPlugin.TEST_IMPLEMENTATION_CONFIGURATION_NAME, "net.bytebuddy:byte-buddy-agent:" + project.property("byteBuddyVersion"));
    dependencies.add(JavaPlugin.TEST_IMPLEMENTATION_CONFIGURATION_NAME, "org.hamcrest:hamcrest:" + project.property("hamcrestVersion"));
    dependencies.add(JavaPlugin.TEST_IMPLEMENTATION_CONFIGURATION_NAME, "org.mockito:mockito-core:" + project.property("mockitoVersion"));
    ModuleDependency md = (ModuleDependency)dependencies.add(JavaPlugin.TEST_IMPLEMENTATION_CONFIGURATION_NAME, "org.terracotta:terracotta-utilities-test-tools:" + project.property("terracottaUtilitiesVersion"));
    if (md != null) {
      md.exclude(Map.of("group", "org.slf4j"));
    }

    project.getConfigurations().all(config -> {
      config.getResolutionStrategy().dependencySubstitution(subs -> {
        subs.substitute(subs.module("org.hamcrest:hamcrest-core:1.3")).with(subs.module("org.hamcrest:hamcrest-core:" + project.property("hamcrestVersion")));
        subs.substitute(subs.module("org.hamcrest:hamcrest-library:1.3")).with(subs.module("org.hamcrest:hamcrest-library:" + project.property("hamcrestVersion")));
        subs.substitute(subs.module("junit:junit:4.12")).using(subs.module("junit:junit:4.13.1"));
      });
      config.getResolutionStrategy().eachDependency(details -> {
        String group = details.getRequested().getGroup();
        String name = details.getRequested().getName();
        if ("net.bytebuddy".equals(group) && ("byte-buddy".equals(name) || "byte-buddy-agent".equals(name))) {
          details.useVersion(project.property("byteBuddyVersion").toString());
          details.because("Align Byte Buddy family versions across AssertJ and Mockito");
        }
      });
    });
  }
}
