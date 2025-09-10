package org.ehcache.build.conventions;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.ResolutionStrategy;
import org.gradle.api.plugins.BasePlugin;

import java.net.URI;

public class BaseConvention implements Plugin<Project> {

  @Override
  public void apply(Project project) {
    project.getPlugins().apply(BasePlugin.class);

    //TODO temporary for local testing - rollback before merging
    project.getRepositories().mavenLocal();
    project.getRepositories().mavenCentral();
    project.getRepositories().maven(repo -> repo.setUrl(URI.create("https://repo.terracotta.org/maven2")));

    project.getConfigurations().configureEach(
      config -> config.resolutionStrategy(ResolutionStrategy::failOnVersionConflict)
    );
  }
}
