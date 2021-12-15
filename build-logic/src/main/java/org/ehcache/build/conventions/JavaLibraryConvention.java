package org.ehcache.build.conventions;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaLibraryPlugin;

public class JavaLibraryConvention implements Plugin<Project> {

  @Override
  public void apply(Project project) {
    project.getPlugins().apply(JavaConvention.class);
    project.getPlugins().apply(JavaLibraryPlugin.class);
  }
}
