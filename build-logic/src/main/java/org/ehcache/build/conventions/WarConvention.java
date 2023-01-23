package org.ehcache.build.conventions;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.WarPlugin;

public class WarConvention implements Plugin<Project> {
  @Override
  public void apply(Project project) {
    project.getPlugins().apply(WarPlugin.class);
    project.getPlugins().apply(JavaConvention.class);
  }
}
