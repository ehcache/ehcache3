package org.ehcache.build;

import org.ehcache.build.conventions.DeployConvention;
import org.ehcache.build.plugins.PackagePlugin;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

public class EhcachePackage implements Plugin<Project> {

  @Override
  public void apply(Project project) {
    project.setGroup("org.ehcache");
    project.getPlugins().apply(PackagePlugin.class);
    project.getPlugins().apply(DeployConvention.class);
  }
}
