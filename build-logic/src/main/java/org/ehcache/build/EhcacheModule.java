package org.ehcache.build;

import org.ehcache.build.conventions.BndConvention;
import org.ehcache.build.conventions.JavaLibraryConvention;
import org.ehcache.build.conventions.DeployConvention;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

public abstract class EhcacheModule implements Plugin<Project> {

  @Override
  public void apply(Project project) {
    project.getPlugins().apply(JavaLibraryConvention.class);
    project.getPlugins().apply(DeployConvention.class);
    project.getPlugins().apply(BndConvention.class);
  }
}
