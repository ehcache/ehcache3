package org.ehcache.build;

import org.ehcache.build.conventions.DeployConvention;
import org.ehcache.build.plugins.VoltronPlugin;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

public class ClusteredServerModule implements Plugin<Project> {

  @Override
  public void apply(Project project) {
    project.setGroup("org.ehcache.modules.clustered");

    project.getPlugins().apply(DeployConvention.class);
    project.getPlugins().apply(VoltronPlugin.class);
  }
}
