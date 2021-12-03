package org.ehcache.build;

import org.gradle.api.Project;

public class ClusteredEhcacheModule extends EhcacheModule {

  @Override
  public void apply(Project project) {
    project.setGroup("org.ehcache.modules.clustered");
    super.apply(project);
  }
}
